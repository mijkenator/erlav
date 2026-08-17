# Why the `array<string>` batch-write optimization didn't work

## Background

PR #17 sped up `array<int>`/`array<long>` encoding by ~34-40% by replacing a
per-element `vector::insert` (in `encodearray`'s scalar loop) with a single
batched write: extract every element into a flat scratch buffer via a raw
pointer, then append the whole buffer to the output vector in one `insert`
call. That eliminated repeated capacity checks and `memmove`s that dominated
the per-element path.

Given that win, the natural next question was whether the same pattern
applies to `array<string>`/`array<bytes>`, which goes through
`encode_binary_data()`:

```cpp
inline int
encode_binary_data(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    ErlNifBinary sbin;
    if (!enif_inspect_binary(env, *input, &sbin)) {
        return 5;
    }
    auto len = sbin.size;
    auto offset = ret->size();
    if (len < 64) {
        ret->resize(offset + 1 + len);
        ret->data()[offset] = static_cast<uint8_t>(len << 1);
        memcpy(ret->data() + offset + 1, sbin.data, len);
    } else {
        std::array<uint8_t, 10> output;
        auto len2 = encodeInt64(len, output);
        ret->resize(offset + len2 + len);
        memcpy(ret->data() + offset, output.data(), len2);
        memcpy(ret->data() + offset + len2, sbin.data, len);
    }
    return 0;
}
```

## What profiling suggested

Profiling `array<string>` encoding (100K short strings, e.g.
`"example123.com"`, via macOS `sample` at a 1ms interval) showed:

| Component | Share of in-NIF time |
|---|---|
| String write path (`encode_binary_data` self + `memmove` + `bzero`) | ~33% |
| Binary extraction (`enif_inspect_binary` + `erts_get_aligned_binary_bytes_extra`) | ~23% |
| `encodearray` traversal + `enif_get_list_cell` | ~25% |
| `encodescalar` dispatch | ~9.5% |

Critically, a `_platform_bzero` frame appeared directly under
`encode_binary_data`, at roughly 7.8% of total in-NIF time. The cause:
`std::vector<uint8_t>::resize()` value-initializes (zero-fills) newly added
elements for a trivial type, and here every zero-filled byte is immediately
overwritten by the following `memcpy`. That looked like the same shape of
waste the PR #17 fix eliminated for `array<int>`/`array<long>` — pure
overhead with no functional purpose.

## What was tried

### Attempt 1: two-pass batch, mirroring PR #17 exactly

Pre-inspect every array element once (recording a `{data, size}` pointer
pair and the running total payload size), size one scratch buffer for the
whole array, write every element's length-prefix + bytes into it via raw
pointer, then append the whole thing in one `insert`.

**Result: ~15% *slower*** (100K-element array, 1000 iterations): ~35ns/elem
vs. ~30ns/elem baseline. The bookkeeping pass (walking a `BinRef{data,size}`
array once to size the buffer, then a second time to write it) cost more
than the `bzero` it was meant to save. Unlike `int`/`long`, where a fixed
worst-case size (5 or 10 bytes) makes the "size buffer, then write" split
cheap, strings are variable-length — sizing the buffer correctly requires an
extra full pass over every element's actual length, which the int/long case
never needed.

### Attempt 2: minimal fix — replace `resize()+memcpy()` with `insert()`

Drop the two-pass bookkeeping entirely and just swap `resize()+memcpy()` for
direct `insert()` calls, which append a source range in one step without a
separate zero-fill pass:

```cpp
if (len < 64) {
    ret->push_back(static_cast<uint8_t>(len << 1));
} else {
    std::array<uint8_t, 10> output;
    auto len2 = encodeInt64(len, output);
    ret->insert(ret->end(), output.data(), output.data() + len2);
}
ret->insert(ret->end(), sbin.data, sbin.data + len);
```

Re-profiling with `sample` confirmed the `bzero` frame was gone entirely.
But end-to-end wall-clock timing (50K-element arrays, 500 iterations, 6 runs
each, at three string lengths) told a different story:

| String size | Before (resize+memcpy) | After (insert-based) | Delta |
|---|---|---|---|
| short (~20B, e.g. domain names) | 26.17 ns/elem | 26.07 ns/elem | ~flat (noise) |
| medium (~200B) | 66.71 ns/elem (σ=0.66, 6 runs) | 70.01 ns/elem (σ=0.23, 6 runs) | **+5.0% slower** |
| long (~2000B) | 420.98 ns/elem | 405.92 ns/elem | -3.6% (within noise band seen elsewhere) |

The medium-string result is the most statistically solid of the three (tight
standard deviation, consistent across six repeats) — and it's a
**regression**, not a win.

## Why the profiling signal didn't translate into a real speedup

`resize()` here does one capacity check followed by a zero-fill over just
the *new* bytes (1-10 bytes for the length prefix, plus the string itself) —
and libc++'s `bzero` is a tight, well-optimized primitive. Splitting the
same work into `push_back()` + `insert()` trades that single
allocate-or-not check for **two** separate append operations, each carrying
its own capacity check and branch.

The `array<int>`/`array<long>` fix in PR #17 won because it collapsed **many
small per-element appends** (one call per array element) into **one big
append for the whole array** — eliminating N-1 capacity checks and
potentially several `memmove`s during vector growth. `encode_binary_data`
never had that "many small appends" shape to begin with: it was already just
two `memcpy`s per element (prefix, then payload), operating on a
thread-local buffer that's reused and pre-sized across calls, so there's
essentially no reallocation happening in steady state either way. Removing
the `bzero` saved a small, real amount of work, but restructuring the calls
to do it cost slightly more than that savings on typical string sizes — a
wash at best, a regression at worst.

## Conclusion

The `array<int>`/`array<long>` win doesn't generalize to `array<string>`.
The fixed-size, uniform nature of varint encoding (bounded at 5 or 10 bytes)
is what made batching cheap to set up for integers; strings' variable length
makes the equivalent setup cost (or the naive call-splitting) outweigh the
savings. No change was made to `encode_binary_data()` as a result of this
investigation — the existing `resize()+memcpy()` implementation stays.

If `array<string>` encoding needs further optimization, the next best lead
from this profiling is binary *extraction* (`enif_inspect_binary` +
`erts_get_aligned_binary_bytes_extra`, ~23% of time), not the write path —
but that cost lives inside the Erlang runtime's own binary handling, on the
other side of the NIF boundary, and isn't something this codebase can
influence directly.

## Related

- PR #17: batch varint writes for `array<int>`/`array<long>` — the
  optimization that motivated this investigation.
- PR #19: bump `encodearray` stack-buffer caps based on production array-size
  data (this document lives alongside that PR).
