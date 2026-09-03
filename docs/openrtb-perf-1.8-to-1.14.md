# OpenRTB schema performance: 1.8 → 1.14

This tracks whether any of the changes between tags `1.8` and `1.14` moved
encode throughput on the OpenRTB schemas used by `erlav_perf`. Covers both
the incremental comparison (`1.12` → `1.14`, just the negative block-count
feature) and the full range (`1.8` → `1.14`, six releases of fixes and
optimizations).

## Method

Each tag was built in its own `git worktree` (`rebar3 compile`, which
invokes `make -C c_src`) so the comparison uses an isolated `.so` per tag
rather than relying on rebuild-in-place. Benchmarks used the existing,
unmodified `erlav_perf` module — no new test code was written:

- `erlav_perf:erlav_perf_tst2/1` — schema `test/opnrtb_test1.avsc`, term from
  `test/opnrtb_perf.data`, randomized per iteration.
- `erlav_perf:erlav_perf_tst3/1` — schema `test/opnrtb.avsc` (the larger/
  wider full schema, 2006 lines), term from `test/field_test.data`,
  randomized per iteration.

Both run N iterations of `iolist_to_binary(ErlavroEncoder(Term))` vs.
`erlav_nif:erlav_encode(SchemaId, Term)` back to back, timing each loop with
`erlang:system_time(microsecond)`, and return
`{outputs_equal, erlavro_us_per_op, erlav_nif_us_per_op}`. All runs used
N=50000, with 2 runs per tag/benchmark to sanity-check noise.

## What changed between 1.8 and 1.14

| Tag | Change |
|---|---|
| 1.9 | Fix encodearray double-walk, batch varint writes, tune stack-buffer caps (#16/#17/#19) |
| 1.10 | Fix array-of-union encoding/decoding for record/map/enum members, null items, named-type-ref segfault (#12/#14) |
| 1.11 | Single-pass map scan for `encoderecord` above a small-field threshold (#21) |
| 1.12 | Fix compiler warnings in `AvroException::what()` (no behavior change) (#31) |
| 1.14 | Support Avro negative block-count encoding (opt-in via `use_negative_block_count`, off by default) (#15) |

None of these are targeted at the specific shapes exercised by the OpenRTB
fixtures (`opnrtb_test1.avsc` / `opnrtb.avsc` have no unions-of-record/map/
enum arrays, aren't near the small-field record threshold in a way that
flips code paths both before and after #21, and don't opt into negative
block-count encoding), so the prior was "no measurable change" going in.

## Results (µs/op, 50k iterations)

### `opnrtb_test1.avsc` (`erlav_perf_tst2`)

| tag | erlavro | erlav_nif |
|---|---|---|
| 1.8  | 976 | 91.4 |
| 1.12 | 1138 / 1568 | 76.1 / 71.9 |
| 1.14 | 1437 / 1528 | 80.3 / 76.3 |

### `opnrtb.avsc`, full schema (`erlav_perf_tst3`)

| tag | erlavro | erlav_nif |
|---|---|---|
| 1.8  | 3117 / 2900 | 163.0 / 163.5 |
| 1.12 | 3953 / 3553 | 159.2 / 156.3 |
| 1.14 | 3689 / 2907 | 167.5 / 152.0 |

(Two numbers = two separate runs, shown to indicate noise band rather than a
trend.)

## Findings

- **`erlav_nif` (C++ NIF) encode time is flat from 1.8 through 1.14** — all
  runs land in the same noise band (~72–91µs on `opnrtb_test1`, ~150–167µs
  on the full schema). No regression, no improvement attributable to any of
  the intervening commits.
- The pure-Erlang `erlavro` baseline is similarly flat/noisy across tags, as
  expected — none of these commits touch `erlavro`.
- `erlav_nif` stays **~10–20x faster** than `erlavro` on this schema across
  every tag tested, consistent with the project's general 3-5x+ speedup
  claim.
- Scaling sanity check on 1.8 (before the #21 single-pass map scan fix):
  timed `erlav_perf_tst3` at N = 100, 500, 1000, 2000, 4000, 8000, 16000.
  Wall-clock scaled linearly with N (e.g. ~0.36s → ~57s, roughly 2x per
  doubling), so 1.8 has no pathological (quadratic) blowup on this schema —
  the earlier 50k run just legitimately takes a few minutes because
  `erlavro`'s own encode is ~3ms/op on the large `opnrtb.avsc` schema, not
  because anything is hanging.

## Bottom line

No measurable performance regression or improvement for the OpenRTB schema
between `1.8` and `1.14` **at the default settings**. The six releases in
between are correctness fixes and optimizations targeted at code paths
(array-of-union, large maps, negative block-count) that these specific
OpenRTB fixtures don't exercise, so encode throughput on this schema is
unchanged. Opting into 1.14's new `use_negative_block_count` flag, however,
does cost real throughput — see below.

## Cost of opting into `use_negative_block_count` (1.14)

The comparisons above all use `erlav_init/1`, i.e. the default plain
positive block-count encoding, on every tag including 1.14. But 1.14 also
added an *opt-in* `erlav_init/2` mode (`erlav_init(File,
[use_negative_block_count])`) that makes erlav's own encoder emit Avro's
negative block-count form — the form erlavro's encoder always emits, useful
if something downstream expects byte-identical output to erlavro. That mode
is off by default and wasn't exercised by the 1.8–1.14 comparison above, so
it was benchmarked separately, same method (isolated worktree build of
`1.14`, both schemas, `erlav_nif:erlav_encode/2`).

### Method

Registered each schema twice — once via `erlav_init/1` (positive, default)
and once via `erlav_init/2(File, [use_negative_block_count])` (negative) —
and timed N back-to-back `erlav_encode/2` calls against each, alternating
which mode ran first each repeat to cancel out warm-up bias. Two variants:

- **Randomized term per call, N=20000/10000, 4 repeats** — matches the style
  of `erlav_perf`'s own benchmarks (fresh random data each iteration).
- **Fixed term, N=50000, 4 repeats** — same term re-encoded every iteration,
  to strip out the randomizer's own (nontrivial — tens of seconds for 20k
  terms on the full schema) cost from the timing noise.

### Results (µs/op)

| schema | mode | randomized-term avg | fixed-term avg |
|---|---|---|---|
| `opnrtb_test1.avsc` | positive (default) | 54.2 | 18.6 |
| `opnrtb_test1.avsc` | negative block-count | 64.7 | 24.2 |
| `opnrtb_test1.avsc` | **overhead** | **+19%** | **+30%** |
| `opnrtb.avsc` (full) | positive (default) | 105.0 | 34.2 |
| `opnrtb.avsc` (full) | negative block-count | 126.1 | 47.1 |
| `opnrtb.avsc` (full) | **overhead** | **+20%** | **+38%** |

(The fixed-term numbers are the more trustworthy ones — lower absolute
µs/op because there's no per-call randomization cost mixed in, and a
cleaner, more consistent ratio across repeats. The randomized-term numbers
are noisier but point the same direction.)

Encoded output size also grows, independent of speed: encoding the same
`field_test.data` term against `opnrtb.avsc` produced 1634 bytes (positive)
vs. 1690 bytes (negative) — 56 extra bytes for the block-length varints that
the negative form has to write.

### Why

`encodearray`/`encodemap` in `c_src/mkh_avro2.hh` can't write the
negative-block header until they know the encoded byte length of the whole
block, but that length isn't known until the block is encoded. So the
negative-block path encodes every array/map's entries into a scratch
`std::vector<uint8_t> block_buf` first, then copies that buffer into the
real output buffer once the header (negative count + byte length) is known
and prefixed. The positive-count path just writes the count up front and
streams entries straight into the destination buffer — no scratch buffer,
no extra copy. That per-array/per-map scratch-and-copy is the source of the
~20–40% overhead; it's proportionally worse on `opnrtb.avsc` (the wider
schema) since it has more arrays/maps to pay the extra allocation+copy on.

### Bottom line for this flag

`use_negative_block_count` is opt-in and off by default specifically
because it's an interop trade-off, not a free-standing improvement:
enabling it to match erlavro's own byte-for-byte encoding costs roughly
**20–40% more encode time** (and a handful of extra bytes) versus the
default positive block-count form, on both OpenRTB schemas tested. Only
turn it on if byte-compatibility with erlavro's output is actually required
downstream.
