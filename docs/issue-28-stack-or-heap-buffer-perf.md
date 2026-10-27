# `StackOrHeapBuffer` consolidation (#28): performance check

#28 flagged that `mkh_avro2.hh` carried several hand-rolled copies of the same
"stack array + heap vector + size check + pick a pointer" skeleton. This
change adds `c_src/stack_or_heap_buffer.hh` (`StackOrHeapBuffer<T, Cap>`) and
uses it at every site. The issue asks for rigorous before/after numbers on
these hot paths (see #17/#19/#21), so this records them.

## The change

Four sites, not the three the issue lists (`encodemap`'s per-entry scratch,
`ENTRY_STACK_CAP = 256`, was added by #18 after the issue was filed):

| site | element | cap | fill style |
|---|---|---|---|
| `encodearray` element buffer | `ERL_NIF_TERM` | 384 | `push_back` (length unknown) |
| `encodearray` varint scratch | `uint8_t` | 3840 | `acquire(len * max_bytes)` |
| `encoderecord` found / present | `ERL_NIF_TERM`, `uint8_t` | 64 | `acquire(nfields)` x2 |
| `encodemap` entry scratch | `uint8_t` | 256 | `acquire(needed)` per entry |

Caps and behaviour are unchanged. The helper has two fill modes because the
sites differ: `acquire(n)` when the size is known up front, `push_back` for
the array element buffer, whose length is only known after walking the list.
The inline array is left uninitialized, like the raw arrays it replaces.

Not changed: the decoder's `STACK_CAP` gather buffers in
`mkh_avro_decoder.cc` (record 64, map 16). They are the same skeleton but
live in a different file and were out of #28's scope.

## Correctness

- New `test/stack_buffer_boundary_test.erl` (33 tests): every cap at cap-1,
  cap, cap+1 (array elements 383/384/385; array<int> scratch 767/768/769;
  record fields 63/64/65/130, including half the fields omitted; map entry
  key lengths 235/236/237 for long and 240/241/242 for int), checked against
  erlavro's decoder and `erlav_decode_fast`. It passes on the old code too, so
  it pins existing behaviour.
- Mutation check: changing the helper's `push_back` condition from `<` to
  `<=` makes the suite segfault the VM (one-past-the-end stack write), so the
  boundary tests do catch an off-by-one in the decision logic.
- Full suite: 344 tests, 0 failures on C++20 and with `CXX_STD=c++17`; no
  compiler warnings.
- Encoded output is byte-identical to master (`erlang:phash2` of the encoding
  matched for every benchmark below, every round).

## Method

Master (before) in a `git worktree`, this branch (after) in-place; same
compiler flags. `erlav_encode` called in a tight loop with a fixed term after
a 2000-call warmup, timed with `monotonic_time(nanosecond)`. Two batches of 5
**interleaved** rounds (base, new, base, new, ...), i.e. 10 rounds per side,
because the first batch alone gave an inconclusive picture on some rows.
Intel i9-9980HK, macOS, Apple clang 15, OTP 26, C++20; the machine was not idle.

Benchmarks (the driver is throwaway, not committed):

- **arr_long_100 / arr_long_1000** — `array<long>` of 100 / 1000 elements
  (`test/array_simple.avsc`); exercises the element buffer (heap spill above
  384) and varint scratch.
- **rec_6f** — `test/tschema_record_wide.avsc`, 6 fields (single-pass path,
  `found`/`present` on the stack).
- **rec_130f** — generated 130-long-field record (heap path of `found`/`present`).
- **map_50_short** — `map<long>`, 50 short keys (stack entry scratch).
- **map_20_longkey** — `map<long>`, 20 keys of ~300 bytes (heap entry scratch).
- **opnrtb_test1 / opnrtb_full** — `erlav_encode` of the real fixtures
  (`opnrtb_test1.avsc` + `opnrtb_perf.data`, `opnrtb.avsc` + `field_test.data`),
  not randomized.

## Results (µs/op, median; "new wins" = rounds out of 5 where new < base)

| benchmark | batch | base | new | delta | new wins |
|---|---|---|---|---|---|
| arr_long_100 | A | 1.0815 | 1.1392 | +5.3% | 3/5 |
| | B | 1.1912 | 1.0968 | -7.9% | 5/5 |
| | all 10 | 1.1674 | 1.1012 | -5.7% | |
| arr_long_1000 | A | 10.1164 | 8.7537 | -13.5% | 5/5 |
| | B | 11.3245 | 9.2812 | -18.0% | 5/5 |
| | all 10 | 10.5467 | 9.1669 | **-13.1%** | |
| rec_6f | A | 0.6825 | 0.7214 | +5.7% | 1/5 |
| | B | 0.6898 | 0.7081 | +2.7% | 1/5 |
| | all 10 | 0.6862 | 0.7092 | **+3.4%** | |
| rec_130f | A | 9.8618 | 10.2958 | +4.4% | 3/5 |
| | B | 10.0221 | 10.4007 | +3.8% | 1/5 |
| | all 10 | 9.9786 | 10.3483 | **+3.7%** | |
| map_50_short | A | 2.7189 | 2.7471 | +1.0% | 3/5 |
| | B | 2.6580 | 3.1264 | +17.6% | 0/5 |
| | all 10 | 2.7023 | 2.7667 | +2.4% | |
| map_20_longkey | A | 1.7085 | 1.6352 | -4.3% | 3/5 |
| | B | 1.8005 | 1.6645 | -7.6% | 3/5 |
| | all 10 | 1.7270 | 1.6591 | -3.9% | |
| opnrtb_test1 | A | 12.6814 | 13.1352 | +3.6% | 1/5 |
| | B | 13.5300 | 13.7352 | +1.5% | 1/5 |
| | all 10 | 13.4869 | 13.6822 | +1.4% | |
| opnrtb_full | A | 25.6930 | 25.5324 | -0.6% | 2/5 |
| | B | 26.2556 | 28.7044 | +9.3% | 1/5 |
| | all 10 | 25.7669 | 27.3650 | +6.2% | |

## Reading the numbers

- **Large `array<long>` is faster, consistently**: -13.5% and -18.0% in the two
  batches, new wins 10/10 rounds. The cause was not isolated; the likeliest
  candidate is the element-collection loop now being a single `push_back` call
  the compiler handles better than the open-coded branch, but that is a guess.
- **Records are slightly slower, consistently**: `rec_6f` +5.7% / +2.7% and
  `rec_130f` +4.4% / +3.8% in both batches, new winning only 1-3 of 5 rounds
  each time. That is ~0.02 µs on a 6-field record and ~0.4 µs on a 130-field
  one. Not explained: the helper constructs the same two vectors and does the
  same `std::fill` as before; the extra work is two `size_` stores. Code
  layout / inlining differences are possible but unproven.
- **`map_50_short`, `map_20_longkey`, `arr_long_100`**: the two batches
  disagree in sign or the spread covers the difference; treat as no change.
- **OpenRTB**: batch A is flat (-0.6% / +3.6%), batch B is worse (+9.3% /
  +1.5%). The `opnrtb_full` +9.3% in batch B is much larger than anything in
  batch A and the machine was under load, so it is most likely noise, but the
  pooled median is +6.2%/+1.4% and this run **does not show OpenRTB encode
  as unchanged**. A quieter machine / more rounds would be needed to settle it.

## Bottom line

Behaviour is unchanged (byte-identical output, all tests pass, new boundary
tests). Performance is a trade: ~13% faster on long arrays, ~3-4% slower on
record encoding, and inconclusive on whole-document OpenRTB encode. The
consolidation is justified by #28's maintainability argument, not by speed;
if the record-path cost matters, the fix is to look at `encoderecord`'s two
`acquire` calls specifically.

## Caveats

- One machine, macOS only, no Linux/GCC numbers, not an idle machine.
- Microbenchmarks encode one fixed term, so they measure steady state for that
  shape only.
- Decode was not measured; the decoder is untouched.
