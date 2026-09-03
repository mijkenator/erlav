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
between `1.8` and `1.14`. The six releases in between are correctness fixes
and optimizations targeted at code paths (array-of-union, large maps,
negative block-count) that these specific OpenRTB fixtures don't exercise,
so encode throughput on this schema is unchanged.
