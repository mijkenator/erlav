# OpenRTB schema performance: 1.14 → 1.19

This tracks whether any of the changes between tags `1.14` and `1.19` moved
encode throughput on the OpenRTB schemas used by `erlav_perf`. Continues
directly from `docs/openrtb-perf-1.8-to-1.14.md` (which covered `1.8`→`1.14`);
this file covers the five releases since.

## Method

Same method as the prior comparison: each ref was built in its own `git
worktree` (`rebar3 compile`, which invokes `make -C c_src`) so the
comparison uses an isolated `.so` per ref rather than relying on
rebuild-in-place. Benchmarks used the existing, unmodified `erlav_perf`
module — no new test code was written:

- `erlav_perf:erlav_perf_tst2/1` — schema `test/opnrtb_test1.avsc`, term from
  `test/opnrtb_perf.data`, randomized per iteration.
- `erlav_perf:erlav_perf_tst3/1` — schema `test/opnrtb.avsc` (the larger/
  wider full schema, 2006 lines), term from `test/field_test.data`,
  randomized per iteration.

Both run N iterations of `iolist_to_binary(ErlavroEncoder(Term))` vs.
`erlav_nif:erlav_encode(SchemaId, Term)` back to back, timing each loop with
`erlang:system_time(microsecond)`, and return
`{outputs_equal, erlavro_us_per_op, erlav_nif_us_per_op}`. All runs used
N=50000, with 2 runs per ref/benchmark to sanity-check noise.

Compared `1.14` directly against `1.19`'s tip (equivalently, the
`fix/issue-32-map-union-values-crash` branch that `1.19` was tagged from —
same commit, same tree).

## What changed between 1.14 and 1.19

| Tag | Change |
|---|---|
| 1.15 | Apply scratch-buffer batch-write pattern to `encodemap`'s scalar-value loop (#18) |
| 1.16 | Remove production debug output from decode hot path; document `erlav_decode`/`erlav_decode_fast` naming; add `-spec`s (#42) |
| 1.17 | Implement Avro `fixed` type support in schema parser, encoder, decoder (#39) |
| 1.18 | Fix `decode_array` crash/silent-corruption on `array<map>`, `array<fixed>`, `array<enum>` (#44) |
| 1.19 | Fix `decode_map`/`encodemap` VM crash on map values that are a union, e.g. `["null", "string"]` (#32) |

None of these are targeted at the specific shapes exercised by the OpenRTB
fixtures (`opnrtb_test1.avsc`/`opnrtb.avsc` have no `fixed` fields, no
`array<map>`/`array<fixed>`/`array<enum>` shapes, and no map-values-are-a-
union fields), so the prior was "no measurable change" going in — same
expectation as the `1.8`→`1.14` comparison, and for the same reason: these
are correctness fixes for schema shapes this benchmark's fixtures don't
contain, not general encode-path optimizations. (1.15's scratch-buffer
change to `encodemap`'s *scalar*-value loop is the one exception worth
calling out up front — `opnrtb.avsc` does have plain scalar-valued maps, so
that one could plausibly move the needle; see Findings below.)

## Results (µs/op, 50k iterations)

### `opnrtb_test1.avsc` (`erlav_perf_tst2`)

| tag/branch | erlavro | erlav_nif |
|---|---|---|
| 1.14 | 1096.4 / 992.8 | 66.2 / 62.3 |
| 1.19 | 1299.4 / 1440.8 | 68.3 / 67.3 |

### `opnrtb.avsc`, full schema (`erlav_perf_tst3`)

| tag/branch | erlavro | erlav_nif |
|---|---|---|
| 1.14 | 3166.4 / 2993.0 | 154.2 / 133.2 |
| 1.19 | 2757.0 / 2787.4 | 148.8 / 131.1 |

(Two numbers = two separate runs, shown to indicate noise band rather than a
trend.)

## Findings

- **`erlav_nif` (C++ NIF) encode time is flat from 1.14 through 1.19** — all
  runs land in the same noise band (~62–68µs on `opnrtb_test1`, ~131–154µs
  on the full schema). No regression attributable to any of the five
  intervening releases, including the `fixed`-type and `decode_array`/
  `decode_map` correctness fixes, which only add a schema-parse-time flag
  check (`obj_type == 6`, `values_are_union`) evaluated once per field/
  array/map and short-circuiting straight to the pre-existing code path for
  every shape these fixtures actually use.
- 1.15's `encodemap` scratch-buffer change (#18) doesn't show up as a
  measurable win here either — `opnrtb.avsc`'s scalar-valued maps are
  evidently a small enough fraction of total encode cost on this schema
  that the improvement (documented separately for maps specifically in
  #18's own perf doc) doesn't move the needle on the full-schema benchmark.
  Consistent with `1.8`→`1.14`'s finding that OpenRTB-fixture-level
  benchmarks are dominated by other costs (map-key marshaling, NIF
  boundary/BEAM term construction — see #23) rather than any single
  container-type's inner loop.
- The pure-Erlang `erlavro` baseline is noisier than `erlav_nif` between
  runs on both refs (e.g. 992.8→1440.8 on `tst2`), as in the prior
  comparison — expected, since none of these commits touch `erlavro`.
- `erlav_nif` stays **~15–20x faster** than `erlavro` on `opnrtb_test1.avsc`
  and **~19–21x faster** on the full `opnrtb.avsc` schema, consistent with
  `1.8`→`1.14`'s ~10–20x finding and the project's general 3-5x+ speedup
  claim.

## Bottom line

No measurable performance regression for the OpenRTB schema between `1.14`
and `1.19`. The five releases in between are correctness fixes (`fixed`
type, `array<map>`/`array<fixed>`/`array<enum>` decode, map-values-union
decode) and a scalar-map encode optimization, none of which touch the code
paths `opnrtb_test1.avsc`/`opnrtb.avsc` actually exercise — consistent with
the `1.8`→`1.14` comparison's finding that this benchmark's encode
throughput is stable across releases whose changes target schema shapes
these particular fixtures don't contain.
