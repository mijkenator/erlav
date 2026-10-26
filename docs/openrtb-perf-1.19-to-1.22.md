# OpenRTB schema performance: 1.19 → 1.22

Continues `docs/openrtb-perf-1.14-to-1.19.md`. Those two comparisons only
covered **encode**; this one also covers **decode**, because the releases since
`1.19` include a decoder rewrite (#54), which is the first change in this series
that is aimed squarely at the OpenRTB fixtures.

## What changed between 1.19 and 1.22

| Tag | Change |
|---|---|
| 1.20 | Avoid per-lookup `std::string` allocation in `encoderecord` via C++20 heterogeneous `unordered_map` lookup (#25, PR #50) |
| 1.21 | Decoder: build record/map terms in one call (`enif_make_map_from_arrays`), cache `undefined`/`true`/`false` atoms (PR #54); CI (#51); unify `SchemaItem` name→index lookups on `NameIndexMap` (#27, PR #57) |
| 1.22 | Decode robustness tests only (PR #56): test-only, no source change |

Source changes between the two tags: `c_src/Makefile`, `c_src/mkh_avro2.hh`,
`c_src/mkh_avro_decoder.cc`, `c_src/schema_item.hh`. `1.22` builds as C++20 on
the machine below (the Makefile probe for heterogeneous lookup succeeds), so
#25's path is active in these numbers.

## Method

Each tag was built in its own `git worktree` (`rebar3 compile`), so each run uses
an isolated `.so`. Encode uses the existing, unmodified `erlav_perf` functions:

- `erlav_perf:erlav_perf_tst2/1` — `test/opnrtb_test1.avsc`, term from
  `test/opnrtb_perf.data`, randomized per iteration.
- `erlav_perf:erlav_perf_tst3/1` — `test/opnrtb.avsc` (full schema), term from
  `test/field_test.data`, randomized per iteration.

`erlav_perf` has no OpenRTB decode benchmark (`decode_tests/0` uses
`prims.avsc`), so decode used a small throwaway driver (below, not committed):
the fixture term is encoded once with `erlav_encode`, then that binary is decoded
N times with `erlav_nif:erlav_decode_fast/2` and with the `erlavro` decoder
(`avro:make_simple_decoder`). Unlike encode, the decode input is **not**
randomized (same 900 B / 1634 B payload every iteration).

N = 50000 for every benchmark, 3 rounds, **interleaved** (1.19, 1.22, 1.19,
1.22, ...) so drift in machine load affects both tags alike. Tables show the
median of the 3 rounds; the per-round values are listed under Raw data.

Machine: Intel Core i9-9980HK @ 2.40 GHz, macOS (Darwin 23.3), Apple clang 15.0,
OTP 26, `-O3` + LTO. The machine was not idle (load average ~3 at start).

## Correctness check

Output was compared across the two tags in every round:

- `erlav_encode` of each fixture: identical `erlang:phash2` on both tags, all 3
  rounds (`opnrtb_test1`: 2006504, `opnrtb`: 16297721).
- `erlav_decode_fast` of each fixture: identical `erlang:phash2` on both tags,
  all 3 rounds (`opnrtb_test1`: 113664276, `opnrtb`: 91328208).

## Results (µs/op, median of 3 rounds, 50k iterations)

### Decode — `erlav_decode_fast`

| schema (payload) | tag | erlavro | erlav_nif | speedup vs erlavro |
|---|---|---|---|---|
| `opnrtb_test1.avsc` (900 B) | 1.19 | 170.6 | 113.0 | 1.5x |
| | 1.22 | 166.5 | 43.8 | 3.8x |
| `opnrtb.avsc` (1634 B) | 1.19 | 266.2 | 167.3 | 1.6x |
| | 1.22 | 263.2 | 72.8 | 3.6x |

`erlav_nif` decode: **2.6x faster** on `opnrtb_test1` (113.0 → 43.8) and **2.3x
faster** on the full schema (167.3 → 72.8). Every one of the 3 rounds shows it;
the ranges do not overlap (e.g. 111.0–114.2 vs 43.3–44.3).

### Encode — `erlav_encode`

| schema | tag | erlavro | erlav_nif | speedup vs erlavro |
|---|---|---|---|---|
| `opnrtb_test1.avsc` | 1.19 | 1180.7 | 77.8 | 15.2x |
| | 1.22 | 1078.3 | 77.4 | 13.9x |
| `opnrtb.avsc` (full) | 1.19 | 2895.8 | 133.7 | 21.7x |
| | 1.22 | 2821.1 | 131.5 | 21.5x |

## Findings

- **Decode got ~2.3–2.6x faster on both OpenRTB schemas**, consistent with the
  ~3.1x measured in `docs/decode-map-build-perf.md` on a different machine and
  with a more nested fixture. Against `erlavro`, decode went from ~1.5x to
  ~3.6–3.8x faster. That is the whole effect of #54: decode was dominated by
  `enif_make_map_put` re-copying the partial map per field, and building each
  record once removes most of that.
- **Encode is unchanged.** `erlav_nif` encode is 77.8 → 77.4 µs
  (`opnrtb_test1`) and 133.7 → 131.5 µs (full), well inside run-to-run spread.
  The first 1.19 round on the full schema (153.6) is the only outlier and is
  load noise, not a trend (the other two were 133.7 and 129.7). So neither #25
  (`encoderecord` heterogeneous lookup) nor #27 (`NameIndexMap`) moves these
  fixtures; their effect shows up in the targeted microbenchmarks in
  `docs/issue-25-heterogeneous-field-lookup-perf.md` and
  `docs/issue-27-name-index-map-perf.md`, not in whole-document OpenRTB encode.
- The `erlavro` columns are noisier than `erlav_nif` (e.g. full-schema decode
  248–337 µs on 1.19). No commit in this range touches `erlavro`, so the small
  1.19-vs-1.22 differences in those columns, and therefore in the encode
  speedup ratios (15.2x → 13.9x), are noise in the baseline, not a regression.

## Caveats

- One machine (Intel Mac), one OTP version, 3 rounds. No Linux/GCC numbers;
  CI's perf check (`ci/perf_check.sh`) only enforces a speedup floor.
- Decode input is a single fixed payload per schema, so this measures the
  steady-state cost for that shape, not variation across inputs.
- `erlav_decode/2` (the legacy iterator decoder) was not measured; #54 did not
  change it.
- The 2-entry-map decode regression noted in #54 (~15% slower) is not visible
  here; the OpenRTB fixtures' maps are dominated by the larger-record gains.

## Bottom line

`1.19` → `1.22`: no encode regression on the OpenRTB schemas, and `erlav_decode_fast` is
**~2.3–2.6x faster** (1.5–1.6x → 3.6–3.8x vs `erlavro`), with decoded output
bit-identical to `1.19`.

## Raw data (µs/op, rounds 1/2/3)

| benchmark | 1.19 | 1.22 |
|---|---|---|
| dec `opnrtb_test1` erlav | 113.01 / 114.20 / 111.04 | 43.27 / 44.30 / 43.77 |
| dec `opnrtb_test1` erlavro | 148.74 / 181.68 / 170.56 | 168.00 / 166.46 / 154.20 |
| dec `opnrtb` erlav | 166.75 / 167.34 / 170.78 | 72.78 / 72.44 / 73.21 |
| dec `opnrtb` erlavro | 337.23 / 248.28 / 266.16 | 268.19 / 263.18 / 241.76 |
| enc `opnrtb_test1` erlav | 79.56 / 76.54 / 77.76 | 78.38 / 77.42 / 77.29 |
| enc `opnrtb_test1` erlavro | 1180.65 / 1297.06 / 1113.11 | 1104.37 / 1036.56 / 1078.26 |
| enc `opnrtb` erlav | 153.62 / 133.68 / 129.67 | 131.54 / 129.73 / 131.49 |
| enc `opnrtb` erlavro | 2895.65 / 2895.77 / 2910.21 | 2861.06 / 2792.75 / 2821.05 |

## Decode driver (run from each worktree root)

```erlang
dec(Schema, DataFile, N) ->
    {ok, SJ} = file:read_file(binary_to_list(Schema)),
    Dec = avro:make_simple_decoder(SJ, []),
    Id = erlav_nif:erlav_init(Schema),
    {ok, [Term]} = file:consult(DataFile),
    Bin = iolist_to_binary(erlav_nif:erlav_encode(Id, Term)),
    Reps = [Bin || _ <- lists:seq(1, N)],
    [erlav_nif:erlav_decode_fast(Id, B) || B <- lists:sublist(Reps, 2000)], % warmup
    T1 = erlang:monotonic_time(microsecond),
    lists:foreach(fun(B) -> Dec(B) end, Reps),
    Erlavro = (erlang:monotonic_time(microsecond) - T1) / N,
    T2 = erlang:monotonic_time(microsecond),
    lists:foreach(fun(B) -> erlav_nif:erlav_decode_fast(Id, B) end, Reps),
    Erlav = (erlang:monotonic_time(microsecond) - T2) / N,
    {Erlavro, Erlav}.
```

Called as `dec(<<"test/opnrtb_test1.avsc">>, "test/opnrtb_perf.data", 50000)` and
`dec(<<"test/opnrtb.avsc">>, "test/field_test.data", 50000)`.
