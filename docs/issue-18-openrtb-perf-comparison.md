# OpenRTB schema performance: `master` vs #18's `encodemap` scratch-buffer fix

This tracks whether the `encodemap` scratch-buffer batch-write fix (#18,
branch `fix/issue-18-encodemap-scratch-buffer`) moved encode throughput on
the OpenRTB schemas used by `erlav_perf`.

## Method

Same approach as `docs/openrtb-perf-1.8-to-1.14.md`: each branch was built in
its own isolated location (`master` in a separate `git worktree`, the fix
branch built in-place) with identical compiler flags (no `NATIVE_ARCH`, so no
`-march=native` skew), using the existing, unmodified `erlav_perf` module —
no new test code was written for this comparison:

- `erlav_perf:erlav_perf_tst2/1` — schema `test/opnrtb_test1.avsc`, term from
  `test/opnrtb_perf.data`, randomized per iteration. N=50000, 2 runs.
- `erlav_perf:erlav_perf_tst3/1` — schema `test/opnrtb.avsc` (the larger/
  wider full schema), term from `test/field_test.data`, randomized per
  iteration. N=20000 (reduced from the 50000 used in the 1.8→1.14 doc purely
  to keep this comparison's wall-clock reasonable — `erlavro`'s own encode
  on this schema is ~2ms/op), 2 runs.

Both run N iterations of `iolist_to_binary(ErlavroEncoder(Term))` vs.
`erlav_nif:erlav_encode(SchemaId, Term)` back to back, timing each loop with
`erlang:system_time(microsecond)`, and return
`{outputs_equal, erlavro_us_per_op, erlav_nif_us_per_op}`.

## Results (µs/op, `erlav_nif` only)

| Benchmark | Schema | N | master | fix#18 | Δ |
|---|---|---|---|---|---|
| `erlav_perf_tst2` | `opnrtb_test1.avsc` | 50000 | 64.7 / 63.1 (avg 63.9) | 64.3 / 63.7 (avg 64.0) | ~0%, within noise |
| `erlav_perf_tst3` | `opnrtb.avsc` (full) | 20000 | 122.3 / 130.9 (avg 126.6) | 136.6 / 121.1 (avg 128.9) | ~+2%, within noise |

(Two numbers = two separate runs, shown to indicate noise band rather than a
trend — the run-to-run spread of ±5–10µs on both branches is larger than the
difference between branches.)

The `erlavro` baseline (not shown) was similarly flat between branches, as
expected — the fix doesn't touch `erlavro`.

## Why no change

Both `opnrtb_test1.avsc` and `opnrtb.avsc` declare their `map` fields as
`map<array<long>>` — complex-valued maps:

```json
{"type": "map", "values": {"type": "array", "items": "long"}}
```

#18's fix only adds a batched fast path for **scalar**-valued maps
(`map<int>`/`map<long>`, i.e. `encodemap`'s `st == 0 || st == 1` branch).
Complex-valued maps (records, arrays, other maps, unions, enums as values)
fall through the pre-existing, untouched `else` branch. Neither OpenRTB
fixture has a directly scalar-valued map field, so the new code path never
executes on either schema — a flat result is exactly what's expected.

## Bottom line

No measurable performance regression or improvement for either OpenRTB
schema from the #18 fix. This is expected: the fix targets `map<int>`/
`map<long>` specifically, and neither `opnrtb_test1.avsc` nor `opnrtb.avsc`
has a scalar-valued map field for it to engage on.

The fix's actual ~13% gain (see PR #35's description) was measured directly
against a `map<long>` schema (`test/tschema_map.avsc`, 5000 entries) — the
shape the fix targets, which OpenRTB's fixtures don't happen to use.
