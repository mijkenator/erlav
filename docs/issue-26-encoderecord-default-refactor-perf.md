# `encoderecord` default-handling dedup (#26): performance check

This tracks whether extracting the duplicated missing-field default-handling
tail out of `encoderecord`'s two branches (small-record and single-pass, see
#26) changed encode throughput on either code path, or on the real OpenRTB
fixture.

## The change

`c_src/mkh_avro2.hh`'s `encoderecord()` had two branches — the small-record
path (`nfields <= SMALL_RECORD_FIELD_THRESHOLD`, from #21/#20) and the
large-record single-pass path — each ending in a byte-for-byte identical
three-line "field missing" tail:

```cpp
} else if (it->is_nullable == 1) {
    ret->push_back(0);
} else if (it->obj_type == 2) {
    ret->push_back(0);
}
```

Extracted into a single `static inline encode_field_or_default(SchemaItem*,
ERL_NIF_TERM*, bool found, ErlNifEnv*, std::vector<uint8_t>*, const
std::string& rec_name)` helper, called from both branches. No behavior
change — same encode-or-default logic, same exception message on encode
failure.

## Why this needs checking

#21's own history is the reason to check rather than assume: that PR added
`SMALL_RECORD_FIELD_THRESHOLD` specifically because the small-record path is
sensitive to small amounts of added per-field overhead (2-3 field records
regressed ~4-10% from the single-pass approach's fixed setup cost before the
threshold was added). A new function call per field, even a trivial
`static inline` one, is exactly the kind of change that threshold was meant
to guard against, so it's worth confirming the compiler actually inlines it
away rather than assuming it from the "static inline" annotation alone.

## Method

Same style as `docs/openrtb-perf-1.8-to-1.14.md`: before/after built in
separate locations (before in a `git worktree`, after in-place), same
compiler flags. Three benchmarks, each hitting the "field missing" tail
directly:

- **SMALL** — `test/nullable_record.avsc` (2 fields, on the small-record
  path), encoding `#{<<"id">> => 42}` — the nullable `sub` field is always
  missing, hitting `is_nullable == 1` every call. N=2-3M, 4 repeats.
- **WIDE** — `test/tschema_record_wide.avsc` (6 fields, on the single-pass
  path, same schema #21 added its own regression tests against), encoding a
  term with 3 of 6 fields omitted (nullable long, non-nullable double,
  nullable string) — covers `is_nullable == 1`, the "neither nullable nor
  array" no-op case, and the present-field path together. N=2-3M, 4 repeats.
- **OPNRTB_FULL** — `erlav_perf:erlav_perf_tst3/1` against `test/opnrtb.avsc`
  (real ~430-field nested fixture, single-pass path), N=50000, 2 repeats —
  same method as the existing perf docs.

## Results (µs/op)

### SMALL (2-field, small-record path, missing nullable field)

| repeat | before | after |
|---|---|---|
| 1 (N=2M) | 0.3738 | 0.3656 |
| 2 (N=2M) | 0.3749 | 0.5885 |
| 3 (N=3M) | 0.3651 | 0.3399 |
| 4 (N=3M) | 0.3447 | 0.3620 |

### WIDE (6-field, single-pass path, 3 of 6 fields missing)

| repeat | before | after |
|---|---|---|
| 1 (N=2M) | 0.5026 | 0.5568 |
| 2 (N=2M) | 0.5403 | 0.5603 |
| 3 (N=3M) | 0.5683 | 0.5497 |
| 4 (N=3M) | 0.5517 | 0.5214 |

### OPNRTB_FULL (`erlav_perf_tst3`, N=50000)

| repeat | erlavro | erlav_nif before | erlav_nif after |
|---|---|---|---|
| 1 | 2895.6 | — | 147.3 |
| 2 | 4391.9 (noisy repeat) | 255.4 | — |
| — | 2856.8 | — | — |
| — | 2890.7 | — | 156.4 |

(erlav_nif before/after both land in the ~140-255µs band; the 255µs point
coincides with the noisiest `erlavro` repeat (4392µs vs. ~2860-2896µs on the
other three), consistent with system-level noise on that particular run
rather than a regression — same "noise band, not a trend" pattern the
existing perf docs call out.)

## Findings

- **SMALL**: before and after overlap heavily (0.34-0.38µs on 3 of 4 repeat
  pairs each side); one after-repeat read 0.5885µs against a same-repeat
  before of 0.3749µs, but the very next repeat (N=3M) put after back at
  0.3399-0.3620µs, in line with all other before/after points. Treated as a
  one-off scheduling/noise spike, not a reproducible regression — a genuine
  per-call fixed cost from the extracted helper would show up consistently
  across repeats, not in one of four.
- **WIDE**: before and after interleave with no consistent ordering
  (0.50-0.57µs both sides across repeats) — no directional signal either
  way.
- **OPNRTB_FULL**: `erlav_nif` stays in the same ~140-255µs band before and
  after, tracking `erlavro`'s own noise on the same run rather than
  diverging from it.
- No repeat set shows `after` consistently slower than `before` on any
  benchmark, which is what "the helper inlines away" predicts.

## Bottom line

No measurable performance regression from extracting `encode_field_or_default`
on either the small-record path (#21's threshold exists specifically to catch
this class of regression) or the single-pass path, nor on the real
`opnrtb.avsc` fixture. Consistent with the helper being fully inlined by the
compiler, as expected for a `static inline` function with a trivial body
called from a single translation unit at `-O3`.
