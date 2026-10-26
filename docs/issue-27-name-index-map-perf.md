# SchemaItem name->index lookups unified on `NameIndexMap` (#27): performance check

## The change

`SchemaItem` had `array_multi_type` and `array_multi_type_child_index` as
`std::map<std::string, int>` while `field_index_by_name` (#20/#21) was a
`std::unordered_map<std::string, size_t>`: the same "name -> position"
operation with two container types and two value types. All three now use one
`NameIndexMap` (`unordered_map<std::string, size_t>`, with the transparent
hash/eq from #25 when the toolchain supports it). `array_multi_type_reverse`
(index -> name) and `cached_keys` (positional term vector) are different
shapes and are left alone.

The issue suggested aligning on `std::map`, with a caveat that
`field_index_by_name` must stay O(1). #25 measured `std::map` as slower on the
record hot path, so this goes the other way: the two `std::map`s become hashed.
Call sites (`.at()`, `.count()`, `operator[]`) are unchanged.

## Method

Before = the #25 branch (`perf/issue-25-heterogeneous-field-lookup`), after =
this branch; separate builds, same compiler/flags (Apple clang 15, `-std=c++20`).
Alternating base/new for 5 rounds, µs/op in a tight loop through `erlav_nif`:

- **union_arr** - `test/array_multi_type2.avsc`, 80-element array of
  `[long, array<[long,string]>]` union items (exercises `array_multi_type` and
  `array_multi_type_child_index` per element). N=200k.
- **enum** - `test/enum.avsc`, one enum field (`array_multi_type` lookup of the
  symbol on encode, `array_multi_type_reverse` on decode). N=1M.

## Results (µs/op)

| round | union_arr enc base | new | union_arr dec base | new | enum enc base | new | enum dec base | new |
|---|---|---|---|---|---|---|---|---|
| 1 | 11.17 | 9.32 | 10.48 | 10.69 | 0.288 | 0.308 | 0.116 | 0.127 |
| 2 | 11.17 | 9.40 | 10.60 | 11.04 | 0.286 | 0.292 | 0.140 | 0.115 |
| 3 | 11.38 | 9.24 | 10.48 | 10.51 | 0.291 | 0.301 | 0.123 | 0.117 |
| 4 | 11.19 | 9.49 | 10.51 | 10.76 | 0.298 | 0.293 | 0.118 | 0.119 |
| 5 | 11.31 | 9.28 | 10.73 | 10.71 | 0.287 | 0.294 | 0.117 | 0.118 |
| **median** | **11.19** | **9.32** | **10.51** | **10.71** | **0.288** | **0.294** | **0.118** | **0.118** |

## Findings

- **Union-array encode: ~17% faster**, new faster in 5/5 rounds. The encoder
  does several `array_multi_type` / `array_multi_type_child_index` lookups per
  element, and hashing a short string beats an `std::map` tree walk.
- **Union-array decode**: ~2% slower at the median (10.51 vs 10.71), within the
  round-to-round spread (10.48-11.04). Decode uses `array_multi_type_reverse`
  (still a `std::map<int, std::string>`) and `.at(...)` on the child index; it
  gained nothing here and the difference is not clearly outside noise.
- **Enum**: encode ~2% slower at the median (0.288 vs 0.294) with overlapping
  ranges; decode identical. Unions here have a handful of members, where a
  tree lookup is already cheap, so a small cost from hashing is plausible but
  not demonstrated.
- No change on the record hot path (`field_index_by_name` is the same type as
  before).

## Verification

`rebar3 eunit`: 287 tests, 0 failures with `-std=c++20` and `CXX_STD=c++17`;
no new warnings.
