# Decoder: building maps in one call (performance investigation)

Not tracked in any GitHub issue. All existing perf issues/docs (#16, #18, #20,
#23, #26, `docs/*-attempt.md`) are about **encode**; decode had not been
profiled.

## Finding

Profiling `erlav_decode_fast` on `test/opnrtb_test1.avsc` +
`test/opnrtb_perf.data` (macOS `sample`, 1 ms, active threads only) showed the
time is almost entirely inside `beam.smp` building the result, not in erlav's
own decode code (~8.5% of active samples):

| share of active samples | where |
|---|---|
| 27% | key comparison (`erts_cmp_compound`, `eq`, `memcmp`) |
| 21% | map building (`erts_maps_put`, hashmap/rbt/qsort) |
| 14% | GC / heap growth |
| 14% | allocation, binary creation, memmove |
| 8.5% | erlav decode code |
| 7% | atom table (`erts_atom_put_index`, `atom_hash`) |
| 6% | an rwmutex, whose callers in the call graph are `erts_atom_put_index` (i.e. `enif_make_atom`), not erlav's `find_schema` |

Cause: `decode()` called `enif_make_map_put` once per field. Each put returns a
new map and re-compares keys, so building an n-field map is roughly O(n^2).
`enif_make_map_from_arrays` (OTP >= 21, present in the 26 headers used here)
builds the map once. Separately, `enif_make_atom("undefined"/"true"/"false")`
took the atom-table lock and hashed the name on every decoded null/boolean.

## Prototype (this branch)

`c_src/mkh_avro_decoder.cc`, pointer-based decoder only (the iterator-based
legacy `decode()` is untouched):

1. `decode()` (records) collects keys/values into stack arrays and builds the
   map once with `enif_make_map_from_arrays`. Record field names are unique
   (enforced at schema init), so the duplicate-key failure cannot happen; if it
   somehow did it throws an `AvroException`.
2. `decode_map()` gathers entries in a stack buffer (16 entries, heap spill
   beyond). For **4+ entries** it uses `from_arrays`; for fewer it keeps the
   `enif_make_map_put` loop. Wire maps may contain duplicate keys, which
   `from_arrays` rejects, so on failure it falls back to the put loop
   (last occurrence wins), matching previous behaviour (checked with a
   hand-built duplicate-key payload).
3. `atom_undefined/true/false` are looked up once (function-local statics)
   instead of per value.
4. `decode_enum` takes the symbol by `const&` (it copied the `std::string`
   on every decode).

## Results (µs/decode, master vs. prototype, 5 interleaved rounds)

Apple clang 15, `-std=c++20`, same flags. Medians:

| workload | master | prototype | change |
|---|---|---|---|
| OpenRTB (900 B, deeply nested) | 118.0 | 38.3 | **~3.1x faster** (5/5 rounds) |
| 6-field record | 1.33 | 0.53 | ~2.5x faster |
| 4-field record | 0.49 | 0.28 | ~1.7x faster |
| 2-field record | 0.18 | 0.18 | no change (noise) |

`map<string,long>` by entry count (single map field):

| entries | 2 | 4 | 8 | 16 | 32 | 64 | 128 |
|---|---|---|---|---|---|---|---|
| master | 0.22 | 0.53 | 1.44 | 5.1 | 20.1 | 27.2 | 37.5 |
| prototype | 0.26 | 0.33 | 0.54 | 0.97 | 2.0 | 6.7 | 15.5 |

Large maps are 2.5-10x faster. **Known regression:** a 2-entry map is ~15%
slower (0.22 -> 0.26 µs; the gather buffer is pure overhead when the put
loop is still used). 3-entry maps were not in the final sweep; they take the
put loop, so should match the 2-entry behaviour, but that is unmeasured.

### Dead ends found while tuning

- Using `std::vector` for the gather buffer made 3-entry maps ~55% slower than
  master (two heap allocations per call): replaced by the stack buffer.
- `from_arrays` for **every** map, including tiny ones, made the OpenRTB
  fixture ~47 µs instead of ~38 µs: the fixture's maps are almost all 1-5
  entries, where `from_arrays`' fixed setup cost loses to the put loop. Hence
  the 4-entry threshold. It is a conservative pick from one machine's sweep,
  not a measured optimum: thresholds 3 and 8 were also tried, 8 left 8-entry
  maps at master speed, and 3 vs 4 was not cleanly separated;
  other OTP versions or CPUs may put the crossover elsewhere.

## Verification

- `rebar3 eunit`: 287 tests, 0 failures with `-std=c++20` and `CXX_STD=c++17`.
- Decoded output of OpenRTB, enum, and union-array fixtures hashes identically
  (`erlang:phash2`) on master and prototype; 200-entry map round-trips; the
  duplicate-key payload decodes to the same term on both.
- Not covered: no new unit tests were added in this prototype; `erlav_decode/2`
  (the iterator-based path) was not changed or measured.

## Other candidates noticed while reading (NOT measured, NOT implemented)

Treat these as hypotheses to profile, not findings:

- `decode_array` scalar path calls `get_scalar_type(si->obj_field)` (a linear
  string scan) on every array decode; `SchemaItem` already has `scalar_type`.
  The multi-type union path additionally copies a `std::string` and runs a
  chain of string compares per element.
- `decode_array` builds a `std::vector` per call with no `reserve`.
- `decode_float`/`decode_double` copy byte-by-byte into a temporary array;
  a `memcpy` would do (assumes little-endian host, like the current code).
- Encoder: `encodearray`'s union-member path does `array_multi_type.count("x")`
  / `.at("x")` with `const char*` literals per element (hash + possibly a
  temporary `std::string` per lookup); resolving the member indexes once at
  schema-init would remove them.
- Encoder: `encodeenum` copies the symbol into a `std::string` before lookup.
