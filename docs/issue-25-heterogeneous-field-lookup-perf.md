# `encoderecord` heterogeneous field lookup (#25): performance check

`encoderecord`'s single-pass path probed `field_index_by_name` with
`find(std::string(key_bin.data, key_bin.size))`, building a temporary
`std::string` for every input-map entry scanned. That heap-allocates whenever
the key exceeds the SSO cutoff (22 bytes on libc++).

## The change

- `c_src/schema_item.hh`: when the standard library advertises
  `__cpp_lib_generic_unordered_lookup` (C++20, P0919/P2077), `field_index_by_name`
  uses a transparent hash/equal pair and is probed with a `std::string_view`
  (no allocation, still O(1)). Otherwise it stays a plain
  `unordered_map<std::string, size_t>` and the lookup falls back to the old
  `std::string` temporary. Both go through `SchemaItem::find_field_index()`.
- `c_src/Makefile`: compiles a tiny probe with `-std=c++20`; if it passes
  (GCC >= 11, libc++ >= 16) the build uses `-std=c++20`, else `-std=c++17`.
  `make CXX_STD=c++17` (or `c++20`) overrides the probe.

This is option 1 from #25: no toolchain floor is raised, old compilers keep
building exactly as before.

## Method

Before = `master` built in a separate `git worktree`; after = this branch.
Same compiler (Apple clang 15 / libc++), same flags apart from `-std`.
Each `.so` was loaded in a fresh VM and the three benchmarks run, alternating
base/new for 5 rounds to cancel drift. `erlav_encode` in a tight loop,
µs/encode:

- **opnrtb** - `test/opnrtb_test1.avsc` with `test/opnrtb_perf.data`, N=100k.
- **long30** - synthetic 30-field record, every name 33 chars (all past SSO,
  worst case for this fix), N=1M.
- **wide6** - `test/tschema_record_wide.avsc`, short names (never allocated),
  N=2M.

## Results (µs/encode)

| round | opnrtb base | opnrtb new | long30 base | long30 new | wide6 base | wide6 new |
|---|---|---|---|---|---|---|
| 1 | 15.34 | 14.54 | 4.23 | 2.53 | 0.603 | 0.620 |
| 2 | 15.06 | 14.66 | 4.18 | 2.57 | 0.593 | 0.616 |
| 3 | 16.70 | 14.64 | 4.48 | 2.53 | 0.627 | 0.601 |
| 4 | 15.19 | 14.21 | 4.20 | 2.53 | 0.587 | 0.603 |
| 5 | 15.11 | 14.35 | 4.28 | 2.55 | 0.612 | 0.599 |
| **median** | **15.19** | **14.54** | **4.23** | **2.53** | **0.603** | **0.603** |

| workload | delta (median) |
|---|---|
| opnrtb (368+ fields, ~3% long names) | ~4% faster, new faster in 5/5 rounds |
| long30 (all names > 22 bytes) | ~40% faster |
| wide6 (short names) | no change (interleaved, noise-bound) |

## Verification

- `rebar3 eunit`: 287 tests, 0 failures, with both `-std=c++20` (default on
  this toolchain) and `CXX_STD=c++17` (fallback path).
- No new compiler warnings in either mode.
