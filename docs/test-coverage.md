# Test coverage

This project has two layers, and they need to be measured separately since
it's a C++ NIF — `rebar3 cover` only sees the thin Erlang wrapper, not the
actual encoding/decoding logic, which lives in C++.

## Erlang side (`rebar3 eunit --cover`)

| Module | Coverage | Note |
|---|---|---|
| `erlav_nif.erl` (the real wrapper) | **47%** | Uncovered: `init/0`'s NIF-loading path variants, and the `not_loaded/1` stub bodies (never called — they're the placeholder functions the NIF overwrites at load time, unreachable once the `.so` loads successfully) |
| `tst_utils.erl` (test helpers) | 92% | |
| `erlav_manual.erl` / `erlav_perf.erl` | 0% | By design — these are manual/benchmark scratch modules (see the root `CLAUDE.md`: "not part of the test suite, run manually"), not exercised by EUnit |

A naive `rebar3 cover` run reports a "total" of ~9%, which is misleading —
it's dragging in `erlav_manual.erl`/`erlav_perf.erl`, two intentionally
untested benchmark modules, into the denominator. The one production module
that matters, `erlav_nif.erl`, is at 47%, and its gaps are mostly dead/
unreachable code rather than real risk.

## C++ side — where the actual logic lives

`rebar3 cover` can't instrument the NIF, so this was measured separately via
clang's source-based coverage (`-fprofile-instr-generate -fcoverage-mapping`),
building the four C++ source files with `-O0 -g` plus that instrumentation,
linking with `-fprofile-instr-generate`, then running the full `rebar3
eunit` suite against the instrumented `.so` (`LLVM_PROFILE_FILE` set so each
BEAM scheduler's profile data lands in its own file), merging the resulting
`.profraw` files with `llvm-profdata merge -sparse`, and reporting with
`llvm-cov report`/`llvm-cov show`. All 218 tests passed against the
instrumented build, confirming the instrumentation itself didn't change
behavior.

| File | Line coverage | Function coverage | Branch coverage |
|---|---|---|---|
| `mkh_avro2.hh` (core encoder) | 89.4% | 97.3% | 85.0% |
| `mkh_avro_decoder.cc` (decoder) | 92.2% | 95.2% | 80.1% |
| `schema_item.hh` (schema parsing) | 87.3% | 88.9% | 77.2% |
| `erlav_nif.cpp` (NIF entry points) | 69.5% | 100% | 75.0% |
| **Total** | **87.5%** | **96.0%** | **81.8%** |

This is a strong number for the code that actually does the work — 87.5%
line coverage and 96% function coverage across the C++ core.

### Reproducing this measurement

The instrumented build isn't wired into the normal `make`/`rebar3 compile`
flow (unlike the PGO instrumented-build variant, which the Makefile already
supports via `PGO_PHASE`) — it was a one-off manual build in a scratch git
worktree, to avoid touching the real build artifacts. To reproduce:

```bash
# from a scratch worktree/copy of the repo
cd c_src
ERTS_INCLUDE_DIR=$(erl -noshell -eval "io:format(\"~ts/erts-~ts/include/\", [code:root_dir(), erlang:system_info(version)])." -s init stop)
ERL_INTERFACE_INCLUDE_DIR=$(erl -noshell -eval "io:format(\"~ts\", [code:lib_dir(erl_interface, include)])." -s init stop)
ERL_INTERFACE_LIB_DIR=$(erl -noshell -eval "io:format(\"~ts\", [code:lib_dir(erl_interface, lib)])." -s init stop)

for f in mkh_avro_decoder.cc schema_item.cpp erlav_nif.cpp; do
  c++ -O0 -g -fprofile-instr-generate -fcoverage-mapping -std=c++17 -fpermissive \
    -I "$ERTS_INCLUDE_DIR" -I "$ERL_INTERFACE_INCLUDE_DIR" \
    -c -o "${f%.*}.o" "$f"
done

c++ mkh_avro_decoder.o schema_item.o erlav_nif.o \
  -flat_namespace -undefined suppress -shared -fprofile-instr-generate \
  -L "$ERL_INTERFACE_LIB_DIR" -lei -o ../priv/erlav_nif.so

cd ..
mkdir -p /tmp/erlav_cov_profiles
LLVM_PROFILE_FILE="/tmp/erlav_cov_profiles/erlav-%p.profraw" rebar3 eunit

xcrun llvm-profdata merge -sparse /tmp/erlav_cov_profiles/*.profraw \
  -o /tmp/erlav_cov_profiles/merged.profdata

cd c_src
xcrun llvm-cov report ../priv/erlav_nif.so \
  -instr-profile=/tmp/erlav_cov_profiles/merged.profdata \
  mkh_avro2.hh mkh_avro_decoder.cc mkh_avro_decoder.hh schema_item.hh erlav_nif.cpp

# per-file line-by-line detail:
xcrun llvm-cov show ../priv/erlav_nif.so \
  -instr-profile=/tmp/erlav_cov_profiles/merged.profdata \
  <file> --format=text
```

Requires `llvm-cov`/`llvm-profdata` (bundled with Xcode Command Line Tools
on macOS, available via `xcrun`; on Linux, matching binaries ship with the
LLVM toolchain used to build clang).

## The one concrete gap worth flagging

`erlav_nif.cpp`'s uncovered lines are almost entirely the **exception-
handling branches for `erlav_decode_nif`/`erlav_decode_nif_fast`** — the
`AvroException`/`out_of_range`/catch-all `{error, Msg, Code}` paths added in
PR #12 specifically to stop a decode-time exception from calling
`std::terminate()` and crashing the whole BEAM VM. No test currently forces
a decode call to actually throw, so that safety net — the exact thing PR #12
was written to fix — is unverified by the test suite.

The rest of the gaps in the encoder/decoder core (`mkh_avro2.hh`,
`mkh_avro_decoder.cc`, `schema_item.hh`) are defensive branches: malformed-
input rejections (`enif_is_map` failing, `enif_map_iterator_create` failing),
and a couple of unreachable `default:`/debug-print cases. Lower risk than
the decode-exception gap above, but worth revisiting if pushing toward
95%+.

## Related

- `docs/array-string-encoding-optimization-attempt.md` — a related
  measurement exercise (profiling-driven, not coverage) from the same
  investigation thread.
