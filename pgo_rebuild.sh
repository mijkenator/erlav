#!/bin/bash
# Profile-Guided Optimization rebuild.
#
# Runs the full PGO pipeline: build an instrumented NIF, exercise it against
# eunit + erlav_perf's benchmark schemas to collect real encode/decode branch
# frequencies, merge the profile (clang only), then rebuild the final .so
# using that profile. The end result replaces priv/erlav_nif.so exactly like
# rebuild.sh does -- this is just a slower variant with a training step in
# the middle.
#
# Not part of the normal build: rebar3 compile / rebuild.sh never touch PGO
# and stay fully reproducible. Run this manually when you want to try PGO's
# effect, and always re-verify with `rebar3 eunit` + erlav_perf before
# trusting the result -- see CLAUDE.md for context on why LTO already
# captured a lot of the available gain here.

set -euo pipefail
cd "$(dirname "$0")"

PGO_DIR="$(pwd)/c_src/pgo-data"
IS_CLANG=$(c++ --version 2>/dev/null | grep -qi clang && echo 1 || echo 0)

rm -rf "$PGO_DIR"
mkdir -p "$PGO_DIR"

echo "==> [1/4] Building instrumented NIF (PGO_PHASE=generate)"
find c_src -name "*.o" -delete
rm -f priv/erlav_nif.so
PGO_PHASE=generate PGO_DIR="$PGO_DIR" NATIVE_ARCH="${NATIVE_ARCH:-0}" rebar3 compile

echo "==> [2/4] Training: running eunit + erlav_perf against the instrumented NIF"
rebar3 as test compile >/dev/null
erl -pa _build/test/lib/*/ebin -pa _build/default/lib/*/ebin -noshell -eval '
    eunit:test(erlav_nif, [verbose]),
    erlav_perf:all_tests(20000, 50, null),
    init:stop().
'

if [ "$IS_CLANG" = "1" ]; then
    echo "==> [3/4] Merging profraw -> profdata (clang)"
    if ! ls "$PGO_DIR"/*.profraw >/dev/null 2>&1; then
        echo "No .profraw files were produced -- training run above must have failed." >&2
        exit 1
    fi
    xcrun llvm-profdata merge -output="$PGO_DIR/erlav_nif.profdata" "$PGO_DIR"/*.profraw
else
    echo "==> [3/4] Skipping merge step (gcc accumulates .gcda directly in $PGO_DIR)"
fi

echo "==> [4/4] Rebuilding final NIF using the collected profile (PGO_PHASE=use)"
find c_src -name "*.o" -delete
rm -f priv/erlav_nif.so
PGO_PHASE=use PGO_DIR="$PGO_DIR" NATIVE_ARCH="${NATIVE_ARCH:-0}" rebar3 compile

echo "==> Done. priv/erlav_nif.so was rebuilt with PGO."
echo "    Re-run 'rebar3 eunit' and erlav_perf:all_tests/3 to verify before trusting it."
