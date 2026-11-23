#!/usr/bin/env bash
# C++ line/branch coverage of the NIF, measured by running the eunit suite
# against a build instrumented with GCC's --coverage. `rebar3 eunit --cover`
# cannot see the NIF, only the thin Erlang wrapper, so this is the number that
# says how much of the encoder/decoder the tests exercise.
#
# Needs gcc/g++ and gcovr (apt install gcovr). Run from the repository root.
#
# Writes: coverage-summary.json, coverage.xml (Cobertura), coverage.txt (per file)
# and prints "cpp-coverage-lines: NN.N" as its last line.
#
# The build is -O0 without LTO: with -flto the line counters do not map back to
# source lines reliably. It is a throwaway build, so priv/erlav_nif.so is
# replaced and c_src/*.o are removed first and again afterwards.
set -euo pipefail

cd "$(dirname "$0")/.."

command -v gcovr >/dev/null || { echo "cpp_coverage: gcovr not found" >&2; exit 2; }

clean() {
    find c_src -name '*.o' -delete
    find c_src \( -name '*.gcda' -o -name '*.gcno' -o -name '*.d' \) -delete
    rm -f priv/erlav_nif.so
}
clean
trap clean EXIT

# The flags go in through the environment, not the make command line: the
# Makefile appends -fPIC, the Erlang include paths and -std= to CXXFLAGS with +=,
# and a command-line value would replace all of that. LTOFLAGS is the exception:
# the Makefile assigns it with =, so only the command line can clear it.
# rebar3's compile pre-hook then finds the .so up to date and leaves it alone.
CXXFLAGS="-O0 -g --coverage -fprofile-update=atomic -Wall -include cstdint" \
CFLAGS="-O0 -g --coverage -fprofile-update=atomic" \
LDFLAGS="--coverage" \
    make -C c_src LTOFLAGS=

# -fprofile-update=atomic: several BEAM schedulers run NIF code concurrently.
rebar3 eunit

gcovr --root . \
    --object-directory c_src \
    --filter 'c_src/' --exclude 'c_src/include/' \
    --print-summary \
    --json-summary coverage-summary.json --json-summary-pretty \
    --xml coverage.xml \
    --txt coverage.txt

lines=$(python3 -c 'import json; print("%.1f" % json.load(open("coverage-summary.json"))["line_percent"])')
echo "cpp-coverage-lines: ${lines}"
