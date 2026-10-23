#!/usr/bin/env bash
# Runs erlav_perf:all_tests/3 and fails if any benchmark's speedup over
# erlavro (measured in the same run, so machine speed mostly cancels out)
# drops below a floor, or if erlav and erlavro outputs stop matching.
#
# Usage: ci/perf_check.sh [iterations] [min_speedup]
# Writes the raw log to perf.log and a markdown table to perf-summary.md.
set -euo pipefail

ITER="${1:-5000}"
MIN_SPEEDUP="${2:-2.0}"

erl -noshell -pa _build/*/lib/*/ebin \
    -eval "erlav_perf:all_tests(${ITER}, 50, null), halt()." | tee perf.log

# Table rows look like: | name | true | 1024.51 | 64.69 | 15.84x |
rows=$(grep -E '^\| [a-z_0-9]+ +\| (true|false) ' perf.log || true)
if [ -z "$rows" ]; then
    echo "perf_check: no result rows found in perf.log" >&2
    exit 2
fi

{
    echo "| Test | Equal | Erlavro us | Erlav us | Speedup |"
    echo "|---|---|---|---|---|"
    echo "$rows" | awk -F'|' '{for(i=2;i<=6;i++)gsub(/^ +| +$/,"",$i); printf "| %s | %s | %s | %s | %s |\n",$2,$3,$4,$5,$6}'
} > perf-summary.md

status=0
while IFS='|' read -r _ name equal _ _ speedup _; do
    name=$(echo "$name" | xargs); equal=$(echo "$equal" | xargs)
    speedup=$(echo "$speedup" | tr -d 'x ')
    if [ "$equal" != "true" ]; then
        echo "FAIL: $name: erlav output differs from erlavro" >&2; status=1
    fi
    if awk -v s="$speedup" -v m="$MIN_SPEEDUP" 'BEGIN{exit !(s < m)}'; then
        echo "FAIL: $name: speedup ${speedup}x < ${MIN_SPEEDUP}x" >&2; status=1
    fi
done <<< "$rows"
exit $status
