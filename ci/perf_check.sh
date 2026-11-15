#!/usr/bin/env bash
# Runs erlav_perf:all_tests/3 and fails if any benchmark's speedup over
# erlavro (measured in the same run, so machine speed mostly cancels out)
# drops below a floor, or if erlav and erlavro outputs stop matching.
#
# A single run can hit a GC-heavy outlier (e.g. decode_perf_tst3 occasionally
# runs ~4x slower in a VM left with a large heap by earlier benchmarks), so
# when a floor is missed the whole suite is re-run, up to `attempts` times,
# and each benchmark is judged by its best speedup across the attempts. A
# run with no failures costs a single pass. An erlav/erlavro mismatch is a
# correctness failure and is never retried.
#
# Usage: ci/perf_check.sh [iterations] [min_speedup] [attempts]
# Writes the raw log of all attempts to perf.log and a markdown table of the
# best result per benchmark to perf-summary.md.
set -euo pipefail

ITER="${1:-5000}"
MIN_SPEEDUP="${2:-2.0}"
ATTEMPTS="${3:-3}"

: > perf.log
best=$(mktemp)
trap 'rm -f "$best" "$best.new"' EXIT

# Table rows look like: | name | true | 1024.51 | 64.69 | 15.84x |
# Normalised to tab-separated: name equal erlavro_us erlav_us speedup
extract_rows() {
    grep -E '^\| [a-z_0-9]+ +\| (true|false) ' "$1" \
        | awk -F'|' '{for(i=2;i<=6;i++)gsub(/^ +| +$/,"",$i); sub(/x$/,"",$6); printf "%s\t%s\t%s\t%s\t%s\n",$2,$3,$4,$5,$6}' \
        || true
}

# Keeps, per benchmark, the row with the highest speedup; "equal" is false
# if it was false in any attempt. First-seen order is preserved.
merge_best() {
    awk -F'\t' -v OFS='\t' '
        { if (!($1 in row)) order[++n] = $1
          if ($2 != "true") bad[$1] = 1
          if (!($1 in row) || $5 + 0 > sp[$1] + 0) { row[$1] = $3 OFS $4; sp[$1] = $5 } }
        END { for (i = 1; i <= n; i++) { k = order[i]
                print k, (k in bad) ? "false" : "true", row[k], sp[k] } }'
}

failures() {
    local status=0 name equal _e _l speedup
    while IFS=$'\t' read -r name equal _e _l speedup; do
        if [ "$equal" != "true" ]; then
            echo "FAIL: $name: erlav output differs from erlavro" >&2; status=2
        elif awk -v s="$speedup" -v m="$MIN_SPEEDUP" 'BEGIN{exit !(s < m)}'; then
            echo "FAIL: $name: best speedup ${speedup}x < ${MIN_SPEEDUP}x" >&2
            [ "$status" -eq 0 ] && status=1
        fi
    done < "$best"
    return $status
}

attempt=0
status=1
while [ "$attempt" -lt "$ATTEMPTS" ]; do
    attempt=$((attempt + 1))
    echo "=== perf attempt ${attempt}/${ATTEMPTS} ===" | tee -a perf.log
    attempt_log=$(mktemp)
    erl -noshell -pa _build/*/lib/*/ebin \
        -eval "erlav_perf:all_tests(${ITER}, 50, null), halt()." | tee "$attempt_log" | tee -a perf.log >/dev/null
    rows=$(extract_rows "$attempt_log")
    rm -f "$attempt_log"
    if [ -z "$rows" ]; then
        echo "perf_check: no result rows found in attempt ${attempt}" >&2
        exit 2
    fi
    { cat "$best"; echo "$rows"; } | merge_best > "$best.new"
    mv "$best.new" "$best"

    status=0
    failures || status=$?
    # 0: all pass. 2: output mismatch (correctness, don't retry).
    if [ "$status" -ne 1 ]; then break; fi
    [ "$attempt" -lt "$ATTEMPTS" ] && echo "perf_check: floor missed, retrying" >&2
done

{
    echo "| Test | Equal | Erlavro us | Erlav us | Speedup |"
    echo "|---|---|---|---|---|"
    awk -F'\t' '{printf "| %s | %s | %s | %s | %sx |\n",$1,$2,$3,$4,$5}' "$best"
    echo
    echo "Best of ${attempt} attempt(s) per benchmark."
} > perf-summary.md

cat perf-summary.md
exit $status
