#!/usr/bin/env bash
# Time esoconv on every .eso file in a directory with hyperfine.
#
#   bench/run.sh <eso dir> <results dir>
#
# Environment:
#   ESOCONV    binary under test            (default target/release/esoconv)
#   BASELINE   second esoconv binary, timed back to back with ESOCONV on the
#              same machine; the report shows the difference (optional)
#   ROUNDS     how often each case is repeated  (default 3)
#   RUNS       timed runs per command and round (default 7)
#              With a baseline the rounds alternate between the two binaries,
#              so a slow spell on the machine hits both.
#   BENCH_TMP  where converted output goes  (default /dev/shm: a RAM disk
#              keeps the runner's disk out of the measurement)
#   OTHERS     "1" to also time other ESO readers, once each, on the file
#              named by OTHERS_FILE (default: the smallest input)
#   READVARS   path to EnergyPlus's ReadVarsESO      (used when OTHERS=1)
#   PYTHON     interpreter with the Python readers   (used when OTHERS=1)
#
# One hyperfine JSON file per input, case and round lands in the results
# directory; bench/report.py turns them into a table.
set -euo pipefail

esodir=$(cd "$1" && pwd)
mkdir -p "$2"
res=$(cd "$2" && pwd)
here=$(cd "$(dirname "$0")" && pwd)

esoconv=$(realpath "${ESOCONV:-target/release/esoconv}")
baseline=${BASELINE:+$(realpath "$BASELINE")}
rounds=${ROUNDS:-3}
runs=${RUNS:-7}
tmp=$(mktemp -d -p "${BENCH_TMP:-/dev/shm}" esobench.XXXXXX)
trap 'rm -rf "$tmp"' EXIT

# name | esoconv arguments
cases=(
    "wide|-f wide -o $tmp/o.csv"
    "wide-1thread|-f wide -j 1 -o $tmp/o.csv"
    "long|-f long -o $tmp/o.csv"
    "parquet|-f parquet -o $tmp/o.parquet"
    "sqlite|-f sqlite -o $tmp/o.sqlite"
    "bin|-f bin -o $tmp/o.esobin"
)

rm -f "$res"/*.json
shopt -s nullglob
inputs=("$esodir"/*.eso)
[ ${#inputs[@]} -gt 0 ] || { echo "no .eso files in $esodir" >&2; exit 1; }

for eso in "${inputs[@]}"; do
    name=$(basename "$eso" .eso)
    cat "$eso" >/dev/null # warm the page cache
    for c in "${cases[@]}"; do
        case=${c%%|*}
        args=${c#*|}
        cmds=(-n head "$esoconv $eso $args")
        [ -n "$baseline" ] && cmds+=(-n base "$baseline $eso $args")
        # Every run starts without output files, so each one pays for the
        # same page allocations.
        for round in $(seq "$rounds"); do
            hyperfine -N --warmup 1 --runs "$runs" --style basic \
                --prepare "find $tmp -type f -delete" \
                --export-json "$res/$name@$case@$round.json" "${cmds[@]}"
        done
    done
    find "$tmp" -type f -delete
done

[ "${OTHERS:-0}" = 1 ] || exit 0

# Other ESO readers, for scale. They are orders of magnitude slower, so each
# gets a single run on one file and a time limit. A reader that fails or is
# missing is reported and skipped.
if [ -n "${OTHERS_FILE:-}" ]; then
    eso="$esodir/$OTHERS_FILE.eso"
else
    eso=$(ls -Sr "${inputs[@]}" | head -n 1)
fi
name=$(basename "$eso" .eso)
py=${PYTHON:-python3}
limit=${OTHERS_TIMEOUT:-1200}

other() { # other <label> <command...>
    label=$1
    shift
    if ! hyperfine -N --runs 1 --style basic --export-json "$res/$name@other-$label.json" \
        -n "$label" "timeout $limit $*"; then
        echo "::warning::benchmark of $label failed or timed out"
        rm -f "$res/$name@other-$label.json"
    fi
}

if [ -n "${READVARS:-}" ]; then
    # ReadVarsESO takes its input and output names from an .rvi file;
    # "unlimited" lifts its 255 column cap.
    printf '%s\n%s\n0\n' "$eso" "$tmp/readvars.csv" >"$tmp/in.rvi"
    other ReadVarsESO "$(realpath "$READVARS") $tmp/in.rvi unlimited"
fi
for tool in esoreader db-eplusout-reader opyplus; do
    other "$tool" "$py $here/others.py $tool $eso $tmp"
    find "$tmp" -type f ! -name in.rvi -delete
done
