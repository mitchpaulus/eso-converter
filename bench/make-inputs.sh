#!/usr/bin/env bash
# Generate the benchmark .eso files by running EnergyPlus on the example
# files listed in bench/inputs.txt.
#
#   bench/make-inputs.sh <EnergyPlus install dir> <output dir>
#
# Writes <output dir>/<ExampleFile>.eso. Every file is run for its own run
# periods against the Chicago TMY3 weather file that ships with EnergyPlus.
set -euo pipefail

ep=$(cd "$1" && pwd)
mkdir -p "$2"
out=$(cd "$2" && pwd)
list=$(dirname "$0")/inputs.txt
epw="$ep/WeatherData/USA_IL_Chicago-OHare.Intl.AP.725300_TMY3.epw"
exe="$ep/energyplus"
[ -x "$exe" ] || exe="$ep/energyplus.exe"

run_one() {
    name=$1
    # ExpandObjects (-x) works in the current directory, so every run gets
    # its own.
    work=$(mktemp -d)
    cd "$work"
    cp "$ep/ExampleFiles/$name.idf" in.idf
    if ! "$exe" -x -w "$epw" -d "$work" in.idf >log.txt 2>&1; then
        tail -n 20 log.txt >&2
        echo "EnergyPlus failed on $name" >&2
        exit 1
    fi
    mv eplusout.eso "$out/$name.eso"
    cd / && rm -rf "$work"
    echo "$name: $(stat -c %s "$out/$name.eso") bytes"
}
export -f run_one
export ep out epw exe

grep -v '^\s*\(#\|$\)' "$list" | awk '{print $1}' |
    xargs -P "$(nproc)" -I{} bash -euo pipefail -c 'run_one {}'
