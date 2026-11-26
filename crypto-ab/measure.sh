#!/usr/bin/env bash
# Twenty cold submitblock rounds, with arm order reversed on alternating rounds.
set -euo pipefail
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
root=${RUN_ROOT:-$here/output/crypto-ab}
bench=$here/regtest
fixtures=${BENCH_FIXTURES:-$here/fixtures/zakura-1.8.1/standard}
signed=${BENCH_SIGNED_FIXTURES:-$here/fixtures/transparent-signed}
read -r -a arm_list <<< "${ARMS:-zebra zakura}"
read -r -a fixture_list <<< "${FIXTURE_LIST:-orchard-250000 orchard-1900000 mixed-1900000}"
(( ${#arm_list[@]} >= 2 && ${#fixture_list[@]} > 0 )) || {
    echo 'ARMS needs at least two binaries and FIXTURE_LIST must not be empty' >&2; exit 2; }
threads=${THREADS:-$(if [[ -n ${V5_CPUS:-} ]]; then taskset -c "$V5_CPUS" nproc; else nproc; fi)}
[[ $threads =~ ^[1-9][0-9]*$ ]] || { echo 'THREADS must be positive' >&2; exit 2; }
[[ -r $bench/phase-probe.py ]] || { echo 'Missing phase-probe.py' >&2; exit 2; }
root=$(cd -- "$root" && pwd -P)
declare -A labels fixture_dirs
for arm in "${arm_list[@]}"; do
    [[ -z ${labels[$arm]:-} ]] || { echo "Duplicate arm: $arm" >&2; exit 2; }
    [[ $arm =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ && -x $root/bins/$arm ]] || {
        echo "Missing/invalid arm: $arm" >&2; exit 2; }
    [[ -r $root/bins/$arm.build.json ]] || { echo "Missing build receipt: $arm" >&2; exit 2; }
    labels[$arm]=$(python3 - "$root/bins/$arm" <<'PY'
import hashlib
import json
from pathlib import Path
import shlex
import sys

binary = Path(sys.argv[1])
receipt = json.loads(binary.with_name(binary.name + '.build.json').read_text())
with binary.open('rb') as stream:
    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
if receipt.get('sha256') != digest:
    print(f'Build receipt SHA-256 mismatch: {binary}', file=sys.stderr)
    raise SystemExit(2)
build = shlex.split(receipt.get('build', ''))
if '-p' not in build or build.index('-p') + 1 == len(build):
    print(f'Build receipt lacks package: {binary}', file=sys.stderr)
    raise SystemExit(2)
package = build[build.index('-p') + 1]
if package not in ('zebrad', 'zakura'):
    print(f'Unknown arm package {package}: {binary}', file=sys.stderr)
    raise SystemExit(2)
print('zakura-main' if package == 'zakura' else 'zebra-optimized')
PY
    )
done
for fixture in "${fixture_list[@]}"; do
    [[ -z ${fixture_dirs[$fixture]:-} ]] || { echo "Duplicate fixture: $fixture" >&2; exit 2; }
    [[ $fixture =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || {
        echo "Invalid fixture: $fixture" >&2; exit 2; }
    fixture_dir=$fixtures
    [[ $fixture != transparent-signed-* ]] || fixture_dir=$signed
    if [[ -n ${EXTRA_FIXTURES:-} && -r $EXTRA_FIXTURES/blocks/$fixture.json ]]; then
        fixture_dir=$EXTRA_FIXTURES
    fi
    [[ -r $fixture_dir/manifest.sha256 && -r $fixture_dir/blocks/$fixture.json ]] || {
        echo "Missing sealed fixture: $fixture in $fixture_dir" >&2; exit 2; }
    fixture_dirs[$fixture]=$(cd -- "$fixture_dir" && pwd -P)
done
out=${MEASURE_ROOT:-$root/measure-$(date -u +%Y%m%dT%H%M%SZ)-$$}
mkdir -- "$out" "$out/bins" "$out/runs" "$out/logs"
out=$(cd -- "$out" && pwd -P)
for arm in "${arm_list[@]}"; do
    cp -- "$root/bins/$arm.build.json" "$out/$arm.build.json"
done
cleanup() {
    find "$out/runs" -type d -name state -prune -exec rm -rf -- {} +
    rm -rf -- "$out/bins"
}
trap cleanup EXIT
for arm in "${arm_list[@]}"; do cp -- "$root/bins/$arm" "$out/bins/$arm"; done
printf 'threads=%s arms=%s fixtures=%s v5_cpus=%s\n' \
    "$threads" "${ARMS:-zebra zakura}" "${FIXTURE_LIST:-orchard-250000 orchard-1900000 mixed-1900000}" \
    "${V5_CPUS:-inherited}" | tee "$out/settings.txt"
for fixture in "${fixture_list[@]}"; do
    fixture_dir=${fixture_dirs[$fixture]}
    for round in $(seq 1 20); do
        if ((round % 2)); then arms=("${arm_list[@]}")
        else arms=(); for ((i = ${#arm_list[@]} - 1; i >= 0; i--)); do arms+=("${arm_list[i]}"); done; fi
        for arm in "${arms[@]}"; do
            label=${labels[$arm]}
            sample="$out/runs/$fixture-r$round-$arm"
            command=(python3 -B "$bench/phase-probe.py" "$out/bins/$arm" "$label" "$fixture" 1 "$sample"
                --fixtures "$fixture_dir" --endpoint cold-submitblock-nominer
                --parallel-cpu-threads "$threads")
            if [[ -n ${V5_CPUS:-} ]]; then command=(taskset -c "$V5_CPUS" "${command[@]}"); fi
            "${command[@]}" > "$out/logs/$fixture-r$round-$arm.log" 2>&1 || {
                echo "Failed: $fixture r$round $arm; see $out/logs/$fixture-r$round-$arm.log" >&2; exit 1; }
            python3 -c 'import json,sys
for line in sys.stdin:
    print(json.dumps(dict(json.loads(line), arm=sys.argv[1], fixture=sys.argv[2],
                          round=int(sys.argv[3])), separators=(",", ":")))' \
                "$arm" "$fixture" "$round" < "$sample/samples.jsonl" >> "$out/rows.jsonl"
        done
    done
done
(cd "$out" && sha256sum bins/* > binaries.sha256)
python3 "$here/crypto-ab/report.py" "$out" | tee "$out/summary.txt"
printf 'Results: %s\n' "$out"
