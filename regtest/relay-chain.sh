#!/bin/bash
# Usage: relay-chain.sh [--dry-run] <label:binary-name>...
# Five interleaved repetitions; isolated A--B--C, 20-second links, cold/warm B and C.
set -euo pipefail
if [[ ${1:-} == --help || ${1:-} == -h || $# == 0 ]]; then
    echo "Usage: $0 [--dry-run] label:binary-name ..."
    echo 'Five interleaved repetitions; isolated A--B--C, 20-second links, cold/warm B and C.'
    echo 'BENCH_BINS=bins, BENCH_FIXTURES=fixtures/regtest, BENCH_SIGNED_FIXTURES=fixtures/transparent-signed, BENCH_OUTPUT=output (repository-relative defaults).'
    echo 'Overrides are invocation-relative. V5_CPUS optionally pins probes (default: inherited affinity).'
    echo 'BENCH_PORT_BASE=0 uses ephemeral loopback ports; nonzero reserves consecutive ports. QUIET_DEVICE is required for the quiet gate.'
    exit 0
fi
p=$(dirname "$(dirname "$(realpath "$0")")")
b=$(realpath -m "${BENCH_BINS:-$p/bins}")
f=$(realpath -m "${BENCH_FIXTURES:-$p/fixtures/regtest}")
s=$(realpath -m "${BENCH_SIGNED_FIXTURES:-$p/fixtures/transparent-signed}")
o=$(realpath -m "${BENCH_OUTPUT:-$p/output}")
cd "$(dirname "$0")"
dry=false
if [[ ${1:-} == --dry-run ]]; then dry=true; shift; fi
(($#)) || { echo "Usage: $0 [--dry-run] <label:binary-name>..." >&2; exit 2; }
arms=("$@")
declare -A labels=()
for arm in "${arms[@]}"; do
	[[ $arm =~ ^[a-zA-Z0-9_-]+:[a-zA-Z0-9_.-]+$ ]] || { echo "Invalid arm: $arm" >&2; exit 2; }
	label=${arm%%:*}
	[[ ! ${labels[$label]+present} ]] || { echo "Duplicate arm: $label" >&2; exit 2; }
	labels[$label]=1
	$dry || [[ -x $b/${arm#*:} ]] || { echo "Missing executable: $b/${arm#*:}" >&2; exit 1; }
done
run() {
    if [[ $1 == python3 && -n ${V5_CPUS:-} ]]; then set -- taskset -c "$V5_CPUS" "$@"; fi
	if $dry; then printf '%q ' "$@"; printf '\n'; else "$@"; fi
}
run bash wait-quiet.sh
stamp=$(date -u +%Y%m%dT%H%M%S)
$dry && stamp=DRY-RUN
o=$o/relay-chain-$stamp
if ! $dry; then mkdir -p "${o%/*}"; mkdir "$o"; fi
for rep in 1 2 3 4 5; do
	offset=$(((rep - 1) % ${#arms[@]}))
	order=("${arms[@]:offset}" "${arms[@]:0:offset}")
	for fixture in empty-2000 transparent-signed-1900000 orchard-1900000 mixed-1900000; do
        fixtures=$f
        [[ $fixture == transparent-signed-* ]] && fixtures=$s
		for temperature in cold warm; do
			for arm in "${order[@]}"; do
				run python3 -B sweep/relay_chain.py --fixtures "$fixtures" \
					--temperature "$temperature" --settle-seconds 20 --poll-ms 1 \
					"$b/${arm#*:}" "${arm%%:*}" "$fixture" \
					"$o/$temperature/$fixture/${arm%%:*}-$rep"
			done
		done
	done
done
if ! $dry; then printf '%s\n' 'All scheduled samples completed successfully.' >"$o/completion.txt"; fi
printf '%s\n' "$o"
