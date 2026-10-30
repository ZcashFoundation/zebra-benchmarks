#!/usr/bin/env bash
# Prepared submitblock phases, eight repetitions per 1.9 MB fixture.
set -euo pipefail
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    cat <<'EOF'
Usage: OPTIMIZED_BIN=... [BENCH_FIXTURES=...] bash precommit-phase.sh

Environment:
  OPTIMIZED_BIN   Required executable with Prometheus metrics
  BENCH_FIXTURES  Sealed corpus directory (default: repository fixtures/regtest)
  OUTPUT         Default: repository output/initial-sync/precommit-phase-<UTC timestamp>
  CPUS           Optional taskset CPU list; unset/all inherits current affinity
  REPS           8
  PROVENANCE     Optional JSON arm receipts; verifies label optimized
Runs transparent-1900000, mixed-1900000, orchard-1900000, in that order per rep.
Prints phase medians; nested/overlapping phases are not additive CPU partitions.
EOF
    exit 0
fi
[[ $# == 0 ]] || { echo 'Use --help; configure this wrapper through environment variables.' >&2; exit 2; }
: "${OPTIMIZED_BIN:?Set OPTIMIZED_BIN}"
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
out=${OUTPUT:-$root/output/initial-sync/precommit-phase-$(date -u +%Y%m%dT%H%M%SZ)}
reps=${REPS:-8}
[[ $reps =~ ^[1-9][0-9]*$ ]] || { echo 'REPS must be positive' >&2; exit 2; }
command=(python3 -B "$root/regtest/phase-probe.py" --fixtures "${BENCH_FIXTURES:-$root/fixtures/regtest}")
if [[ -n ${PROVENANCE:-} ]]; then command+=(--provenance "$PROVENANCE"); fi
if [[ -n ${CPUS:-} && $CPUS != all ]]; then command=(taskset -c "$CPUS" "${command[@]}"); fi
for ((rep = 1; rep <= reps; rep++)); do
    for fixture in transparent-1900000 mixed-1900000 orchard-1900000; do
        "${command[@]}" "$OPTIMIZED_BIN" optimized "$fixture" 1 "$out/$fixture-$rep"
    done
done
python3 -B "$root/initial-sync/phase-summary.py" "$out"/*/samples.jsonl
