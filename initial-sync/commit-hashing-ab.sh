#!/usr/bin/env bash
# Prepared submitblock phases: baseline then one-hash indexes, interleaved per fixture/repetition.
set -euo pipefail
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    cat <<'EOF'
Usage: BASE_BIN=... ONEHASH_BIN=... [BENCH_FIXTURES=...] bash commit-hashing-ab.sh

Environment:
  BASE_BIN, ONEHASH_BIN  Required executables with Prometheus metrics
  BENCH_FIXTURES  Sealed corpus directory (default: repository fixtures/regtest)
  OUTPUT         Default: repository output/initial-sync/commit-hashing-ab-<UTC timestamp>
  CPUS           Optional taskset CPU list; unset/all inherits current affinity
  REPS           10
  PROVENANCE     Optional JSON arm receipts; verifies labels base and onehash
Runs transparent-1900000, mixed-1900000, orchard-1900000 per rep, base then onehash.
Historical comparison build receipts were not recovered; record your own builds.
EOF
    exit 0
fi
[[ $# == 0 ]] || { echo 'Use --help; configure this wrapper through environment variables.' >&2; exit 2; }
: "${BASE_BIN:?Set BASE_BIN}" "${ONEHASH_BIN:?Set ONEHASH_BIN}"
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
out=${OUTPUT:-$root/output/initial-sync/commit-hashing-ab-$(date -u +%Y%m%dT%H%M%SZ)}
reps=${REPS:-10}
[[ $reps =~ ^[1-9][0-9]*$ ]] || { echo 'REPS must be positive' >&2; exit 2; }
command=(python3 -B "$root/regtest/phase-probe.py" --fixtures "${BENCH_FIXTURES:-$root/fixtures/regtest}")
if [[ -n ${PROVENANCE:-} ]]; then command+=(--provenance "$PROVENANCE"); fi
if [[ -n ${CPUS:-} && $CPUS != all ]]; then command=(taskset -c "$CPUS" "${command[@]}"); fi
for ((rep = 1; rep <= reps; rep++)); do
    for fixture in transparent-1900000 mixed-1900000 orchard-1900000; do
        "${command[@]}" "$BASE_BIN" base "$fixture" 1 "$out/base-$fixture-$rep"
        "${command[@]}" "$ONEHASH_BIN" onehash "$fixture" 1 "$out/onehash-$fixture-$rep"
    done
done
python3 -B "$root/initial-sync/phase-summary.py" "$out"/*/samples.jsonl
echo "$out"
