#!/usr/bin/env bash
# The first arm supplies all four source nodes: reference sources for this run.
set -euo pipefail
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    cat <<'EOF'
Usage: REFERENCE_BIN=... OPTIMIZED_BIN=... PROVENANCE=... SNAPSHOT=... SOURCE_STATE=... bash run-sync-full.sh

Checkpoint sync from genesis, reference first and optimized second. Inputs must
be stopped state.cache_dir roots. The first arm supplies every source node.
Required environment:
  REFERENCE_BIN, OPTIMIZED_BIN  Local executables (v7.0.0 and optimized-asm)
  PROVENANCE                   Local JSON arm receipts with actual SHA256 values
  SNAPSHOT                     Genesis-only snapshot-0
  SOURCE_STATE                 Synced source snapshot at or above END_HEIGHT
Optional environment:
  OUTPUT          Repository output/initial-sync/checkpoint-full-<UTC timestamp>
  END_HEIGHT      3522460
  REPS            1
  TIMEOUT         259200 seconds per attempt
  SOURCES         4
  SOURCE_PORT_BASE 31233 (consecutive loopback P2P ports)
  CPUS, SOURCE_CPUS  all (each inherits the current process affinity)
No CPUs are reserved automatically; choose disjoint masks for controlled timing.
EOF
    exit 0
fi
[[ $# == 0 ]] || { echo 'Use --help; configure this wrapper through environment variables.' >&2; exit 2; }
: "${REFERENCE_BIN:?Set REFERENCE_BIN}" "${OPTIMIZED_BIN:?Set OPTIMIZED_BIN}"
: "${PROVENANCE:?Set PROVENANCE}" "${SNAPSHOT:?Set SNAPSHOT}" "${SOURCE_STATE:?Set SOURCE_STATE}"
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
exec python3 -B "$root/regtest/sync_replay.py" \
    --provenance "$PROVENANCE" \
    --arm "v7.0.0=$REFERENCE_BIN" --arm "optimized-asm=$OPTIMIZED_BIN" \
    --snapshot "$SNAPSHOT" --source-state "$SOURCE_STATE" \
    --start-height 0 --end-height "${END_HEIGHT:-3522460}" --mode checkpoint \
    --repetitions "${REPS:-1}" --timeout "${TIMEOUT:-259200}" \
    --sources "${SOURCES:-4}" --source-port-base "${SOURCE_PORT_BASE:-31233}" \
    --output "${OUTPUT:-$root/output/initial-sync/checkpoint-full-$(date -u +%Y%m%dT%H%M%SZ)}" \
    --cpus "${CPUS:-all}" --source-cpus "${SOURCE_CPUS:-all}"
