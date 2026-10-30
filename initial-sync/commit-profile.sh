#!/usr/bin/env bash
# Frame-pointer call stacks for prepared transparent 1.9 MB submission.
set -euo pipefail
if [[ ${1:-} == --help || ${1:-} == -h || $# == 0 ]]; then
    cat <<'EOF'
Usage: bash commit-profile.sh BINARY NAME

BINARY must have frame pointers and Prometheus metrics; Linux perf access is required.
Environment:
  BENCH_FIXTURES  Sealed corpus directory (default: repository fixtures/regtest)
  OUTPUT         Default: repository output/initial-sync/commit-profile-NAME-<UTC timestamp>
  CPUS           Optional taskset CPU list; unset/all inherits current affinity
  REPS           3
  PROVENANCE     Optional JSON arm receipts; verifies the supplied NAME
Profiles prepared-submitblock for transparent-1900000 with --perf --perf-callgraph fp.
EOF
    exit 0
fi
[[ $# == 2 ]] || { echo 'Expected BINARY NAME; use --help.' >&2; exit 2; }
[[ $2 =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo 'NAME must be an arm label, not a path.' >&2; exit 2; }
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
out=${OUTPUT:-$root/output/initial-sync/commit-profile-$2-$(date -u +%Y%m%dT%H%M%SZ)}
reps=${REPS:-3}
[[ $reps =~ ^[1-9][0-9]*$ ]] || { echo 'REPS must be positive' >&2; exit 2; }
command=(python3 -B "$root/regtest/phase-probe.py" --fixtures "${BENCH_FIXTURES:-$root/fixtures/regtest}" --perf --perf-callgraph fp)
if [[ -n ${PROVENANCE:-} ]]; then command+=(--provenance "$PROVENANCE"); fi
if [[ -n ${CPUS:-} && $CPUS != all ]]; then command=(taskset -c "$CPUS" "${command[@]}"); fi
for ((rep = 1; rep <= reps; rep++)); do
    "${command[@]}" "$1" "$2" transparent-1900000 1 "$out/$rep"
done
echo "$out"
