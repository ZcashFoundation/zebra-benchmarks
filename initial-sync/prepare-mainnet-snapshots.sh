#!/usr/bin/env bash
# Online Mainnet sync with sampled resources and verified stopped-state snapshots.
set -euo pipefail
if [[ ${1:-} == --help || ${1:-} == -h || $# == 0 ]]; then
    cat <<'EOF'
Usage: [STOP_AT=HEIGHT] bash prepare-mainnet-snapshots.sh BINARY RUN_DIRECTORY

Syncs public Mainnet, or resumes RUN_DIRECTORY/state. Use a compatible archive
binary; never run two nodes against the same state. With STOP_AT, a successful
configured-height stop is required before reflinking the stopped database.
Without STOP_AT, syncs until you create RUN_DIRECTORY/STOP (or interrupt it).
Environment:
  STOP_AT          Finalized stop height; unset means follow the public tip
  SNAPSHOT_OUTPUT  Default: RUN_DIRECTORY/snapshot-HEIGHT (must not exist)
  CPUS             Optional taskset CPU list; unset/all inherits process affinity
  RPC_PORT         28232 (loopback-only, cookie auth disabled)
  P2P_PORT         28233 (public Mainnet listener)
  SAMPLE_SECONDS   60; state size is sampled every tenth observation
  STARTUP_TIMEOUT  1200 seconds for RPC readiness
Requires Linux, curl, GNU coreutils, Python 3.11+, and a reflink-capable filesystem.
Re-running the same directory clears its STOP marker and appends samples/logs.
Prepare replay inputs with successive STOP_AT=1750000 and STOP_AT=2316084 runs;
these are Mainnet snapshots, not Regtest fixtures. No node binary is built here.
EOF
    exit 0
fi
[[ $# == 2 ]] || { echo 'Expected BINARY RUN_DIRECTORY; use --help.' >&2; exit 2; }
if [[ -n ${STOP_AT:-} && ! $STOP_AT =~ ^(0|[1-9][0-9]*)$ ]]; then
    echo 'STOP_AT must be a nonnegative integer height.' >&2; exit 2
fi
if [[ -n ${SNAPSHOT_OUTPUT:-} && -z ${STOP_AT:-} ]]; then
    echo 'SNAPSHOT_OUTPUT requires STOP_AT.' >&2; exit 2
fi
for value in "${RPC_PORT:-28232}" "${P2P_PORT:-28233}"; do
    [[ $value =~ ^[1-9][0-9]{0,4}$ ]] && ((value <= 65535)) || { echo 'Ports must be in 1..65535.' >&2; exit 2; }
done
for value in "${SAMPLE_SECONDS:-60}" "${STARTUP_TIMEOUT:-1200}"; do
    [[ $value =~ ^[1-9][0-9]*$ ]] || { echo 'Intervals must be positive integer seconds.' >&2; exit 2; }
done
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin=$(realpath -e -- "$1")
run=$(realpath -m -- "$2")
[[ -x $bin ]] || { echo 'BINARY must be executable.' >&2; exit 2; }
snapshot=${SNAPSHOT_OUTPUT:-$run/snapshot-${STOP_AT:-tip}}
if [[ -n ${STOP_AT:-} && ( -e $snapshot || -L $snapshot ) ]]; then
    echo "Snapshot destination already exists: $snapshot" >&2; exit 2
fi
rpc=127.0.0.1:${RPC_PORT:-28232}
mkdir -p "$run/state"
rm -f -- "$run/STOP"
stage=${STOP_AT:-tip}
sha256sum "$bin" >"$run/binary.sha256"
"$bin" generate -o "$run/zebrad.toml"
python3 -B - "$root/regtest" "$run" "$rpc" "${P2P_PORT:-28233}" "${STOP_AT:-}" <<'PY'
import json
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from front_common import set_key
run, rpc, p2p_port, stop_at = sys.argv[2:]
path = Path(run) / "zebrad.toml"
text = path.read_text()
edits = [
    ("network", "network", '"Mainnet"'),
    ("network", "listen_addr", json.dumps(f"0.0.0.0:{p2p_port}")),
    ("network", "cache_dir", json.dumps(str(Path(run) / "network"))),
    ("state", "cache_dir", json.dumps(str(Path(run) / "state"))),
    ("rpc", "listen_addr", json.dumps(rpc)),
    ("rpc", "enable_cookie_auth", "false"),
    ("tracing", "use_color", "false"),
    ("tracing", "force_use_color", "false"),
]
if stop_at:
    edits.append(("state", "debug_stop_at_height", stop_at))
for section, key, value in edits:
    text = set_key(text, section, key, value)
path.write_text(text)
PY
call() {
    curl -fsS -m 10 -H 'content-type: application/json' \
        --data-binary "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":[]}" "http://$rpc/" 2>/dev/null |
        python3 -c "import json, sys; r = json.load(sys.stdin)['result']; print($2)" 2>/dev/null || echo ""
}
stop_node() {
    if kill -0 "$pid" 2>/dev/null; then
        kill -INT "$pid" 2>/dev/null || true
        local deadline=$((SECONDS + 120))
        while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do sleep 1; done
        if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
    fi
}
log_offset=$(stat -c %s "$run/zebrad.log" 2>/dev/null || echo 0)
start=$(date +%s.%N)
command=("$bin" -c "$run/zebrad.toml" start)
if [[ -n ${CPUS:-} && $CPUS != all ]]; then command=(taskset -c "$CPUS" "${command[@]}"); fi
"${command[@]}" >>"$run/zebrad.log" 2>&1 &
pid=$!
trap 'stop_node; wait "$pid" 2>/dev/null || true' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
echo "$pid $start" >"$run/pid"
echo "start stage=$stage $(date -u +%FT%TZ)" >>"$run/events.txt"
[[ -e $run/samples.csv ]] || echo 'unix_s,rss_kib,cpu_s,height,estimated_height,peers,state_bytes' >"$run/samples.csv"
deadline=$((SECONDS + ${STARTUP_TIMEOUT:-1200}))
while kill -0 "$pid" 2>/dev/null; do
    if [[ -e $run/STOP ]]; then stop_node; break; fi
    if [[ -n $(call getblockcount 'r') ]]; then
        echo "rpc_ready_after_s $(python3 -c "print(round($(date +%s.%N) - $start, 3))")" >>"$run/events.txt"
        break
    fi
    ((SECONDS < deadline)) || { echo 'RPC startup timeout' >&2; exit 1; }
    sleep 0.2
done
tick=0
height=
while kill -0 "$pid" 2>/dev/null; do
    if [[ -e $run/STOP ]]; then stop_node; break; fi
    rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null || echo '')
    cpu=$(awk -v hz="$(getconf CLK_TCK)" '{printf "%.1f", ($14 + $15) / hz}' "/proc/$pid/stat" 2>/dev/null || echo '')
    height=$(call getblockchaininfo 'r["blocks"]')
    estimated=$(call getblockchaininfo 'r.get("estimatedheight", "")')
    peers=$(call getpeerinfo 'len(r)')
    state=
    if ((tick % 10 == 0)); then state=$(du -sb "$run/state" | cut -f1); fi
    echo "$(date +%s),$rss,$cpu,$height,$estimated,$peers,$state" >>"$run/samples.csv"
    tick=$((tick + 1))
    sleep "${SAMPLE_SECONDS:-60}"
done
status=0
wait "$pid" || status=$?
trap - EXIT INT TERM
echo "stopped stage=$stage status=$status last_sampled_height=$height $(date -u +%FT%TZ)" >>"$run/events.txt"
if [[ -n ${STOP_AT:-} && ! -e $run/STOP ]]; then
    # Only this invocation's log can authorize the snapshot; earlier stops cannot.
    python3 -B - "$root/regtest" "$run" "$snapshot" "$STOP_AT" "$status" "$log_offset" <<'PY'
from pathlib import Path
import shutil
import sys
sys.path.insert(0, sys.argv[1])
from front_common import reflink_copy
from sync_replay import stop_height
run, snapshot = map(Path, sys.argv[2:4])
height, status, offset = map(int, sys.argv[4:])
if status != 0:
    raise SystemExit(f"snapshot refused: node exited with status {status}")
stage_log = run / f"stop-{height}.log"
with (run / "zebrad.log").open("rb") as source, stage_log.open("wb") as target:
    source.seek(offset)
    shutil.copyfileobj(source, target)
if stop_height(stage_log) != height:
    raise SystemExit(f"snapshot refused: no verified stop at {height}")
reflink_copy(run / "state", snapshot)
PY
    echo "snapshot $snapshot" >>"$run/events.txt"
fi
exit "$status"
