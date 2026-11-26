#!/usr/bin/env bash
# Start, stop, or follow logs for one node on its own SSH host.
set -euo pipefail

if (( $# != 4 )) || [[ ! $4 =~ ^(start|stop|logs)$ ]]; then
    printf 'usage: %s <host> <binary> <config> start|stop|logs\n' "$0" >&2
    exit 2
fi

printf -v remote 'bash -s -- %q %q %q' "$2" "$3" "$4"
ssh -- "$1" "$remote" <<'REMOTE'
set -euo pipefail
binary=$1 config=$2 action=$3
pid_file="${config}.pid"
log_file="${config}.log"
case "$action" in
    start)
        if [[ -f $pid_file ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
            printf 'already running: %s\n' "$pid_file" >&2
            exit 1
        fi
        nohup "$binary" --config "$config" start >>"$log_file" 2>&1 </dev/null &
        printf '%s\n' "$!" >"$pid_file"
        ;;
    stop)
        [[ -f $pid_file ]] || { printf 'no pid file: %s\n' "$pid_file" >&2; exit 1; }
        kill "$(<"$pid_file")"
        rm -- "$pid_file"
        ;;
    logs)
        tail -n 100 -f -- "$log_file"
        ;;
esac
REMOTE
