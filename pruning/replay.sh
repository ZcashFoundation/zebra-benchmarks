#!/usr/bin/env bash
set -euo pipefail
here=$(dirname -- "$(realpath -- "$0")")
exec python3 -B "$here/measure.py" replay "$@"
