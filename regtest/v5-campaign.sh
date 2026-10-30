#!/bin/bash
# The planner applies V5_CPUS to workloads, not the driver: replay sources may
# use a different SOURCE_CPUS mask. Unset masks preserve inherited affinity.
set -euo pipefail
exec python3 -B "$(dirname "$(realpath "$0")")/v5_campaign.py" "$@"
