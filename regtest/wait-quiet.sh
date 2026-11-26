#!/bin/bash
# Three consecutive quiet minutes, maximum two hours, before measurement.
set -euo pipefail
usage() {
    cat <<'HELP'
Usage: QUIET_DEVICE=DEVICE [QUIET_WRITE_MIB=20] bash wait-quiet.sh [--device DEVICE]
DEVICE is a whole-device name under /sys/block (for example sda or nvme0n1).
No device is guessed: select the disk hosting benchmark states and output.
Three consecutive 60-second intervals must have disk busy time <1%, writes
<QUIET_WRITE_MIB MiB and one-minute system load <2. Exit 3 if not quiet within 120 intervals.
--help prints this text without waiting. --device overrides QUIET_DEVICE.
HELP
}
device=${QUIET_DEVICE:-}
write_mib=${QUIET_WRITE_MIB:-20}
[[ $write_mib =~ ^[0-9]+$ ]] || { echo 'QUIET_WRITE_MIB must be a whole number of MiB.' >&2; exit 2; }
while (($#)); do
    case $1 in
        --help|-h) usage; exit 0 ;;
        --device) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; device=$2; shift 2 ;;
        *) usage >&2; exit 2 ;;
    esac
done
[[ $device =~ ^[a-zA-Z0-9_.-]+$ && -r /sys/block/$device/stat ]] || {
    echo 'Set QUIET_DEVICE or --device to an existing whole block device.' >&2
    usage >&2
    exit 2
}
quiet=0
for ((minute=0; minute<120; minute++)); do
    read -r -a a <"/sys/block/$device/stat"
    sleep 60
    read -r -a b <"/sys/block/$device/stat"
    read -r load _ </proc/loadavg
    busy=$((b[9] - a[9]))
    written=$(((b[6] - a[6]) * 512))
    if ((busy >= 0 && busy < 600 && written >= 0 && written < write_mib*1024*1024)) && [[ ${load%%.*} -lt 2 ]]; then
        quiet=$((quiet + 1))
    else
        quiet=0
    fi
    if ((quiet >= 3)); then
        echo "wait-quiet: host quiet $(date +%T)"
        exit 0
    fi
done
echo 'wait-quiet: host not quiet for three consecutive minutes within two hours; not measuring' >&2
exit 3
