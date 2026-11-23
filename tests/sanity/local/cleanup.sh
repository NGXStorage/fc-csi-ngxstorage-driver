#!/bin/bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly DRIVER="$SCRIPT_DIR/../../../cmd/csi-driver/fc-csi-ngxstorage-driver"

find_driver_pids() {
    local proc exe
    for proc in /proc/[0-9]*; do
        exe="$(readlink "$proc/exe" 2>/dev/null || true)"
        if [ "$exe" = "$DRIVER" ] || [ "$exe" = "$DRIVER (deleted)" ]; then
            printf '%s\n' "${proc##*/}"
        fi
    done
}

mapfile -t pids < <(find_driver_pids)
echo "EXACT_SANITY_PROCESSES_BEFORE=${#pids[@]}"
if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}"
fi

for _ in $(seq 1 20); do
    mapfile -t remaining < <(find_driver_pids)
    [ "${#remaining[@]}" -eq 0 ] && break
    sleep 1
done

if [ "${#remaining[@]}" -gt 0 ]; then
    kill -KILL "${remaining[@]}"
    sleep 1
    mapfile -t remaining < <(find_driver_pids)
fi
echo "EXACT_SANITY_PROCESSES_AFTER=${#remaining[@]}"
[ "${#remaining[@]}" -eq 0 ]
