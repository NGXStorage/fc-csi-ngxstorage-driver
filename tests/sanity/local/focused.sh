#!/bin/bash

set -euo pipefail

if [ "$#" -ne 1 ] || [[ ! "$1" =~ ^[a-z]+\.go:[0-9]+$ ]]; then
    echo "usage: $0 <sanity-source.go:line>" >&2
    exit 2
fi

readonly FOCUS_FILE_ARG="$1"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly DRIVER="$SCRIPT_DIR/../../../cmd/csi-driver/fc-csi-ngxstorage-driver"
readonly SAFE_NAME="${FOCUS_FILE_ARG//[:\/]/-}"
readonly SCRATCH_ROOT="$HOME/.cache/ngxstorage"
case "$SCRATCH_ROOT" in /tmp|/tmp/*|/var/tmp|/var/tmp/*) echo "refusing temporary cache under /tmp or /var/tmp" >&2; exit 2 ;; esac
mkdir -p "$SCRATCH_ROOT"
readonly OUTPUT="$SCRATCH_ROOT/fc-sanity-$SAFE_NAME.out"

API_KEY="$(cat)"
if [ -z "$API_KEY" ]; then
    echo "empty API key on stdin" >&2
    exit 2
fi
export STORAGE_API_KEY="$API_KEY"

set +e
SUDO="" FOCUS_FILE="$FOCUS_FILE_ARG" bash "$SCRIPT_DIR/../../run-all-tests.sh" sanity local --allow-live >"$OUTPUT" 2>&1
sanity_status=$?
set -e

grep -E "Will run|Ran [0-9]+|SUCCESS|FAIL|Passed|ERROR" "$OUTPUT" | tail -n 20

find_driver_pids() {
    local proc exe
    for proc in /proc/[0-9]*; do
        exe="$(readlink "$proc/exe" 2>/dev/null || true)"
        if [ "$exe" = "$DRIVER" ] || [ "$exe" = "$DRIVER (deleted)" ]; then
            printf '%s\n' "${proc##*/}"
        fi
    done
}

remaining=()
for _ in $(seq 1 20); do
    mapfile -t remaining < <(find_driver_pids)
    [ "${#remaining[@]}" -eq 0 ] && break
    sleep 1
done
echo "EXACT_SANITY_PROCESSES_AFTER=${#remaining[@]}"
[ "${#remaining[@]}" -eq 0 ] || exit 1
exit "$sanity_status"
