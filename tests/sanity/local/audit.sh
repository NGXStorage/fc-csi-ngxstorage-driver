#!/bin/bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly DRIVER="$SCRIPT_DIR/../../../cmd/csi-driver/fc-csi-ngxstorage-driver"

count=0
for proc in /proc/[0-9]*; do
    exe="$(readlink "$proc/exe" 2>/dev/null || true)"
    if [ "$exe" != "$DRIVER" ] && [ "$exe" != "$DRIVER (deleted)" ]; then
        continue
    fi
    state="$(awk '{print $3}' "$proc/stat" 2>/dev/null || true)"
    printf 'PID=%s STATE=%s EXE=%s\n' "${proc##*/}" "${state:-unknown}" "${exe:-none}"
    count=$((count + 1))
done
echo "EXACT_SANITY_PROCESSES=$count"
