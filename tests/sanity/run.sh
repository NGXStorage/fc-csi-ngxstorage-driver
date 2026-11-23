#!/usr/bin/env bash
set -euo pipefail

sanity_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
  echo "Usage: bash tests/sanity/run.sh <local|cloud> [--allow-live]"
  exit 0
fi
case "${1:-}" in
  local|cloud) mode="$1"; shift ;;
  *) echo "Usage: bash tests/sanity/run.sh <local|cloud> [--allow-live]" >&2; exit 2 ;;
esac
if [[ "$mode" == local && "$EUID" == 0 ]]; then
  echo "Run local sanity as your normal user, without outer sudo. The local runner elevates driver and mount operations itself; outer sudo drops STORAGE_API_KEY." >&2
  exit 2
fi
exec bash "$sanity_dir/../run-all-tests.sh" sanity "$mode" "$@"
