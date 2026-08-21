#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/ops/restore_vm_delta_bundle.sh --confirm-vm-delta-restore <bundle-dir>

Status:
  Not enabled until export_vm_delta_bundle.sh is implemented with reliable
  loaded_at/load-run metadata.
EOF
}

if [[ "${1:-}" != "--confirm-vm-delta-restore" || -z "${2:-}" ]]; then
  usage
  exit 64
fi

ops_init "vm_delta_restore"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_environment_snapshot

cat <<'EOF'
Delta restore is intentionally blocked.

Reason:
  Delta export is not safe yet because SensorTower core tables do not have
  reliable loaded_at/load-run metadata across all required fact tables.
EOF

exit 78
