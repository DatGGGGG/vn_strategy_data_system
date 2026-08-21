#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/ops/export_vm_delta_bundle.sh --since <loaded_at-cutoff>

Status:
  Not enabled yet. SensorTower modeling tables need reliable loaded_at/load-run
  metadata before safe delta export is possible.
EOF
}

if [[ "${1:-}" != "--since" || -z "${2:-}" ]]; then
  usage
  exit 64
fi

ops_init "vm_delta_export"
ops_load_env
ops_common_defaults
ops_activate_venv
ops_environment_snapshot

cat <<'EOF'
Delta export is intentionally blocked.

Reason:
  The VM migration playbook requires delta bundles to use loaded_at, not metric
  date. The current SensorTower core schema does not yet define reliable
  loaded_at columns across the large fact tables.

Next implementation step:
  Add load metadata to modeling fact loads, backfill it for existing rows where
  possible, then implement SQL-backed delta export safely.
EOF

exit 78
