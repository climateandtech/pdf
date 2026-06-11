#!/usr/bin/env bash
# SSH to GPU and run gpu_recover_workers.sh without mutating git.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/gpu_ssh.sh
source "$(dirname "$0")/lib/gpu_ssh.sh"

echo "==> GPU worker recovery (no git reset) -> ${GPU_PROD_DIR}"
gpu_systemd_user "cd ${GPU_PROD_DIR} && ./scripts/gpu_recover_workers.sh"
