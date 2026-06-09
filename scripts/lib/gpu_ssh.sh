# Shared SSH helpers for laptop → GPU smoldocling (systemd user bus).
# shellcheck shell=bash

GPU_SSH_HOST="${GPU_SSH_HOST:-gpu}"
GPU_WORKER_USER="${GPU_WORKER_USER:-smoldocling}"
SMOLDOCLING_UID="${SMOLDOCLING_UID:-1003}"
GPU_PROD_DIR="${GPU_PROD_DIR:-/home/smoldocling/apps/pdf}"
GPU_TEST_DIR="${GPU_TEST_DIR:-/home/smoldocling/apps/pdf-test}"

gpu_ssh() {
  ssh -o BatchMode=yes "${GPU_SSH_HOST}" "$@"
}

gpu_smoldocling() {
  gpu_ssh "sudo -u ${GPU_WORKER_USER} bash -lc $(printf '%q' "$1")"
}

gpu_systemd_user() {
  gpu_ssh "sudo -u ${GPU_WORKER_USER} env XDG_RUNTIME_DIR=/run/user/${SMOLDOCLING_UID} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${SMOLDOCLING_UID}/bus bash -lc $(printf '%q' "$1")"
}
