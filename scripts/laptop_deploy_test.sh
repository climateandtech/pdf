#!/usr/bin/env bash
# Deploy pdf to ~/apps/pdf-test on GPU via git (ct-nats-test :4223).
#
#   git push origin main
#   ./scripts/laptop_deploy_test.sh --branch main
#   ./scripts/laptop_deploy_test.sh --no-restart
#
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/gpu_ssh.sh
source "$(dirname "$0")/lib/gpu_ssh.sh"

BRANCH="${GPU_GIT_BRANCH:-main}"
REMOTE="${GPU_GIT_REMOTE:-origin}"
RESTART=true
GIT_RESET=false

while [ $# -gt 0 ]; do
  case "$1" in
    --branch) BRANCH="${2:?}"; shift 2 ;;
    --reset) GIT_RESET=true; shift ;;
    --no-restart) RESTART=false; shift ;;
    -h|--help)
      sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
done

RESET_CMD="git merge --ff-only ${REMOTE}/${BRANCH}"
[ "$GIT_RESET" = true ] && RESET_CMD="git reset --hard ${REMOTE}/${BRANCH}"

echo "==> test deploy ${REMOTE}/${BRANCH} -> ${GPU_TEST_DIR}"
gpu_smoldocling "
  set -e
  cd ${GPU_TEST_DIR}
  if [ ! -d .git ]; then
    echo 'pdf-test clone missing — see docs/GPU_NATS_TEST.md one-time setup' >&2
    exit 1
  fi
  git fetch ${REMOTE} ${BRANCH}
  git checkout ${BRANCH} 2>/dev/null || git checkout -b ${BRANCH} ${REMOTE}/${BRANCH}
  ${RESET_CMD}
  git log -1 --oneline
  source venv/bin/activate
  pip install -r requirements.txt
  pytest tests/test_bootstrap_gpu.py tests/test_gpu_memory_config.py \
    tests/test_verify_torch_import.py tests/test_worker_runtime.py \
    tests/test_result_publish.py tests/test_parse_artifact_storage.py -m unit -q --no-cov
  ./scripts/install_systemd_test_workers.sh
  python scripts/ensure_documents_stream.py
"

if [ "$RESTART" = true ]; then
  gpu_systemd_user "
    systemctl --user restart smoldocling-docling-worker-test.service \
      smoldocling-docling-chunk-worker-test.service
    systemctl --user is-active smoldocling-docling-worker-test.service \
      smoldocling-docling-chunk-worker-test.service
  "
fi
echo "Done."
