#!/usr/bin/env bash
# Deploy pdf main/production clone on GPU via git (from laptop, inside pdf repo).
#
#   git push origin main
#   ./scripts/laptop_deploy_prod.sh
#   ./scripts/laptop_deploy_prod.sh --no-restart
#   ./scripts/laptop_deploy_prod.sh --reset
#
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/gpu_ssh.sh
source "$(dirname "$0")/lib/gpu_ssh.sh"

BRANCH="${GPU_GIT_BRANCH:-main}"
REMOTE="${GPU_GIT_REMOTE:-origin}"
RESTART=true
GIT_RESET=false

for arg in "$@"; do
  case "$arg" in
    --no-restart) RESTART=false ;;
    --reset) GIT_RESET=true ;;
    -h|--help)
      sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
  esac
done

RESET_CMD="git merge --ff-only ${REMOTE}/${BRANCH}"
[ "$GIT_RESET" = true ] && RESET_CMD="git reset --hard ${REMOTE}/${BRANCH}"

echo "==> prod deploy ${REMOTE}/${BRANCH} -> ${GPU_PROD_DIR}"
gpu_smoldocling "
  set -e
  cd ${GPU_PROD_DIR}
  git fetch ${REMOTE} ${BRANCH}
  ${RESET_CMD}
  git log -1 --oneline
  source venv/bin/activate
  pip install -U pip wheel
  if [ -f constraints-cu12.txt ]; then
    pip install -c constraints-cu12.txt -r requirements.txt
  else
    pip install -r requirements.txt
  fi
  pip install -r requirements-gliner.txt
  if [ -f scripts/cudnn_probe.py ]; then python scripts/cudnn_probe.py; fi
  python scripts/verify_torch_import.py
  pytest tests/test_bootstrap_gpu.py tests/test_gpu_memory_config.py \
    tests/test_verify_torch_import.py tests/test_worker_runtime.py \
    tests/test_result_publish.py tests/test_parse_artifact_storage.py -m unit -q --no-cov
  ./scripts/install_systemd_services.sh
  python scripts/ensure_documents_stream.py
"

if [ "$RESTART" = true ]; then
  gpu_systemd_user "
    systemctl --user restart smoldocling-docling-worker.service \
      smoldocling-docling-chunk-worker.service smoldocling-kg-gliner-worker.service
    systemctl --user is-active smoldocling-docling-worker.service \
      smoldocling-docling-chunk-worker.service smoldocling-kg-gliner-worker.service
  "
fi
echo "Done."
