# Worker unit names and groups for prod/test NATS stacks.
# shellcheck shell=bash

# shellcheck source=scripts/lib/gpu_ssh.sh
source "$(dirname "${BASH_SOURCE[0]}")/gpu_ssh.sh"

gpu_worker_unit() {
  case "$1" in
    prod-docling) echo "smoldocling-docling-worker.service" ;;
    prod-chunk) echo "smoldocling-docling-chunk-worker.service" ;;
    prod-kg) echo "smoldocling-kg-gliner-worker.service" ;;
    test-docling) echo "smoldocling-docling-worker-test.service" ;;
    test-chunk) echo "smoldocling-docling-chunk-worker-test.service" ;;
    *) echo "unknown worker: $1" >&2; return 1 ;;
  esac
}

gpu_worker_dir() {
  case "$1" in
    prod-docling|prod-chunk|prod-kg) echo "$GPU_PROD_DIR" ;;
    test-docling|test-chunk) echo "$GPU_TEST_DIR" ;;
    *) return 1 ;;
  esac
}

gpu_worker_log() {
  case "$1" in
    prod-docling) echo "${GPU_PROD_DIR}/worker.log" ;;
    prod-chunk) echo "${GPU_PROD_DIR}/chunk-worker.log" ;;
    prod-kg) echo "${GPU_PROD_DIR}/kg_gliner.log" ;;
    test-docling) echo "${GPU_TEST_DIR}/worker-test.log" ;;
    test-chunk) echo "${GPU_TEST_DIR}/chunk-worker-test.log" ;;
    *) return 1 ;;
  esac
}

gpu_worker_label() {
  case "$1" in
    prod-docling) echo "prod docling parse (:4222)" ;;
    prod-chunk) echo "prod docling chunk CPU (:4222)" ;;
    prod-kg) echo "prod kg-gliner (:4222)" ;;
    test-docling) echo "test docling parse (:4223)" ;;
    test-chunk) echo "test docling chunk CPU (:4223)" ;;
    *) return 1 ;;
  esac
}

gpu_workers_expand() {
  local target="${1:-all}"
  case "$target" in
    prod) printf '%s\n' prod-docling prod-chunk prod-kg ;;
    test) printf '%s\n' test-docling test-chunk ;;
    docling) printf '%s\n' prod-docling ;;
    chunk) printf '%s\n' prod-chunk ;;
    kg|gliner|kg-gliner) printf '%s\n' prod-kg ;;
    all) printf '%s\n' prod-docling prod-chunk prod-kg test-docling test-chunk ;;
    prod-docling|prod-chunk|prod-kg|test-docling|test-chunk)
      printf '%s\n' "$target"
      ;;
    *)
      echo "unknown target: $target" >&2
      return 1
      ;;
  esac
}

gpu_worker_systemctl() {
  local action="$1"
  local worker="$2"
  local unit
  unit="$(gpu_worker_unit "$worker")" || return 1
  gpu_systemd_user "systemctl --user ${action} ${unit}"
}

gpu_worker_status_one() {
  local worker="$1"
  local unit dir log label
  unit="$(gpu_worker_unit "$worker")" || return 1
  dir="$(gpu_worker_dir "$worker")" || return 1
  log="$(gpu_worker_log "$worker")" || return 1
  label="$(gpu_worker_label "$worker")" || return 1

  echo "==> ${label} (${unit})"
  gpu_systemd_user "
    set +e
    active=\$(systemctl --user is-active ${unit} 2>/dev/null)
    enabled=\$(systemctl --user is-enabled ${unit} 2>/dev/null)
    echo \"  systemd: \${active} (enabled=\${enabled})\"
    if [ -f ${dir}/.env ]; then
      grep -E '^NATS_URL=' ${dir}/.env | head -1 | sed 's/^/  /'
    fi
    if [ -d ${dir}/.git ]; then
      cd ${dir} && git log -1 --oneline 2>/dev/null | sed 's/^/  git: /'
    fi
    if [ -f ${log} ]; then
      echo '  log (last 3):'
      tail -3 ${log} | sed 's/^/    /'
    else
      echo '  log: (missing)'
    fi
  "
  echo ""
}

gpu_worker_logs_one() {
  local worker="$1"
  local lines="${2:-40}"
  local follow="${3:-false}"
  local log
  log="$(gpu_worker_log "$worker")" || return 1
  if [ "$follow" = true ]; then
    gpu_smoldocling "tail -f ${log}"
  else
    gpu_smoldocling "tail -n ${lines} ${log}"
  fi
}
