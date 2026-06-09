#!/usr/bin/env bash
# Monitor/control GPU NATS workers from your laptop (run inside pdf repo).
#
#   ./scripts/laptop_workers.sh status
#   ./scripts/laptop_workers.sh status test
#   ./scripts/laptop_workers.sh restart prod
#   ./scripts/laptop_workers.sh logs test-chunk -n 50 -f
#
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/workers_lib.sh
source "$(dirname "$0")/lib/workers_lib.sh"

usage() {
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
}

CMD="${1:-status}"
shift || true

case "$CMD" in
  -h|--help|help) usage; exit 0 ;;
  status)
    TARGET="${1:-all}"
    while IFS= read -r worker; do
      gpu_worker_status_one "$worker"
    done < <(gpu_workers_expand "$TARGET")
    ;;
  start|stop|restart)
    TARGET="${1:-all}"
    while IFS= read -r worker; do
      echo "==> ${CMD} ${worker}"
      gpu_worker_systemctl "$CMD" "$worker"
    done < <(gpu_workers_expand "$TARGET")
    ;;
  logs)
    TARGET="${1:-prod-docling}"
    LINES=40
    FOLLOW=false
    shift || true
    while [ $# -gt 0 ]; do
      case "$1" in
        -f|--follow) FOLLOW=true; shift ;;
        -n) LINES="${2:?}"; shift 2 ;;
        prod|test|all|prod-docling|prod-chunk|prod-kg|test-docling|test-chunk)
          TARGET="$1"; shift ;;
        *) echo "Unknown: $1" >&2; exit 1 ;;
      esac
    done
    case "$TARGET" in
      prod) TARGET=prod-docling ;;
      test) TARGET=test-docling ;;
    esac
    gpu_worker_logs_one "$TARGET" "$LINES" "$FOLLOW"
    ;;
  *)
    echo "Unknown command: $CMD" >&2
    usage
    exit 1
    ;;
esac
