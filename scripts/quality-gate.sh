#!/usr/bin/env bash
# Quality gate for pdf (parse modes, benchmark registry, scripts).
set -euo pipefail

QG_STRICT="${QG_STRICT:-1}"
MODE="${1:-full}"
shift || true

BASE_REF="${BASE_REF:-}"
SKIP_PYTEST=0
PATHS_FILE=""
SINGLE_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base) BASE_REF="$2"; shift 2 ;;
    --paths-file) PATHS_FILE="$2"; shift 2 ;;
    --skip-pytest) SKIP_PYTEST=1; shift ;;
    quick|full) MODE="$1"; shift ;;
    *) SINGLE_FILE="$1"; shift ;;
  esac
done

if [[ -z "$BASE_REF" ]]; then
  BASE_REF="${GITHUB_BASE_REF:+origin/$GITHUB_BASE_REF}"
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"
# shellcheck source=lib/git-changed.sh
source "$ROOT/scripts/lib/git-changed.sh"
# shellcheck source=lib/resolve-tools.sh
source "$ROOT/scripts/lib/resolve-tools.sh"
# shellcheck source=lib/coverage-regression.sh
source "$ROOT/scripts/lib/coverage-regression.sh"
qg_setup_path "$ROOT" "$REPO_ROOT"

FAILED=0
log() { echo "$@" >&2; }
PDF_AFFECTED_TESTS=()
PDF_AFFECTED_COV=()
PDF_AFFECTED_APP_FILES=()

CHANGED=()
if [[ -n "$PATHS_FILE" ]]; then
  while IFS= read -r _line; do
    [[ -n "$_line" ]] && CHANGED+=("$_line")
  done < <(qg_collect_paths_file "$PATHS_FILE" | grep -E '^pdf/' || true)
elif [[ -n "$SINGLE_FILE" ]]; then
  while IFS= read -r _line; do
    [[ -n "$_line" ]] && CHANGED+=("$_line")
  done < <(qg_collect_changed "$BASE_REF" "$SINGLE_FILE" | grep -E '^pdf/' || true)
else
  while IFS= read -r _line; do
    [[ -n "$_line" ]] && CHANGED+=("$_line")
  done < <(qg_collect_changed "$BASE_REF" "" | grep -E '^pdf/' || true)
fi

if [[ ${#CHANGED[@]} -eq 0 && "$MODE" == quick ]]; then
  log "quality-gate: no pdf changes (quick mode)"
  exit 0
fi

if [[ -n "$PATHS_FILE" && ${#CHANGED[@]} -gt 0 ]]; then
  log "quality-gate: session-scoped — fix ONLY failures for paths you touched:"
  for p in "${CHANGED[@]}"; do
    log "  $p"
  done
fi

py_files=()
for p in "${CHANGED[@]}"; do
  if [[ "$p" =~ ^pdf/(tests|scripts)/.*\.py$ || "$p" =~ ^pdf/(parse_modes|parser_registry)\.py$ ]]; then
    py_files+=("${p#pdf/}")
  fi
done

if [[ ${#py_files[@]} -gt 0 ]]; then
  log "=== pdf: ruff (changed files) ==="
  if ! qg_require_ruff; then
    exit 2
  fi
  "$ROOT/scripts/lint-changed.sh" "${py_files[@]}" || FAILED=1
fi

pdf_discover_tests() {
  PDF_AFFECTED_TESTS=()
  PDF_AFFECTED_COV=()
  PDF_AFFECTED_APP_FILES=()
  local rel base
  for p in "${@}"; do
    rel="${p#pdf/}"
    case "$rel" in
      parse_modes.py)
        PDF_AFFECTED_TESTS+=("tests/test_parse_modes.py")
        PDF_AFFECTED_COV+=("--cov=parse_modes")
        PDF_AFFECTED_APP_FILES+=("parse_modes.py")
        ;;
      parser_registry.py)
        PDF_AFFECTED_TESTS+=("tests/test_parser_registry.py")
        PDF_AFFECTED_COV+=("--cov=parser_registry")
        PDF_AFFECTED_APP_FILES+=("parser_registry.py")
        ;;
      scripts/parser_benchmark.py)
        PDF_AFFECTED_TESTS+=("tests/test_parser_benchmark_contract.py")
        PDF_AFFECTED_COV+=("--cov=parser_registry")
        PDF_AFFECTED_APP_FILES+=("parser_registry.py")
        ;;
      s3_bucket.py|s3_client.py|s3_config.py)
        PDF_AFFECTED_TESTS+=("tests/test_s3_bucket.py")
        PDF_AFFECTED_COV+=("--cov=s3_bucket")
        PDF_AFFECTED_APP_FILES+=("s3_bucket.py")
        ;;
      tests/test_s3_client_tdd.py)
        # Lint only in quick gate — moto/integration; unit coverage in test_s3_bucket.py
        ;;
      tests/test_s3_bucket.py)
        PDF_AFFECTED_TESTS+=("$rel")
        PDF_AFFECTED_COV+=("--cov=s3_bucket")
        ;;
      tests/test_*.py)
        PDF_AFFECTED_TESTS+=("$rel")
        ;;
    esac
  done
}

if [[ "$SKIP_PYTEST" -eq 0 && ${#CHANGED[@]} -gt 0 ]]; then
  pdf_discover_tests "${CHANGED[@]}"
  if [[ ${#PDF_AFFECTED_TESTS[@]} -gt 0 ]]; then
    log "=== pdf: pytest (affected unit tests) ==="
    qg_resolve_python || { log "quality-gate: python3 not found"; exit 2; }
    py="${QG_PYTHON[0]}"
    if ! "$py" -c "import pytest" >/dev/null 2>&1; then
      log "quality-gate: pytest missing — pip install -r tests/requirements-test.txt"
      exit 2
    fi
    # shellcheck disable=SC2206
    unique_tests=($(printf '%s\n' "${PDF_AFFECTED_TESTS[@]}" | sort -u))
    unique_cov=()
    if ((${#PDF_AFFECTED_COV[@]})); then
      # shellcheck disable=SC2206
      unique_cov=($(printf '%s\n' "${PDF_AFFECTED_COV[@]}" | sort -u))
    fi
    cov_args=()
    cov_dir="$ROOT/.qg-coverage"
    if "$py" -c "import pytest_cov" >/dev/null 2>&1 && [[ ${#unique_cov[@]} -gt 0 ]]; then
      mkdir -p "$cov_dir"
      cov_args=(
        --cov-report=term-missing:skip-covered
        --cov-report=xml:"$cov_dir/coverage.xml"
        --cov-report=json:"$cov_dir/coverage.json"
        "${unique_cov[@]}"
      )
    fi
    log "quality-gate: tests: ${unique_tests[*]}"
    out="$(mktemp)"
    set +e
    (
      cd "$ROOT"
      pytest_args=(-q --tb=short -ra -m "not integration and not e2e")
      if [[ ${#cov_args[@]} -gt 0 ]]; then
        "$py" -m pytest "${pytest_args[@]}" "${cov_args[@]}" "${unique_tests[@]}"
      else
        "$py" -m pytest "${pytest_args[@]}" "${unique_tests[@]}"
      fi
    ) 2>&1 | tee "$out"
    rc="${PIPESTATUS[0]}"
    set -e
    if [[ "$rc" -ne 0 ]]; then
      FAILED=1
    elif [[ ${#PDF_AFFECTED_APP_FILES[@]} -gt 0 && ${#cov_args[@]} -gt 0 ]]; then
      unique_apps=()
      # shellcheck disable=SC2206
      unique_apps=($(printf '%s\n' "${PDF_AFFECTED_APP_FILES[@]}" | sort -u))
      regress_out="$(mktemp)"
      set +e
      qg_run_coverage_regression "$ROOT" "$py" "${unique_apps[@]}" >"$regress_out" 2>&1
      cov_rc=$?
      set -e
      if [[ "$cov_rc" -ne 0 ]]; then
        log "=== COVERAGE REGRESSION ==="
        cat "$regress_out" >&2
        FAILED=1
      fi
      rm -f "$regress_out"
    fi
    rm -f "$out"
  fi
fi

if [[ "$FAILED" -ne 0 ]]; then
  log "quality-gate: FAILED ($MODE)"
  exit 1
fi
log "quality-gate: OK ($MODE, ${#CHANGED[@]} changed path(s))"
exit 0
