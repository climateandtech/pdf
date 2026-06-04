#!/usr/bin/env bash
# Lint changed Python under pdf with ruff.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"
# shellcheck source=lib/resolve-tools.sh
source "$ROOT/scripts/lib/resolve-tools.sh"
qg_setup_path "$ROOT" "$REPO_ROOT"
cd "$ROOT"

FILES=()
while IFS= read -r _line; do
  [[ -n "$_line" ]] && FILES+=("$_line")
done < <(
  if [[ $# -gt 0 ]]; then
    printf '%s\n' "$@"
  else
    exit 0
  fi
)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "lint-changed: no pdf Python files to check"
  exit 0
fi

if ! qg_require_ruff; then
  exit 2
fi

echo "lint-changed: ruff check ${#FILES[@]} file(s)"
qg_run_ruff check "${FILES[@]}"
