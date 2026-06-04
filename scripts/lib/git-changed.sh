#!/usr/bin/env bash
# Resolve changed paths for pdf (monorepo or standalone).
set -euo pipefail

qg_git_root() {
  local start="${1:-${ROOT:-.}}"
  git -C "$start" rev-parse --show-toplevel 2>/dev/null || true
}

qg_normalize_path() {
  local rel="$1"
  rel="${rel#/}"
  if [[ "$rel" == pdf/* ]]; then
    echo "$rel"
  elif [[ "$rel" == tests/* || "$rel" == scripts/* || "$rel" == parse_modes.py || "$rel" == parser_registry.py ]]; then
    echo "pdf/$rel"
  else
    echo "$rel"
  fi
}

qg_collect_changed() {
  local base_ref="${1:-}"
  local single_file="${2:-}"
  local git_root
  git_root="$(qg_git_root "${ROOT:-.}")"

  if [[ -n "$single_file" ]]; then
    local rel="$single_file"
    if [[ "$rel" == /* && -n "$git_root" ]]; then
      rel="${rel#"$git_root"/}"
    fi
    qg_normalize_path "$rel"
    return
  fi

  if [[ -z "$git_root" ]]; then
    return
  fi

  local p
  if [[ -n "$base_ref" ]] && git -C "$git_root" rev-parse --verify "${base_ref}" >/dev/null 2>&1; then
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      case "$p" in
        pdf/*) echo "$p" ;;
        tests/*|scripts/*|parse_modes.py|parser_registry.py) qg_normalize_path "$p" ;;
      esac
    done < <(git -C "$git_root" diff --name-only "${base_ref}...HEAD" 2>/dev/null || true)
  else
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      case "$p" in
        pdf/*) echo "$p" ;;
        tests/*|scripts/*|parse_modes.py|parser_registry.py) qg_normalize_path "$p" ;;
      esac
    done < <(
      {
        git -C "$git_root" diff --name-only HEAD 2>/dev/null || true
        git -C "$git_root" diff --cached --name-only 2>/dev/null || true
        git -C "$git_root" ls-files --others --exclude-standard 2>/dev/null || true
      } | sort -u
    )
  fi
}

qg_collect_paths_file() {
  local paths_file="$1"
  local git_root
  git_root="$(qg_git_root "${ROOT:-.}")"
  [[ -f "$paths_file" ]] || return

  local p rel
  while IFS= read -r p; do
    [[ -z "$p" ]] && continue
    rel="$p"
    if [[ "$rel" == /* && -n "$git_root" ]]; then
      rel="${rel#"$git_root"/}"
    fi
    qg_normalize_path "${rel#/}"
  done < "$paths_file" | sort -u
}
