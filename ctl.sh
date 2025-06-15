#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

EXTRA_DBTOOL_ARGS=()
if ! printf '%s\0' "$@" | grep -q -- "-c\|--config"; then
  if [[ -f "${PROJECT_ROOT}/dbtool.config.yml" ]]; then
    EXTRA_DBTOOL_ARGS=(-c "${PROJECT_ROOT}/dbtool.config.yml")
  elif [[ -f "${PROJECT_ROOT}/dbtool.config.example.yml" ]]; then
    echo "[ctl] Using example config → -c dbtool.config.example.yml"
    EXTRA_DBTOOL_ARGS=(-c "${PROJECT_ROOT}/dbtool.config.example.yml")
  else
    echo "[ctl] ❌  No dbtool.config.yml or example found." >&2
    exit 1
  fi
fi

VENV_DIR="$(mktemp -d "${PROJECT_ROOT}/.tmp_venv_XXXX")"

cleanup() {
  echo "[ctl] Cleaning up …"
  find "${PROJECT_ROOT}" -type d \( -name "__pycache__" -o -name "*.egg-info" \) -exec rm -rf {} + 2>/dev/null || true
  rm -rf "${VENV_DIR}"
}
trap cleanup EXIT INT TERM

echo "[ctl] Creating venv at ${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

python -m pip install --quiet -U pip setuptools wheel
pip install --quiet -e "${PROJECT_ROOT}"

mkdir -p "${PROJECT_ROOT}/db/migrations" "${PROJECT_ROOT}/db/schema"

if [[ $# -eq 0 ]]; then
  echo "[ctl] No arguments – entering interactive shell inside venv (type 'exit' to quit)"
  bash --login
else
  echo "[ctl] dbtool ${EXTRA_DBTOOL_ARGS[*]} $*"
  dbtool "${EXTRA_DBTOOL_ARGS[@]}" "$@"
fi
