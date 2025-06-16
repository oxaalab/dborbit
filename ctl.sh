#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv_bootstrap"
ENTRYPOINT="bootstrap-mariadb"

cleanup() {
    EXIT_CODE=$?
    echo "[INFO] Cleaning up (exit code: ${EXIT_CODE}) …"

    [[ -n "${VIRTUAL_ENV:-}" ]] && deactivate || true
    rm -rf "${VENV_DIR}"

    find "${PROJECT_ROOT}" -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
    find "${PROJECT_ROOT}" -type f -name '*.py[co]' -delete 2>/dev/null || true
    rm -rf "${PROJECT_ROOT}"/{build,dist} "${PROJECT_ROOT}"/*.egg-info 2>/dev/null || true

    echo "[INFO] Done."
}
trap cleanup EXIT INT TERM

[[ -d "${VENV_DIR}" ]] && rm -rf "${VENV_DIR}"
echo "[INFO] Creating virtual environment: ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"

source "${VENV_DIR}/bin/activate"
python -m pip install --quiet --upgrade pip wheel
python -m pip install --quiet "${PROJECT_ROOT}"

echo "[INFO] Running ${ENTRYPOINT} $*"
${ENTRYPOINT} "$@"
