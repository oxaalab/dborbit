#!/usr/bin/env bash
#
# ctl.sh – one‑shot runner for mariadb‑bootstrap
#
# Creates a temporary virtual‑environment, installs the local package,
# runs it, and then removes every artefact it created.
#

set -Eeuo pipefail

# ───────── configuration ─────────────────────────────────────────────────────
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv_bootstrap"
ENTRYPOINT="bootstrap-mariadb"

# ───────── cleanup trap ──────────────────────────────────────────────────────
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

# ───────── prepare venv ──────────────────────────────────────────────────────
[[ -d "${VENV_DIR}" ]] && rm -rf "${VENV_DIR}"
echo "[INFO] Creating virtual environment: ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"

# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"
python -m pip install --quiet --upgrade pip wheel
python -m pip install --quiet "${PROJECT_ROOT}"

# ───────── run utility ───────────────────────────────────────────────────────
echo "[INFO] Running ${ENTRYPOINT} $*"
${ENTRYPOINT} "$@"
