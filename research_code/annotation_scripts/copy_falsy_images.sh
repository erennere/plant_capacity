#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=2
#SBATCH --mem=4gb
#SBATCH --time=48:00:00

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

#
# Usage:
#   ./research_code/annotation_scripts/copy_falsy_images.sh [level] [version] [buffer] [weight_method] [weight_func]
#
# Arguments (all optional config overrides):
#   level         - Processing level (default: from config.yaml arguments.default_level)
#   version       - Data version (default: from config.yaml arguments.default_version)
#   buffer        - Buffer distance in metres (default: from config.yaml params.buffer)
#   weight_method - Weight transform: linear | square_root | logarithmic | sigmoid
#   weight_func   - Distance mode: mult | add | "" (empty = default multiplicative)
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}/copy_falsy_images.log"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/copy_falsy_images.log"
}

ensure_research_code_importable() {
    if ${PYTHON_CMD} -c "import research_code" >/dev/null 2>&1; then
        log "research_code import check passed; skipping editable install"
        return 0
    fi

    log "research_code not importable; attempting editable install"
    ${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/copy_falsy_images.log"
    ${PYTHON_CMD} -c "import research_code" >/dev/null 2>&1
}

log "Checking package importability..."
ensure_research_code_importable

log "Running copy_falsy_images.py"
${PYTHON_CMD} -m research_code.annotation_scripts.copy_falsy_images "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" 2>&1 | tee -a "${LOG_DIR}/copy_falsy_images.log"
log "Completed copy_falsy_images.py"
