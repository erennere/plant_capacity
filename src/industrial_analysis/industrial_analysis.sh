#!/bin/bash
# Industrial analysis pipeline: download/vectorize and find unconnected areas
#SBATCH --partition=cpu-single
#SBATCH --time=96:00:00
#SBATCH --mem=64gb
#SBATCH --cpus-per-task=16
#SBATCH --job-name=industrial-analysis
#SBATCH --output=logs/industrial_analysis_%j.out
#SBATCH --error=logs/industrial_analysis_%j.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"
cd "${PROJECT_ROOT}"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

# Script paths
DOWNLOAD_SCRIPT="src.industrial_analysis.download_and_vectorize"
FIND_UNCONNECTED_SCRIPT="src.industrial_analysis.find_unconnected_industrial_areas"

# Optional config overrides (all optional)
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"
DYNAMIC_BUFFERING="${6:-}"
DYNAMIC_BUFFER_K="${7:-}"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/industrial_analysis.log"
}

log "Starting industrial analysis pipeline"
log "Configuration:"
log "  - Level: ${LEVEL:-default}"
log "  - Version: ${VERSION:-default}"
log "  - Buffer: ${BUFFER:-default}"
log "  - Weight method: ${WEIGHT_METHOD:-default}"
log "  - Weight func: ${WEIGHT_FUNC:-default}"
log "  - Dynamic buffering: ${DYNAMIC_BUFFERING:-default}"
log "  - Dynamic buffer k: ${DYNAMIC_BUFFER_K:-default}"

log "Installing src module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null 2>&1

# Step 1: Download and vectorize industrial land rasters
log "=========================================="
log "Step 1: Downloading and vectorizing industrial land rasters"
log "=========================================="

if ${PYTHON_CMD} -m "${DOWNLOAD_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/industrial_analysis.log"; then
    log "Step 1 completed successfully"
else
    log "ERROR: Step 1 failed"
    exit 1
fi

# Step 2: Find unconnected industrial areas
log "=========================================="
log "Step 2: Finding unconnected industrial areas"
log "=========================================="

if ${PYTHON_CMD} -m "${FIND_UNCONNECTED_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/industrial_analysis.log"; then
    log "Step 2 completed successfully"
else
    log "ERROR: Step 2 failed"
    exit 1
fi

log "=========================================="
log "Industrial analysis pipeline completed successfully"
log "=========================================="