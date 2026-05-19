#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=16gb
#SBATCH --cpus-per-task=4
#SBATCH --array=0

set -euo pipefail

# Configuration
PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/pop_validation_comparison.log"


#
# Usage:
#   ./comparison.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
#
# Arguments (all optional config overrides):
#   level        - Processing level (default: from config.yaml arguments.default_level)
#   version      - Data version (default: from config.yaml arguments.default_version)
#   buffer       - Buffer distance in metres (default: from config.yaml params.buffer)
#   weight_method - Weight transform: linear | square_root | logarithmic | sigmoid
#   weight_func  - Distance mode: mult | add | "" (empty = default multiplicative)
## Parse optional config override arguments
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"
DYNAMIC_BUFFERING="${6:-}"
DYNAMIC_BUFFER_K="${7:-}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/pop_validation_comparison.log"
}

log "Installing src module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "Running verification_script"
${PYTHON_CMD} -m src.pop_validation_scripts.verification_script "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "Running hw_comparison"
${PYTHON_CMD} -m src.pop_validation_scripts.hw_comparison "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "Running eu_comparison"
${PYTHON_CMD} -m src.pop_validation_scripts.eu_comparison "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "All pop validation comparisons completed"