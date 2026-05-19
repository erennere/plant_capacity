#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=8
#SBATCH --mem=32gb
#SBATCH --time=48:00:00

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/find_unserved_pop.log"


#
# Usage:
#   ./find_unserved_pop.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
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
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/find_unserved_pop.log"
}

log "Installing src module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/find_unserved_pop.log"

log "Running find_unserved_pop"
${PYTHON_CMD} -m src.pop_at_risk_river_calculations.find_unserved_pop "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/find_unserved_pop.log"
log "Completed find_unserved_pop"
