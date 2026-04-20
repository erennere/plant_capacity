#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=234gb
#SBATCH --time=96:00:00

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

# Parse optional config override arguments
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
IS_MULTIPLICATIVE="${5:-}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/find_pop_in_danger_pop.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/find_pop_in_danger_pop.log"

log "Running find_pop_in_danger_pop"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.find_pop_in_danger_pop "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${IS_MULTIPLICATIVE}" 2>&1 | tee -a "${LOG_DIR}/find_pop_in_danger_pop.log"
log "Completed find_pop_in_danger_pop"






