#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=8
#SBATCH --mem=32gb
#SBATCH --time=48:00:00

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/find_unserved_pop.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/find_unserved_pop.log"

log "Running find_unserved_pop"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.find_unserved_pop 2>&1 | tee -a "${LOG_DIR}/find_unserved_pop.log"
log "Completed find_unserved_pop"
