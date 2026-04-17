#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=32
#SBATCH --mem=128gb
#SBATCH --time=48:00:00

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/find_intersection_river.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/find_intersection_river.log"

log "Running find_intersection_river with 32 workers"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.find_intersection_river 32 2>&1 | tee -a "${LOG_DIR}/find_intersection_river.log"
log "Completed find_intersection_river"