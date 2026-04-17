#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=64gb
#SBATCH --time=48:00:00

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/assign_rivers_to_basin.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/assign_rivers_to_basin.log"

log "Running assign_rivers_to_basin with 2 workers"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.assign_rivers_to_basin 2 2>&1 | tee -a "${LOG_DIR}/assign_rivers_to_basin.log"
log "Completed assign_rivers_to_basin"