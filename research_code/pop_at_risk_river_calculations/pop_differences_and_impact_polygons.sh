#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=64
#SBATCH --mem=234gb
#SBATCH --time=96:00:00

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"

log "Running find_unserved_pop"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.find_unserved_pop 2>&1 | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"

log "Running find_diff_pop"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.find_diff_pop 2>&1 | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"

log "Running assign_rivers_to_basin"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.assign_rivers_to_basin 2 2>&1 | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"

log "Running find_intersection_river"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.find_intersection_river 32 2>&1 | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"

log "Running impact_polygons_pop"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.impact_polygons_pop 64 2>&1 | tee -a "${LOG_DIR}/pop_differences_and_impact_polygons.log"

log "All pop_at_risk pipeline stages completed"

