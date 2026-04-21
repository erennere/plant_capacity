#!/bin/bash
# Run sensitivity analysis across all pop_voronoi_layers outputs (HW vs EU).
# Usage:
#   bash research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.sh [level] [version] [buffer] [weight_method] [weight_func]

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_MODULE="research_code.sensitivity_analysis_scripts.compare_pop_sweep_hw_eu"

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/compare_pop_sweep_hw_eu.log"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

log "Installing research_code module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null

log "Running HW/EU sensitivity analysis for all pop-output GPKGs"
${PYTHON_CMD} -m "${PYTHON_MODULE}" "$@" 2>&1 | tee -a "${LOG_FILE}"

log "Completed HW/EU sensitivity analysis"
