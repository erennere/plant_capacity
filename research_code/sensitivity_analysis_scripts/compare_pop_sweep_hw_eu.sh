#!/bin/bash
# Run sensitivity analysis across all pop_voronoi_layers outputs (HW vs EU).
#SBATCH --partition=cpu-single
#SBATCH --time=96:00:00
#SBATCH --mem=64gb
#SBATCH --cpus-per-task=8
#SBATCH --job-name=compare-pop-sweep-hw-eu
#SBATCH --output=logs/compare_pop_sweep_hw_eu.out
#SBATCH --error=logs/compare_pop_sweep_hw_eu.err
# Usage:
#   bash research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_MODULE="research_code.sensitivity_analysis_scripts.compare_pop_sweep_hw_eu"
MAX_WORKERS="${SLURM_CPUS_PER_TASK:-8}"

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/compare_pop_sweep_hw_eu.log"
export COMPARE_POP_SWEEP_MAX_WORKERS="${MAX_WORKERS}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

log "Installing research_code module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null

log "Running HW/EU sensitivity analysis for all pop-output GPKGs (workers=${MAX_WORKERS})"
${PYTHON_CMD} -m "${PYTHON_MODULE}" "$@" 2>&1 | tee -a "${LOG_FILE}"

log "Completed HW/EU sensitivity analysis"
