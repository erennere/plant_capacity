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

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/pop_validation_comparison.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "Running verification_script"
${PYTHON_CMD} -m research_code.pop_validation_scripts.verification_script 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "Running hw_comparison"
${PYTHON_CMD} -m research_code.pop_validation_scripts.hw_comparison 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "Running eu_comparison"
${PYTHON_CMD} -m research_code.pop_validation_scripts.eu_comparison 2>&1 | tee -a "${LOG_DIR}/pop_validation_comparison.log"

log "All pop validation comparisons completed"