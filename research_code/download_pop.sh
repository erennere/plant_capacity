#!/bin/bash
#
# Population Data Processing Script
# Downloads and processes global population data from WorldPop
# Supports both GeoTIFF mosaicing and CSV rasterization
#
# SLURM Configuration
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=64gb
#SBATCH --time=96:00:00
#SBATCH --job-name=pop-processing
#SBATCH --output=logs/pop_%j.out
#SBATCH --error=logs/pop_%j.err

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
PROJECT_ROOT="$(pwd)"

LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_SCRIPT="research_code.download_pop"
PYTHON_CMD="python"  # or specify full path if needed

# Create log directory
mkdir -p "${LOG_DIR}"

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/pop_run.log"
}

log "=========================================="
log "Population Data Processing Started"
log "=========================================="
log "Project root directory: ${PROJECT_ROOT}"
log "Python command: ${PYTHON_CMD}"
log "Processing with 8 parallel workers"

# Install package in editable mode before running modules
log "Installing research_code module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}"

# Validate Python script exists
if ! python -c "import ${PYTHON_SCRIPT}" &> /dev/null; then
    log "ERROR: Python script not found or cannot be imported: ${PYTHON_SCRIPT}"
    exit 1
fi

# Verify Python is available
if ! command -v "${PYTHON_CMD}" &> /dev/null; then
    log "ERROR: Python command '${PYTHON_CMD}' not found"
    exit 1
fi

log "Python version: $(${PYTHON_CMD} --version 2>&1)"

# Run the population data processing
log "Starting population data download and processing..."
START_TIME=$(date +%s)

if ${PYTHON_CMD} -m "${PYTHON_SCRIPT}"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "=========================================="
    log "Population Data Processing Completed Successfully"
    log "Duration: ${DURATION} seconds ($(($DURATION / 60)) minutes)"
    log "=========================================="
    exit 0
else
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "=========================================="
    log "ERROR: Population Data Processing Failed"
    log "Duration: ${DURATION} seconds"
    log "Check SLURM error output for details"
    log "=========================================="
    exit 1
fi