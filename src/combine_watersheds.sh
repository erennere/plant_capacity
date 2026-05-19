#!/bin/bash
#
# Watershed Archive Merge Script
# Extracts watershed zip archives and merges discovered geospatial layers.
#
# SLURM Configuration
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=4
#SBATCH --mem=32gb
#SBATCH --time=24:00:00
#SBATCH --job-name=combine-watersheds
#SBATCH --output=logs/combine_watersheds.out
#SBATCH --error=logs/combine_watersheds.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_SCRIPT="src.combine_watersheds"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/combine_watersheds.log" "${LOG_DIR}/combine_watersheds.out" "${LOG_DIR}/combine_watersheds.err"


log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/combine_watersheds.log"
}

log "=========================================="
log "Watershed Merge Started"
log "=========================================="
log "Project root directory: ${PROJECT_ROOT}"
log "Python command: ${PYTHON_CMD}"

#
# Usage:
#   ./combine_watersheds.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
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

log "Installing src module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/combine_watersheds.log"

if ! command -v "${PYTHON_CMD}" &> /dev/null; then
    log "ERROR: Python command '${PYTHON_CMD}' not found"
    exit 1
fi

if ! ${PYTHON_CMD} -c "import ${PYTHON_SCRIPT}" &> /dev/null; then
    log "ERROR: Python script not found or cannot be imported: ${PYTHON_SCRIPT}"
    exit 1
fi

log "Python version: $(${PYTHON_CMD} --version 2>&1)"
log "Starting watershed archive merge"

START_TIME=$(date +%s)

if ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/combine_watersheds.log"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "=========================================="
    log "Watershed Merge Completed Successfully"
    log "Duration: ${DURATION} seconds ($(($DURATION / 60)) minutes)"
    log "=========================================="
    exit 0
else
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "=========================================="
    log "ERROR: Watershed Merge Failed"
    log "Duration: ${DURATION} seconds"
    log "Check SLURM error output for details"
    log "=========================================="
    exit 1
fi