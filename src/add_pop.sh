#!/bin/bash
#
# Population Data Integration Script
# Processes Voronoi polygon layers with population raster data
# Can run locally by specifying file index, or in SLURM job array mode
#
# Usage:
#   ./add_pop.sh <index> [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
#   sbatch add_pop.sh              (SLURM array job - uses SLURM_ARRAY_TASK_ID)
#
# SLURM Configuration
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=192gb
#SBATCH --cpus-per-task=8
#SBATCH --array=0-12
#SBATCH --job-name=add-pop-array
#SBATCH --output=logs/add_pop_%a.out
#SBATCH --error=logs/add_pop_%a.err

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
PROJECT_ROOT="$(pwd)"

LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_SCRIPT="src.add_pop"
PYTHON_CMD="python"

# Parse optional config override arguments
LEVEL="${2:-}"
VERSION="${3:-}"
BUFFER="${4:-}"
WEIGHT_METHOD="${5:-}"
WEIGHT_FUNC="${6:-}"
DYNAMIC_BUFFERING="${7:-}"
DYNAMIC_BUFFER_K="${8:-}"

# Create log directory
mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/add_pop_array.log" "${LOG_DIR}"/add_pop_*.out "${LOG_DIR}"/add_pop_*.err


# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/add_pop_array.log"
}

log "=========================================="
log "Population Data Integration Task Started"
log "=========================================="
log "Project root directory: ${PROJECT_ROOT}"

# Determine task ID: from SLURM or command-line argument
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    # Running in SLURM job array
    TASK_ID="${SLURM_ARRAY_TASK_ID}"
    log "Running in SLURM job array"
    log "Job array ID: ${SLURM_ARRAY_JOB_ID:-unknown}"
    log "Array task ID: ${TASK_ID}"
elif [[ $# -ge 1 ]]; then
    # Local mode with command-line argument
    TASK_ID="$1"
    log "Running in local mode"
    log "Task ID from command-line: ${TASK_ID}"
else
    # No task ID provided
    log "ERROR: Task ID not provided"
    log "Usage: $0 <file_index> [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]"
    log "   or: sbatch $0 (SLURM mode)"
    exit 1
fi

# Validate task ID is numeric
if ! [[ "${TASK_ID}" =~ ^[0-9]+$ ]]; then
    log "ERROR: Invalid task ID '${TASK_ID}' - must be a non-negative integer"
    exit 1
fi

log "Python command: ${PYTHON_CMD}"

# Install package in editable mode before running modules
log "Installing src module (editable)"
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
log "Processing Voronoi file index: ${TASK_ID}"

# Run the population data integration
START_TIME=$(date +%s)

if ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${TASK_ID}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "=========================================="
    log "Task ${TASK_ID} Completed Successfully"
    log "Duration: ${DURATION} seconds ($(($DURATION / 60)) minutes)"
    log "=========================================="
    exit 0
else
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "=========================================="
    log "ERROR: Task ${TASK_ID} Failed"
    log "Duration: ${DURATION} seconds"
    log "Check error output above for details"
    log "=========================================="
    exit 1
fi