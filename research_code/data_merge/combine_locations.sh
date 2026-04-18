#!/bin/bash
#
# Combined Location Data Merge Pipeline
# Processes: correct OSM locations -> merge segmentation results -> combine locations -> final merge
# Orchestrates multiple sequential data processing steps
#
# Usage:
#   ./combine_locations.sh    (local mode)
#   sbatch combine_locations.sh (SLURM job)
#
# SLURM Configuration
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=4
#SBATCH --mem=64gb
#SBATCH --time=48:00:00
#SBATCH --job-name=combine-locations
#SBATCH --output=logs/combine_locations.out
#SBATCH --error=logs/combine_locations.err

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
PROJECT_ROOT="$(pwd)"

LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

# Create log directory
mkdir -p "${LOG_DIR}"

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/combine_locations.log"
}

log "=========================================="
log "Combined Location Data Merge Started"
log "=========================================="
log "Project root directory: ${PROJECT_ROOT}"

# Install package in editable mode only if import is unavailable.
ensure_research_code_importable() {
    if ${PYTHON_CMD} -c "import research_code" >/dev/null 2>&1; then
        log "research_code import check passed; skipping editable install"
        return 0
    fi

    log "research_code not importable; attempting editable install"
    ${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
    ${PYTHON_CMD} -c "import research_code" >/dev/null 2>&1
}

log "Checking package importability..."
ensure_research_code_importable

log "Starting data merge pipeline..."

# Step 1: Correct OSM locations
log "Step 1: Correcting locations with OSM data..."
${PYTHON_CMD} -m research_code.data_merge.correct_locations_w_OSM 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
log "Step 1 completed"

# Step 2: Optionally merge legacy segmentation outputs.
# Whether this actually runs is controlled by booleans.legacy_merge in config.yaml.
log "Step 2: Running legacy segmentation merge if enabled in config.yaml..."
${PYTHON_CMD} -m research_code.data_merge.merge_seg_results --variant old 2>&1 | tee -a "${LOG_DIR}/merge_seg_results.log"
log "Step 2 completed"

# Step 3: Combine locations
log "Step 3: Combining location data..."
${PYTHON_CMD} -m research_code.data_merge.final_data_merge 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
log "Step 3 completed"

log "Running merge_seg_results (variant=new)"
${PYTHON_CMD} -m research_code.data_merge.merge_seg_results --variant new 2>&1 | tee -a "${LOG_DIR}/merge_seg_results.log"
log "Completed merge_seg_results (variant=new)"

log "=========================================="
log "Combined Location Data Merge Completed"
log "=========================================="