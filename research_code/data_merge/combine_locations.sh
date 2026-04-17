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
PROJECT_ROOT=""
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
else
    PROJECT_ROOT="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd/../.. && pwd)"
fi

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

# Install package in editable mode
log "Installing package in editable mode..."
pip install -e . > /dev/null 2>&1 || true

log "Starting data merge pipeline..."

# Step 1: Correct OSM locations
log "Step 1: Correcting locations with OSM data..."
${PYTHON_CMD} -m research_code.data_merge.correct_locations_w_OSM 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
log "Step 1 completed"

# Step 2: Merge segmentation results
log "Step 2: Merging segmentation results..."
${PYTHON_CMD} -m research_code.data_merge.merge_seg_results 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
log "Step 2 completed"

# Step 3: Combine locations
log "Step 3: Combining location data..."
${PYTHON_CMD} -m research_code.data_merge.combine_locations 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
log "Step 3 completed"

# Step 4: Final data merge
log "Step 4: Final data merge..."
${PYTHON_CMD} -m research_code.data_merge.final_data_merge 2>&1 | tee -a "${LOG_DIR}/combine_locations.log"
log "Step 4 completed"

log "=========================================="
log "Combined Location Data Merge Completed"
log "=========================================="