#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=64
#SBATCH --mem=234gb
#SBATCH --time=48:00:00
#SBATCH --array=0-9
#SBATCH --job-name=bing-annotate
#SBATCH --output=logs/bing_annotate_%A_%a.out
#SBATCH --error=logs/bing_annotate_%A_%a.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"

PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.annotation_scripts.download_bing_annotate"

NUM_INSTANCES=10
SPLIT_SEED=42
INSTANCE_ID="${SLURM_ARRAY_TASK_ID}"

#
# Usage:
#   ./run_download_bing_annotate_array.sh [level] [version] [buffer] [weight_method] [weight_func]
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

LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/bing_annotate_${INSTANCE_ID}.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/bing_annotate_${INSTANCE_ID}.log"

log "Running download_bing_annotate instance $INSTANCE_ID of $NUM_INSTANCES"
${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "$INSTANCE_ID" --num-instances "$NUM_INSTANCES" --split-seed "$SPLIT_SEED" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" 2>&1 | tee -a "${LOG_DIR}/bing_annotate_${INSTANCE_ID}.log"
log "Completed download_bing_annotate instance $INSTANCE_ID"
