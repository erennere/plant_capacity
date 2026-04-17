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

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.annotation_scripts.download_bing_annotate"

NUM_INSTANCES=10
SPLIT_SEED=42
INSTANCE_ID="${SLURM_ARRAY_TASK_ID}"

LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/bing_annotate_${INSTANCE_ID}.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/bing_annotate_${INSTANCE_ID}.log"

log "Running download_bing_annotate instance $INSTANCE_ID of $NUM_INSTANCES"
${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "$INSTANCE_ID" --num-instances "$NUM_INSTANCES" --split-seed "$SPLIT_SEED" 2>&1 | tee -a "${LOG_DIR}/bing_annotate_${INSTANCE_ID}.log"
log "Completed download_bing_annotate instance $INSTANCE_ID"
