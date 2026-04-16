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

SCRIPT_DIR=""
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
fi

NUM_INSTANCES=10
SPLIT_SEED=42
INSTANCE_ID="${SLURM_ARRAY_TASK_ID}"

mkdir -p logs
python ${SCRIPT_DIR}/download_bing_annotate.py "$INSTANCE_ID" --num-instances "$NUM_INSTANCES" --split-seed "$SPLIT_SEED"
