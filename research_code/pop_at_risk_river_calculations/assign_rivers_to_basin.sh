#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=64gb
#SBATCH --time=48:00:00

SCRIPT_DIR=""
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
fi

python ${SCRIPT_DIR}/assign_rivers_to_basin.py 2