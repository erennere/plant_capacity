#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=16gb
#SBATCH --cpus-per-task=4
#SBATCH --array=0


# Configuration
SCRIPT_DIR=""
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
fi


python ${SCRIPT_DIR}/verification_script.py
python ${SCRIPT_DIR}/hw_comparison.py
python ${SCRIPT_DIR}/eu_comparison.py