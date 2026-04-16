#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=128gb
#SBATCH --time=48:00:00

# Configuration
SCRIPT_DIR=""
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
fi


python ${SCRIPT_DIR}/NEW_01_GENERATEGRIDS.py
python ${SCRIPT_DIR}/NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py
#python ${SCRIPT_DIR}/NEW_03_WASTEWATERJOIN_GEOJSON.py
#python ${SCRIPT_DIR}/export_dataset_from_db.py