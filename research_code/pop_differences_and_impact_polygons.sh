#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=64
#SBATCH --mem=234gb
#SBATCH --time=96:00:00

SCRIPT_DIR=""

if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
fi

#python ${SCRIPT_DIR}/find_unserved_pop.py 
#python ${SCRIPT_DIR}/find_diff_pop.py
#python ${SCRIPT_DIR}/assign_rivers_to_basin.py 2
#python ${SCRIPT_DIR}/find_intersection_river.py 32
python ${SCRIPT_DIR}/impact_polygons_pop.py 64

