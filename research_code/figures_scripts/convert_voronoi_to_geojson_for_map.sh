#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=2gb
#SBATCH --cpus-per-task=2

SCRIPT_DIR=""

if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
fi

python "${SCRIPT_DIR}/convert_voronoi_to_geojson_for_map.py"
