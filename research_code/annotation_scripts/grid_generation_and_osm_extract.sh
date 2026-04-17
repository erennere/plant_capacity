#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=128gb
#SBATCH --time=48:00:00

# Configuration
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

# Use SLURM_SUBMIT_DIR only if it is writable; otherwise keep script-derived directory.
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]] && [[ -w "${SLURM_SUBMIT_DIR}" ]]; then
    SCRIPT_DIR="${SLURM_SUBMIT_DIR}"
fi

PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"

log "Running NEW_01_GENERATEGRIDS"
${PYTHON_CMD} -m research_code.annotation_scripts.NEW_01_GENERATEGRIDS 2>&1 | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"

log "Running NEW_02_EXTRACTOSMDATAFULL_GEOJSON"
${PYTHON_CMD} -m research_code.annotation_scripts.NEW_02_EXTRACTOSMDATAFULL_GEOJSON 2>&1 | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"

log "Grid generation and OSM extraction completed"