#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=2gb
#SBATCH --cpus-per-task=2

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.figures_scripts.convert_voronoi_to_geojson_for_map"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/convert_voronoi_to_geojson.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/convert_voronoi_to_geojson.log"

log "Running convert_voronoi_to_geojson"
${PYTHON_CMD} -m "${PYTHON_SCRIPT}" 2>&1 | tee -a "${LOG_DIR}/convert_voronoi_to_geojson.log"
log "Completed convert_voronoi_to_geojson"
