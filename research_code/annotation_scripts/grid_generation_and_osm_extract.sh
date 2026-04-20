#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=128gb
#SBATCH --time=48:00:00

# Configuration
PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

# Parse optional config override arguments
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
IS_MULTIPLICATIVE="${5:-}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"

log "Running NEW_01_GENERATEGRIDS"
${PYTHON_CMD} -m research_code.annotation_scripts.NEW_01_GENERATEGRIDS "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${IS_MULTIPLICATIVE}" 2>&1 | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"

log "Running NEW_02_EXTRACTOSMDATAFULL_GEOJSON"
${PYTHON_CMD} -m research_code.annotation_scripts.NEW_02_EXTRACTOSMDATAFULL_GEOJSON "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${IS_MULTIPLICATIVE}" 2>&1 | tee -a "${LOG_DIR}/grid_generation_osm_extract.log"

log "Grid generation and OSM extraction completed"