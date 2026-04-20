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

#
# Usage:
#   ./convert_voronoi_to_geojson_for_map.sh [level] [version] [buffer] [weight_method] [weight_func]
#
# Arguments (all optional config overrides):
#   level        - Processing level (default: from config.yaml arguments.default_level)
#   version      - Data version (default: from config.yaml arguments.default_version)
#   buffer       - Buffer distance in metres (default: from config.yaml params.buffer)
#   weight_method - Weight transform: linear | square_root | logarithmic | sigmoid
#   weight_func  - Distance mode: mult | add | "" (empty = default multiplicative)
## Parse optional config override arguments
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/convert_voronoi_to_geojson.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/convert_voronoi_to_geojson.log"

log "Running convert_voronoi_to_geojson"
${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" 2>&1 | tee -a "${LOG_DIR}/convert_voronoi_to_geojson.log"
log "Completed convert_voronoi_to_geojson"
