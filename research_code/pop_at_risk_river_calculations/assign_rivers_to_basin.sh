#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=64gb
#SBATCH --time=48:00:00

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/assign_rivers_to_basin.log"


#
# Usage:
#   ./assign_rivers_to_basin.sh [level] [version] [buffer] [weight_method] [weight_func]
#
# Arguments (all optional config overrides):
#   level        - Processing level (default: from config.yaml arguments.default_level)
#   version      - Data version (default: from config.yaml arguments.default_version)
#   buffer       - Buffer distance in metres (default: from config.yaml params.buffer)
#   weight_method - Weight transform: linear | square_root | logarithmic | sigmoid
#   weight_func  - Distance mode: mult | add | "" (empty = default multiplicative)
## Parse optional config override arguments (will be sys.argv[2+] in Python)
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/assign_rivers_to_basin.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/assign_rivers_to_basin.log"

log "Running assign_rivers_to_basin with 2 workers"
${PYTHON_CMD} -m research_code.pop_at_risk_river_calculations.assign_rivers_to_basin 2 "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" 2>&1 | tee -a "${LOG_DIR}/assign_rivers_to_basin.log"
log "Completed assign_rivers_to_basin"