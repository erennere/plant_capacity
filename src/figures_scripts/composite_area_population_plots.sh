#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=8gb
#SBATCH --cpus-per-task=4

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="src.figures_scripts.composite_area_population_plots"

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}/composite_area_population_plots.log"

#
# Usage:
#   ./composite_area_population_plots.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k] [approach] [color_col]
#
# Optional config overrides:
#   level, version, buffer, weight_method, weight_func, dynamic_buffering, dynamic_buffer_k
# Optional plotting args:
#   approach  - key from create_pop_output_paths: 0 | 1 | 2 | 0_only_round | 1_only_round
#   color_col - boundary column used for color coding (default: ECONOMY)
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"
DYNAMIC_BUFFERING="${6:-}"
DYNAMIC_BUFFER_K="${7:-}"
APPROACH="${8:-}"
COLOR_COL="${9:-ECONOMY}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/composite_area_population_plots.log"
}

log "Installing src module"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/composite_area_population_plots.log"

CMD=(
    "${PYTHON_CMD}" -m "${PYTHON_SCRIPT}"
    "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}"
    --color-col "${COLOR_COL}"
)

if [[ -n "${APPROACH}" ]]; then
    CMD+=(--approach "${APPROACH}")
fi

log "Running composite area/population plots"
"${CMD[@]}" 2>&1 | tee -a "${LOG_DIR}/composite_area_population_plots.log"
log "Completed composite area/population plots"
