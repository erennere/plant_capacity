#!/bin/bash
# Lightweight parameter sweep for add_pop
#SBATCH --partition=cpu-single
#SBATCH --time=96:00:00
#SBATCH --mem=192gb
#SBATCH --cpus-per-task=8
#SBATCH --array=0-9
#SBATCH --job-name=add-pop-sweep
#SBATCH --output=logs/add_pop_sweep_%a.out
#SBATCH --error=logs/add_pop_sweep_%a.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.add_pop"
VERSION="${1:-}"

# Parameter grids
LEVELS=(7 8 9)
WEIGHT_FUNCS=("mult" "add" "")
WEIGHT_METHODS=("linear" "logarithmic" "square_root" "sigmoid")
BUFFERS=(9000 11000 13000 15000)

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}/add_pop_sweep_${SLURM_ARRAY_TASK_ID:-0}.log" "${LOG_DIR}"/add_pop_sweep_*.out "${LOG_DIR}"/add_pop_sweep_*.err

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/add_pop_sweep_${SLURM_ARRAY_TASK_ID:-0}.log"
}

TASK_ID="${SLURM_ARRAY_TASK_ID:-0}"
VORONOI_FILE_INDEX="${TASK_ID}"

if ! [[ "${TASK_ID}" =~ ^[0-9]+$ ]]; then
    log "ERROR: Invalid task id '${TASK_ID}'"
    exit 1
fi
if (( TASK_ID < 0 || TASK_ID > 9 )); then
    log "ERROR: TASK_ID must be between 0 and 9 (got ${TASK_ID})"
    exit 1
fi

log "Starting add_pop parameter sweep task ${TASK_ID}/9 using voronoi_file_index=${VORONOI_FILE_INDEX}"

log "Installing research_code module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null

combo_index=0
run_count=0
for level in "${LEVELS[@]}"; do
    for weight_func in "${WEIGHT_FUNCS[@]}"; do
        for weight_method in "${WEIGHT_METHODS[@]}"; do
            for buffer in "${BUFFERS[@]}"; do
                if (( combo_index % 10 == TASK_ID )); then
                    run_count=$((run_count + 1))
                    log "Run ${run_count}: index=${VORONOI_FILE_INDEX} level=${level} buffer=${buffer} weight_method=${weight_method} weight_func='${weight_func}'"
                    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${VORONOI_FILE_INDEX}" "${level}" "${VERSION}" "${buffer}" "${weight_method}" "${weight_func}" \
                        2>&1 | tee -a "${LOG_DIR}/add_pop_sweep_${TASK_ID}.log"
                fi
                combo_index=$((combo_index + 1))
            done
        done
    done
done

log "Completed task ${TASK_ID}. Executed ${run_count} parameter combinations."
