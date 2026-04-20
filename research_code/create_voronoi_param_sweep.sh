#!/bin/bash
# Lightweight parameter sweep for create_voronoi (approach fixed to 1)
#SBATCH --partition=cpu-single
#SBATCH --time=48:00:00
#SBATCH --mem=192gb
#SBATCH --cpus-per-task=16
#SBATCH --array=0-9
#SBATCH --job-name=voronoi-sweep
#SBATCH --output=logs/voronoi_sweep_%a.out
#SBATCH --error=logs/voronoi_sweep_%a.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.create_voronoi"
APPROACH="1"
VERSION="${1:-}"

# Parameter grids
LEVELS=(7 8 9)
WEIGHT_FUNCS=("mult" "add" "")
WEIGHT_METHODS=("linear" "logarithmic" "square_root" "sigmoid")
BUFFERS=(9000 11000 13000 15000)

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}/voronoi_sweep_${SLURM_ARRAY_TASK_ID:-0}.log" "${LOG_DIR}"/voronoi_sweep_*.out "${LOG_DIR}"/voronoi_sweep_*.err

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/voronoi_sweep_${SLURM_ARRAY_TASK_ID:-0}.log"
}

TASK_ID="${SLURM_ARRAY_TASK_ID:-0}"
if ! [[ "${TASK_ID}" =~ ^[0-9]+$ ]]; then
    log "ERROR: Invalid task id '${TASK_ID}'"
    exit 1
fi
if (( TASK_ID < 0 || TASK_ID > 9 )); then
    log "ERROR: TASK_ID must be between 0 and 9 (got ${TASK_ID})"
    exit 1
fi

log "Starting Voronoi parameter sweep task ${TASK_ID}/9 (approach=${APPROACH})"

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
                    log "Run ${run_count}: level=${level} buffer=${buffer} weight_method=${weight_method} weight_func='${weight_func}'"
                    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${level}" "${VERSION}" "${buffer}" "${weight_method}" "${weight_func}" --approach "${APPROACH}" \
                        2>&1 | tee -a "${LOG_DIR}/voronoi_sweep_${TASK_ID}.log"
                fi
                combo_index=$((combo_index + 1))
            done
        done
    done
done

log "Completed task ${TASK_ID}. Executed ${run_count} parameter combinations."
