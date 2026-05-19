#!/bin/bash
# Lightweight parameter sweep for create_voronoi (approach fixed to 1)
#SBATCH --partition=cpu-single
#SBATCH --time=96:00:00
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
PYTHON_SCRIPT="src.create_voronoi"
APPROACH="1"
VERSION="${1:-}"
DYNAMIC_BUFFERING="${2:-}"
DYNAMIC_BUFFER_K="${3:-}"
SHUFFLE_SEED="${SHUFFLE_SEED:-42}"

# Parameter grids
LEVELS=(6 7 8 9)
WEIGHT_FUNCS=("mult" "add" "")
WEIGHT_METHODS=("linear" "logarithmic" "square_root" "sigmoid")
BUFFERS=(9000 11000 13000 15000)
DYNAMIC_K_VALUES=(0.6 0.7 0.8)

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

log "Starting Voronoi parameter sweep task ${TASK_ID}/9 (approach=${APPROACH}, shuffle_seed=${SHUFFLE_SEED})"

log "Installing src module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null

run_count=0
mapfile -t ASSIGNED_COMBOS < <(
    "${PYTHON_CMD}" - "${TASK_ID}" "${SHUFFLE_SEED}" <<'PY'
import random
import sys

task_id = int(sys.argv[1])
seed = int(sys.argv[2])

levels = [6, 7, 8, 9]
weight_funcs = ["mult", "add", ""]
weight_methods = ["linear", "logarithmic", "square_root", "sigmoid"]
buffers = [9000, 11000, 13000, 15000]
dynamic_k_values = [0.6, 0.7, 0.8]

combos = []
for level in levels:
    for weight_func in weight_funcs:
        for weight_method in weight_methods:
            # When weight_func is empty the distance weighting is disabled,
            # so weight_method has no effect on the output. Only include
            # one canonical method (linear) to avoid redundant runs.
            if weight_func == "" and weight_method != "linear":
                continue

            # (a) Rigid buffering regime.
            for buffer in buffers:
                combos.append((level, buffer, weight_method, weight_func, "false", ""))

            # (b) Dynamic buffering regime by k-values.
            for k in dynamic_k_values:
                combos.append((level, 9000, weight_method, weight_func, "true", str(k)))

random.Random(seed).shuffle(combos)

for idx, (level, buffer, weight_method, weight_func, dynamic_buffering, dynamic_buffer_k) in enumerate(combos):
    if idx % 10 == task_id:
        wf = weight_func if weight_func != "" else "__EMPTY__"
        dbk = dynamic_buffer_k if dynamic_buffer_k != "" else "__EMPTY__"
        print(f"{level}\t{buffer}\t{weight_method}\t{wf}\t{dynamic_buffering}\t{dbk}")
PY
)

for combo in "${ASSIGNED_COMBOS[@]}"; do
    IFS=$'\t' read -r level buffer weight_method weight_func dynamic_buffering dynamic_buffer_k <<< "${combo}"
    if [[ "${weight_func}" == "__EMPTY__" ]]; then
        weight_func=""
    fi
    if [[ "${dynamic_buffer_k}" == "__EMPTY__" ]]; then
        dynamic_buffer_k=""
    fi
    run_count=$((run_count + 1))
    log "Run ${run_count}: level=${level} buffer=${buffer} weight_method=${weight_method} weight_func='${weight_func}' dynamic_buffering=${dynamic_buffering} dynamic_buffer_k=${dynamic_buffer_k}"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${level}" "${VERSION}" "${buffer}" "${weight_method}" "${weight_func}" "${dynamic_buffering}" "${dynamic_buffer_k}" --approach "${APPROACH}" \
        2>&1 | tee -a "${LOG_DIR}/voronoi_sweep_${TASK_ID}.log"
done

log "Completed task ${TASK_ID}. Executed ${run_count} parameter combinations."
