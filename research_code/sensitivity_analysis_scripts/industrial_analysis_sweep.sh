#!/bin/bash
# Parameter sweep for industrial analysis across multiple configurations
#SBATCH --partition=cpu-single
#SBATCH --time=96:00:00
#SBATCH --mem=64gb
#SBATCH --cpus-per-task=16
#SBATCH --array=0-9
#SBATCH --job-name=industrial-analysis-sweep
#SBATCH --output=logs/industrial_analysis_sweep_%a.out
#SBATCH --error=logs/industrial_analysis_sweep_%a.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"
cd "${PROJECT_ROOT}"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
DOWNLOAD_SCRIPT="research_code.industrial_analysis.download_and_vectorize"
FIND_UNCONNECTED_SCRIPT="research_code.industrial_analysis.find_unconnected_industrial_areas"
SHUFFLE_SEED="${SHUFFLE_SEED:-42}"

# Parameter grids for industrial analysis sweep
LEVELS=(6 7 8 9)
WEIGHT_FUNCS=("mult" "add" "")
WEIGHT_METHODS=("linear" "logarithmic" "square_root" "sigmoid")
BUFFERS=(9000 11000 13000 15000)
DYNAMIC_K_VALUES=(0.6 0.7 0.8)

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}/industrial_sweep_${SLURM_ARRAY_TASK_ID:-0}.log" "${LOG_DIR}"/industrial_sweep_*.out "${LOG_DIR}"/industrial_sweep_*.err

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/industrial_sweep_${SLURM_ARRAY_TASK_ID:-0}.log"
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

log "Starting industrial analysis sweep task ${TASK_ID}/9"

log "Installing research_code module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null 2>&1

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
            # When weight_func is empty, only use one canonical method to avoid redundant runs.
            if weight_func == "" and weight_method != "linear":
                continue

            # (a) Rigid buffering regime.
            for buffer in buffers:
                combos.append((level, buffer, weight_method, weight_func, "false", ""))

            # (b) Dynamic buffering regime.
            for k in dynamic_k_values:
                combos.append((level, 9000, weight_method, weight_func, "true", str(k)))

random.Random(seed).shuffle(combos)

for idx, (level, buffer, weight_method, weight_func, dynamic_buffering, dynamic_buffer_k) in enumerate(combos):
    if idx % 10 == task_id:
        wf = weight_func if weight_func != "" else "__EMPTY__"
        dbk = dynamic_buffer_k if dynamic_buffer_k != "" else "__EMPTY__"
        print(f"{level}\t\t{buffer}\t{weight_method}\t{wf}\t{dynamic_buffering}\t{dbk}")
PY
)

for combo in "${ASSIGNED_COMBOS[@]}"; do
    IFS=$'\t' read -r level version buffer weight_method weight_func dynamic_buffering dynamic_buffer_k <<< "${combo}"
    level=$(echo "${level}" | xargs)
    version=$(echo "${version}" | xargs)
    buffer=$(echo "${buffer}" | xargs)
    weight_method=$(echo "${weight_method}" | xargs)
    weight_func=$(echo "${weight_func}" | xargs)
    dynamic_buffering=$(echo "${dynamic_buffering}" | xargs)
    dynamic_buffer_k=$(echo "${dynamic_buffer_k}" | xargs)
    
    if [[ "${weight_func}" == "__EMPTY__" ]]; then
        weight_func=""
    fi
    if [[ "${dynamic_buffer_k}" == "__EMPTY__" ]]; then
        dynamic_buffer_k=""
    fi
    
    run_count=$((run_count + 1))
    log "Run ${run_count}: level=${level} buffer=${buffer} weight_method=${weight_method} weight_func='${weight_func}' dynamic_buffering=${dynamic_buffering} dynamic_buffer_k=${dynamic_buffer_k}"
    
    # Download and vectorize
    ${PYTHON_CMD} -m "${DOWNLOAD_SCRIPT}" "${level}" "${version}" "${buffer}" "${weight_method}" "${weight_func}" "${dynamic_buffering}" "${dynamic_buffer_k}" 2>&1 | tee -a "${LOG_DIR}/industrial_sweep_${TASK_ID}.log"
    
    # Find unconnected areas
    ${PYTHON_CMD} -m "${FIND_UNCONNECTED_SCRIPT}" "${level}" "${version}" "${buffer}" "${weight_method}" "${weight_func}" "${dynamic_buffering}" "${dynamic_buffer_k}" 2>&1 | tee -a "${LOG_DIR}/industrial_sweep_${TASK_ID}.log"
done

log "Completed task ${TASK_ID}. Executed ${run_count} parameter combinations."
