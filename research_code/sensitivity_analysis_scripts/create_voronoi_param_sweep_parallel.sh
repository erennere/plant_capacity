#!/bin/bash
# Parallel parameter sweep for create_voronoi (4 jobs × 16 CPUs = 64 CPUs total)
#SBATCH --partition=cpu-single
#SBATCH --time=96:00:00
#SBATCH --mem=234gb
#SBATCH --cpus-per-task=64
#SBATCH --array=0-9
#SBATCH --job-name=voronoi-parallel-sweep
#SBATCH --output=logs/voronoi_parallel_sweep_%a.out
#SBATCH --error=logs/voronoi_parallel_sweep_%a.err

set -euo pipefail

PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.sensitivity_analysis_scripts.create_voronoi_parallel_sweep"
APPROACH="${1:-1}"
VERSION="${2:-}"
DYNAMIC_BUFFERING="${3:-}"
DYNAMIC_BUFFER_K="${4:-}"
NUM_JOBS=16

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}/voronoi_parallel_sweep_${SLURM_ARRAY_TASK_ID:-0}.log" \
      "${LOG_DIR}"/voronoi_parallel_sweep_*.out \
      "${LOG_DIR}"/voronoi_parallel_sweep_*.err

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/voronoi_parallel_sweep_${SLURM_ARRAY_TASK_ID:-0}.log"
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

log "Starting Voronoi parallel parameter sweep task ${TASK_ID}/9"
log "Configuration:"
log "  - Parallel jobs: ${NUM_JOBS}"
log "  - CPUs per job: $((SLURM_CPUS_PER_TASK / NUM_JOBS))"
log "  - Total CPUs: ${SLURM_CPUS_PER_TASK}"
log "  - Total memory: ${SLURM_MEM_PER_NODE}MB"
log "  - Approach: ${APPROACH}"
log "  - Version: ${VERSION:-default}"
log "  - Dynamic buffering: ${DYNAMIC_BUFFERING:-default}"
log "  - Dynamic buffer k: ${DYNAMIC_BUFFER_K:-default}"

log "Installing research_code module (editable)"
${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" >/dev/null 2>&1

log "Running parallel sweep with ${NUM_JOBS} concurrent jobs"
${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${TASK_ID}" "${VERSION}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" \
    --approach "${APPROACH}" \
    --num-jobs "${NUM_JOBS}" \
    2>&1 | tee -a "${LOG_DIR}/voronoi_parallel_sweep_${TASK_ID}.log"

EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 0 ]; then
    log "Task ${TASK_ID}: Completed successfully"
else
    log "Task ${TASK_ID}: FAILED with exit code ${EXIT_CODE}"
    exit ${EXIT_CODE}
fi
