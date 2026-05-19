#!/bin/bash

################################################################################
# create_voronoi.sh - Voronoi Spatial Allocation Pipeline
#
# Orchestrates approach execution with configurable modes.
#
# Execution Modes:
#   ARRAY JOB:  One approach per SLURM task (via SLURM_ARRAY_TASK_ID)
#   SEQUENTIAL: One approach after another (default for local)
#   PARALLEL:   Multiple approaches concurrently on different CPUs
#
# Configuration (from config.yaml):
#   execution.mode: array | sequential | parallel
#
#SBATCH --partition=cpu-single
#SBATCH --time=48:00:00
#SBATCH --mem=192gb
#SBATCH --cpus-per-task=16
#SBATCH --array=0-2
#
################################################################################

set -euo pipefail

# Change to project root from the current working directory.
PROJECT_ROOT="$(pwd)"

cd "$PROJECT_ROOT"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="src.create_voronoi"

mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/create_voronoi.log"


log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/create_voronoi.log"
}

log "Installing src module"
# Install package in editable mode before running modules
${PYTHON_CMD} -m pip install -e "$PROJECT_ROOT" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log"
log "Installation complete"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 8)}
export OPENBLAS_NUM_THREADS=$OMP_NUM_THREADS
export MKL_NUM_THREADS=$OMP_NUM_THREADS
export NUMEXPR_NUM_THREADS=$OMP_NUM_THREADS

#
# Usage:
#   ./create_voronoi.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
#
# Arguments (all optional config overrides):
#   level         - Processing level (default: resolved from create_voronoi config)
#   version       - Data version (default: resolved from create_voronoi config)
#   buffer        - Buffer distance in metres (default: resolved from create_voronoi config)
#   weight_method - Weight transform: linear | square_root | logarithmic | sigmoid
#   weight_func   - Distance mode: mult | add | "" (empty = default multiplicative)
#
# Approaches: 0 (WWTP no watersheds), 1 (WWTP with watersheds), 2 (cities)
# Use --only_round flag to pass to Python for round-area weights.
## Parse optional config override arguments
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"
DYNAMIC_BUFFERING="${6:-}"
DYNAMIC_BUFFER_K="${7:-}"

# Determine execution mode: environment default + config override
# Defaults: array on HPC, sequential on local
if [[ -n "$SLURM_JOB_ID" ]]; then
    DEFAULT_MODE="array"
else
    DEFAULT_MODE="sequential"
fi

# Resolve execution.mode through starter.py so section inheritance and CLI
# overrides apply consistently.
MODE=$(
    LEVEL_OVERRIDE="${LEVEL}" \
    VERSION_OVERRIDE="${VERSION}" \
    BUFFER_OVERRIDE="${BUFFER}" \
    WEIGHT_METHOD_OVERRIDE="${WEIGHT_METHOD}" \
    WEIGHT_FUNC_OVERRIDE="${WEIGHT_FUNC}" \
    DYNAMIC_BUFFERING_OVERRIDE="${DYNAMIC_BUFFERING}" \
    DYNAMIC_BUFFER_K_OVERRIDE="${DYNAMIC_BUFFER_K}" \
    "${PYTHON_CMD}" - <<'PY'
import os

from src.starter import load_config


def env_value(name, preserve_empty=False):
    value = os.environ.get(name)
    if value is None:
        return None
    if preserve_empty:
        return value
    return value or None


cfg = load_config(
    script_name="create_voronoi",
    level=env_value("LEVEL_OVERRIDE"),
    version=env_value("VERSION_OVERRIDE"),
    buffer=env_value("BUFFER_OVERRIDE"),
    weight_method=env_value("WEIGHT_METHOD_OVERRIDE"),
    weight_func=env_value("WEIGHT_FUNC_OVERRIDE", preserve_empty=True),
    dynamic_buffering=env_value("DYNAMIC_BUFFERING_OVERRIDE"),
    dynamic_buffer_k=env_value("DYNAMIC_BUFFER_K_OVERRIDE"),
)
print((cfg.get("execution") or {}).get("mode", ""))
PY
)
MODE=${MODE:-$DEFAULT_MODE}

log "Execution mode: ${MODE}"

if [[ "$MODE" == "array" ]] && [[ -n "$SLURM_ARRAY_TASK_ID" ]]; then
    # Array job mode: one approach per SLURM task
    APPROACHES=('0' '1' '2')
    APPROACH="${APPROACHES[$SLURM_ARRAY_TASK_ID]}"
    log "Running approach ${APPROACH} in array mode (task ${SLURM_ARRAY_TASK_ID})"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" --approach "$APPROACH" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log"
elif [[ "$MODE" == "sequential" ]]; then
    # Sequential: only run on task 0 (skip other array tasks if present)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        log "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    log "Running all approaches in sequential mode"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log"
elif [[ "$MODE" == "parallel" ]]; then
    # Parallel: run multiple approaches concurrently on different CPUs
    log "Running all approaches in parallel mode"
    APPROACHES=('0' '1' '2')
    FAILED_APPROACHES=()
    
    for APPROACH in "${APPROACHES[@]}"; do
        log "Launching approach ${APPROACH} in background"
        # Launch approach in background
        ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" --approach "$APPROACH" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log" &
    done
    
    # Wait for all background jobs to complete
    for job in $(jobs -p); do
        if ! wait $job; then
            FAILED_APPROACHES+=("$job")
        fi
    done
    
    if [[ ${#FAILED_APPROACHES[@]} -gt 0 ]]; then
        log "ERROR: Some approaches failed"
        exit 1
    fi
    log "All approaches completed successfully"
else
    log "ERROR: Unknown execution mode '$MODE' in config.yaml (valid: array, sequential, parallel)"
    exit 1
fi

log "create_voronoi execution completed"