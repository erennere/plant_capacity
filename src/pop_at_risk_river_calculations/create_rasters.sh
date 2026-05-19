#!/bin/bash

################################################################################
# create_rasters.sh - Raster Processing Pipeline
#
# Orchestrates raster execution with configurable modes.
#
# Execution Modes:
#   ARRAY JOB:  One index per SLURM task (via SLURM_ARRAY_TASK_ID)
#   SEQUENTIAL: Single local run with index 0 and total 1
#   PARALLEL:   Multiple indices concurrently on different CPUs
#
# Configuration (from config.yaml):
#   annotations.default_mode: array | sequential | parallel
#
#SBATCH --partition=cpu-single
#SBATCH --time=48:00:00
#SBATCH --mem=192gb
#SBATCH --cpus-per-task=16
#SBATCH --array=0-9
#
################################################################################

set -euo pipefail

# Use current working directory as project root.
PROJECT_ROOT="$(pwd)"

LOG_DIR="${PROJECT_ROOT}/logs"
cd "$PROJECT_ROOT"
PYTHON_CMD="python"
PYTHON_SCRIPT="src.pop_at_risk_river_calculations.create_rasters"

mkdir -p "${LOG_DIR}"

# Clean up previous run logs and scheduler outputs for a fresh run
rm -f "${LOG_DIR}/create_rasters.log"


log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/create_rasters.log"
}

log "Installing src module"
${PYTHON_CMD} -m pip install -e "$PWD" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log"
log "Installation complete"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 8)}
export OPENBLAS_NUM_THREADS=$OMP_NUM_THREADS
export MKL_NUM_THREADS=$OMP_NUM_THREADS
export NUMEXPR_NUM_THREADS=$OMP_NUM_THREADS

#
# Usage:
#   ./create_rasters.sh [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
#
# Arguments (all optional config overrides):
#   level        - Processing level (default: resolved from create_rasters config)
#   version      - Data version (default: resolved from create_rasters config)
#   buffer       - Buffer distance in metres (default: resolved from create_rasters config)
#   weight_method - Weight transform: linear | square_root | logarithmic | sigmoid
#   weight_func  - Distance mode: mult | add | "" (empty = default multiplicative)
## Parse optional config override arguments (forwarded after job_index/total_jobs in Python)
LEVEL="${1:-}"
VERSION="${2:-}"
BUFFER="${3:-}"
WEIGHT_METHOD="${4:-}"
WEIGHT_FUNC="${5:-}"
DYNAMIC_BUFFERING="${6:-}"
DYNAMIC_BUFFER_K="${7:-}"

# Resolve runtime execution settings through starter.py so section inheritance
# and CLI overrides apply consistently.
mapfile -t RASTER_CONFIG < <(
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
    script_name="create_rasters",
    level=env_value("LEVEL_OVERRIDE"),
    version=env_value("VERSION_OVERRIDE"),
    buffer=env_value("BUFFER_OVERRIDE"),
    weight_method=env_value("WEIGHT_METHOD_OVERRIDE"),
    weight_func=env_value("WEIGHT_FUNC_OVERRIDE", preserve_empty=True),
    dynamic_buffering=env_value("DYNAMIC_BUFFERING_OVERRIDE"),
    dynamic_buffer_k=env_value("DYNAMIC_BUFFER_K_OVERRIDE"),
)
annotations = cfg.get("annotations") or {}
print(annotations.get("default_mode", ""))
print(annotations.get("max_workers", ""))
PY
)
DEFAULT_MODE="${RASTER_CONFIG[0]:-}"
CONFIG_MAX_WORKERS="${RASTER_CONFIG[1]:-}"

# Fallback if create_rasters.annotations.default_mode is not set.
if [[ -z "$DEFAULT_MODE" ]]; then
    if [[ -n "$SLURM_JOB_ID" ]]; then
        DEFAULT_MODE="array"
    else
        DEFAULT_MODE="sequential"
    fi
fi

MODE="${MODE:-$DEFAULT_MODE}"

log "Execution mode: ${MODE}"

if [[ "$MODE" == "array" ]] && [[ -n "$SLURM_ARRAY_TASK_ID" ]]; then
    # Array mode: one index per SLURM task.
    JOB_INDEX="$SLURM_ARRAY_TASK_ID"
    if [[ -n "$SLURM_ARRAY_TASK_COUNT" ]]; then
        TOTAL_JOBS="$SLURM_ARRAY_TASK_COUNT"
    elif [[ -n "$SLURM_ARRAY_TASK_MIN" && -n "$SLURM_ARRAY_TASK_MAX" ]]; then
        TOTAL_JOBS=$((SLURM_ARRAY_TASK_MAX - SLURM_ARRAY_TASK_MIN + 1))
    else
        TOTAL_JOBS=1
    fi
    log "Running raster job ${JOB_INDEX} of ${TOTAL_JOBS} in array mode"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "$JOB_INDEX" "$TOTAL_JOBS" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log"
elif [[ "$MODE" == "sequential" ]]; then
    # Sequential: only run on task 0 (skip other array tasks if present)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        log "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    log "Running raster processing in sequential mode"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" 0 1 "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log"
elif [[ "$MODE" == "parallel" ]]; then
    # Parallel: run indices 0..X-1 concurrently.
    TOTAL_JOBS=${CONFIG_MAX_WORKERS:-${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 1)}}
    FAILED_COUNT=0
    
    log "Running ${TOTAL_JOBS} raster jobs in parallel"
    
    for ((JOB_INDEX=0; JOB_INDEX<TOTAL_JOBS; JOB_INDEX++)); do
        log "Launching raster job ${JOB_INDEX} in background"
        ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "$JOB_INDEX" "$TOTAL_JOBS" "${LEVEL}" "${VERSION}" "${BUFFER}" "${WEIGHT_METHOD}" "${WEIGHT_FUNC}" "${DYNAMIC_BUFFERING}" "${DYNAMIC_BUFFER_K}" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log" &
    done
    
    # Wait for all background jobs to complete
    for job in $(jobs -p); do
        if ! wait $job; then
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    done
    
    if [[ $FAILED_COUNT -gt 0 ]]; then
        log "ERROR: $FAILED_COUNT parallel job(s) failed"
        exit 1
    fi
    log "All raster jobs completed successfully"
else
    log "ERROR: Unknown execution mode '$MODE' (valid: array, sequential, parallel)"
    exit 1
fi

log "create_rasters execution completed"