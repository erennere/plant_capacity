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

set -e

# Use current working directory as project root.
PROJECT_ROOT="$(pwd)"

LOG_DIR="${PROJECT_ROOT}/logs"
cd "$PROJECT_ROOT"
PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.pop_at_risk_river_calculations.create_rasters"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/create_rasters.log"
}

log "Installing research_code module"
${PYTHON_CMD} -m pip install -e "$PWD" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log"
log "Installation complete"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 8)}
export OPENBLAS_NUM_THREADS=$OMP_NUM_THREADS
export MKL_NUM_THREADS=$OMP_NUM_THREADS
export NUMEXPR_NUM_THREADS=$OMP_NUM_THREADS

# Read key from a YAML section using awk (no external dependencies).
get_yaml_value_from_section() {
    local section="$1"
    local key="$2"
    awk -F':' -v section="$section" -v key="$key" '
        BEGIN { in_section=0 }
        /^[[:space:]]*[A-Za-z0-9_]+:[[:space:]]*$/ {
            current=$1
            gsub(/[[:space:]]/, "", current)
            in_section=(current==section)
            next
        }
        in_section && $1 ~ "^[[:space:]]+" key "$" {
            value=$2
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            print value
            exit
        }
    ' config.yaml
}

# Read default mode from config['annotations'].
DEFAULT_MODE=$(get_yaml_value_from_section "annotations" "default_mode")

# Fallback if annotations.default_mode is not set.
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
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "$JOB_INDEX" "$TOTAL_JOBS" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log"
elif [[ "$MODE" == "sequential" ]]; then
    # Sequential: only run on task 0 (skip other array tasks if present)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        log "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    log "Running raster processing in sequential mode"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" 0 1 2>&1 | tee -a "${LOG_DIR}/create_rasters.log"
elif [[ "$MODE" == "parallel" ]]; then
    # Parallel: run indices 0..X-1 concurrently.
    TOTAL_JOBS=$(get_yaml_value_from_section "annotations" "max_workers")
    TOTAL_JOBS=${TOTAL_JOBS:-${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 1)}}
    FAILED_COUNT=0
    
    log "Running ${TOTAL_JOBS} raster jobs in parallel"
    
    for ((JOB_INDEX=0; JOB_INDEX<TOTAL_JOBS; JOB_INDEX++)); do
        log "Launching raster job ${JOB_INDEX} in background"
        ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" "$JOB_INDEX" "$TOTAL_JOBS" 2>&1 | tee -a "${LOG_DIR}/create_rasters.log" &
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