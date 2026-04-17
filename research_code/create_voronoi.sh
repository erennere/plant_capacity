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
#SBATCH --array=0-11
#
################################################################################

set -e

# Change to project root derived from the script location.
PROJECT_ROOT="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

# Use SLURM_SUBMIT_DIR only when it is writable and contains config.yaml.
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]] && [[ -w "${SLURM_SUBMIT_DIR}" ]] && [[ -f "${SLURM_SUBMIT_DIR}/config.yaml" ]]; then
    PROJECT_ROOT="${SLURM_SUBMIT_DIR}"
fi

cd "$PROJECT_ROOT"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"
PYTHON_SCRIPT="research_code.create_voronoi"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/create_voronoi.log"
}

log "Installing research_code module"
# Install package in editable mode before running modules
${PYTHON_CMD} -m pip install -e "$PROJECT_ROOT" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log"
log "Installation complete"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 8)}
export OPENBLAS_NUM_THREADS=$OMP_NUM_THREADS
export MKL_NUM_THREADS=$OMP_NUM_THREADS
export NUMEXPR_NUM_THREADS=$OMP_NUM_THREADS

# Determine execution mode: environment default + config override
# Defaults: array on HPC, sequential on local
if [[ -n "$SLURM_JOB_ID" ]]; then
    DEFAULT_MODE="array"
else
    DEFAULT_MODE="sequential"
fi

# FIX: Robust extraction of the mode value from config.yaml
# This removes comments, handles the 'execution:' block specifically, and strips quotes.
MODE=$(sed -n '/execution:/,/mode:/p' config.yaml | grep "mode:" | sed 's/#.*//' | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")
MODE=${MODE:-$DEFAULT_MODE}

log "Execution mode: ${MODE}"

if [[ "$MODE" == "array" ]] && [[ -n "$SLURM_ARRAY_TASK_ID" ]]; then
    # Array job mode: one approach per SLURM task
    APPROACHES=('0' '1a' '1b' '1c' '1d' '2' '3a' '3b' '3c' '3d' '4' '5')
    APPROACH="${APPROACHES[$SLURM_ARRAY_TASK_ID]}"
    log "Running approach ${APPROACH} in array mode (task ${SLURM_ARRAY_TASK_ID})"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" --approach "$APPROACH" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log"
elif [[ "$MODE" == "sequential" ]]; then
    # Sequential: only run on task 0 (skip other array tasks if present)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        log "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    log "Running all approaches in sequential mode"
    ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log"
elif [[ "$MODE" == "parallel" ]]; then
    # Parallel: run multiple approaches concurrently on different CPUs
    log "Running all approaches in parallel mode"
    APPROACHES=('0' '1a' '1b' '1c' '1d' '2' '3a' '3b' '3c' '3d' '4' '5')
    FAILED_APPROACHES=()
    
    for APPROACH in "${APPROACHES[@]}"; do
        log "Launching approach ${APPROACH} in background"
        # Launch approach in background
        ${PYTHON_CMD} -m "${PYTHON_SCRIPT}" --approach "$APPROACH" 2>&1 | tee -a "${LOG_DIR}/create_voronoi.log" &
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