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
#SBATCH --array=0-11
#
################################################################################

set -e

# Change to script directory (works in SLURM and local execution)
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
    cd "$SCRIPT_DIR"
fi
PYTHON_SCRIPT="create_rasters.py"

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
    python "$PYTHON_SCRIPT" "$JOB_INDEX" "$TOTAL_JOBS"
elif [[ "$MODE" == "sequential" ]]; then
    # Sequential: only run on task 0 (skip other array tasks if present)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        echo "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    python "$PYTHON_SCRIPT" 0 1
elif [[ "$MODE" == "parallel" ]]; then
    # Parallel: run indices 0..X-1 concurrently.
    TOTAL_JOBS=$(get_yaml_value_from_section "annotations" "max_workers")
    TOTAL_JOBS=${TOTAL_JOBS:-${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 1)}}
    FAILED_COUNT=0
    
    for ((JOB_INDEX=0; JOB_INDEX<TOTAL_JOBS; JOB_INDEX++)); do
        python "$PYTHON_SCRIPT" "$JOB_INDEX" "$TOTAL_JOBS" &
    done
    
    # Wait for all background jobs to complete
    for job in $(jobs -p); do
        if ! wait $job; then
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    done
    
    if [[ $FAILED_COUNT -gt 0 ]]; then
        echo "ERROR: $FAILED_COUNT parallel job(s) failed"
        exit 1
    fi
else
    echo "ERROR: Unknown execution mode '$MODE' (valid: array, sequential, parallel)"
    exit 1
fi