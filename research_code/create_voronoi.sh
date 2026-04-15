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

# Change to script directory (works in SLURM and local execution)
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
    cd "$SCRIPT_DIR"
fi
PYTHON_SCRIPT="create_voronoi.py"

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

if [[ "$MODE" == "array" ]] && [[ -n "$SLURM_ARRAY_TASK_ID" ]]; then
    # Array job mode: one approach per SLURM task
    APPROACHES=('0' '1a' '1b' '1c' '1d' '2' '3a' '3b' '3c' '3d' '4' '5')
    APPROACH="${APPROACHES[$SLURM_ARRAY_TASK_ID]}"
    python "$PYTHON_SCRIPT" --approach "$APPROACH"
elif [[ "$MODE" == "sequential" ]]; then
    # Sequential: only run on task 0 (skip other array tasks if present)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        echo "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    python "$PYTHON_SCRIPT"
elif [[ "$MODE" == "parallel" ]]; then
    # Parallel: run multiple approaches concurrently on different CPUs
    APPROACHES=('0' '1a' '1b' '1c' '1d' '2' '3a' '3b' '3c' '3d' '4' '5')
    FAILED_APPROACHES=()
    
    for APPROACH in "${APPROACHES[@]}"; do
        # Launch approach in background
        python "$PYTHON_SCRIPT" --approach "$APPROACH" &
    done
    
    # Wait for all background jobs to complete
    for job in $(jobs -p); do
        if ! wait $job; then
            FAILED_APPROACHES+=("$job")
        fi
    done
    
    if [[ ${#FAILED_APPROACHES[@]} -gt 0 ]]; then
        echo "ERROR: Some approaches failed"
        exit 1
    fi
else
    echo "ERROR: Unknown execution mode '$MODE' in config.yaml (valid: array, sequential, parallel)"
    exit 1
fi