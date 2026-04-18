#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=2
#SBATCH --mem=4gb
#SBATCH --time=48:00:00

# Configuration
PROJECT_ROOT="$(pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
PYTHON_CMD="python"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/annotations_inspection.log"
}

ensure_research_code_importable() {
    if ${PYTHON_CMD} -c "import research_code" >/dev/null 2>&1; then
        log "research_code import check passed; skipping editable install"
        return 0
    fi

    log "research_code not importable; attempting editable install"
    ${PYTHON_CMD} -m pip install -e "${PROJECT_ROOT}" 2>&1 | tee -a "${LOG_DIR}/annotations_inspection.log"
    ${PYTHON_CMD} -c "import research_code" >/dev/null 2>&1
}

log "Checking package importability..."
ensure_research_code_importable

log "Running annotations_inspection.py"
${PYTHON_CMD} -m research_code.annotation_scripts.annotations_inspection 2>&1 | tee -a "${LOG_DIR}/annotations_inspection.log"
