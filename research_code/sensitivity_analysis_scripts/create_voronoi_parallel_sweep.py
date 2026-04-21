#!/usr/bin/env python
"""
Parallel parameter sweep executor for create_voronoi.

Runs 4 instances of create_voronoi.py in parallel, each handling a subset
of parameter combinations. Designed to be called from SLURM array jobs.

Usage:
    python -m research_code.create_voronoi_parallel_sweep [TASK_ID] [VERSION] [APPROACH]

Parameters:
    TASK_ID: SLURM array task ID (0-9)
    VERSION: Optional config version
    APPROACH: Approach to run (default: 1)
"""

import sys
import os
import logging
import argparse
import threading
from multiprocessing import Process, Queue
from typing import List, Tuple

# Setup logging
def setup_logging(log_dir: str, task_id: int) -> logging.Logger:
    """Configure logging to file and stdout."""
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # File handler
    fh = logging.FileHandler(os.path.join(log_dir, f"voronoi_sweep_{task_id}.log"))
    fh.setLevel(logging.DEBUG)
    
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger


def generate_parameter_combinations() -> List[Tuple[int, str, int, str, str]]:
    """Generate all parameter combinations for the sweep."""
    levels = [7, 8, 9]
    weight_funcs = ["mult", "add", ""]
    weight_methods = ["linear", "logarithmic", "square_root", "sigmoid"]
    buffers = [9000, 11000, 13000, 15000]
    
    combinations = []
    for level in levels:
        for weight_func in weight_funcs:
            for weight_method in weight_methods:
                for buffer in buffers:
                    combinations.append((level, "", buffer, weight_method, weight_func))
    
    return combinations


def filter_combinations_by_task(
    combinations: List[Tuple[int, str, int, str, str]],
    task_id: int,
    num_tasks: int = 10
) -> List[Tuple[int, str, int, str, str]]:
    """Filter combinations to those assigned to this task ID."""
    return [combo for idx, combo in enumerate(combinations) if idx % num_tasks == task_id]


def split_combinations_into_jobs(
    combinations: List[Tuple[int, str, int, str, str]],
    num_jobs: int = 4
) -> List[List[Tuple[int, str, int, str, str]]]:
    """Split combinations into num_jobs roughly equal groups."""
    if not combinations:
        return [[] for _ in range(num_jobs)]
    
    jobs = [[] for _ in range(num_jobs)]
    for idx, combo in enumerate(combinations):
        jobs[idx % num_jobs].append(combo)
    
    return jobs


def run_voronoi_job(
    job_id: int,
    combinations: List[Tuple[int, str, int, str, str]],
    approach: str,
    log_queue: Queue,
    project_root: str,
    version: str = ""
) -> None:
    """
    Worker function: Run a subset of parameter combinations.
    
    Parameters:
        job_id: Identifier for this job (0-3)
        combinations: List of (level, version, buffer, weight_method, weight_func) tuples
        approach: Approach ID to run
        log_queue: Queue for logging messages
        project_root: Root project directory
        version: Optional version override
    """
    os.chdir(project_root)
    sys.path.insert(0, project_root)
    
    try:
        from research_code.create_voronoi import logger as base_logger
    except ImportError:
        from create_voronoi import logger as base_logger
    
    log_msg = f"Job {job_id}: Starting with {len(combinations)} parameter combinations"
    log_queue.put((job_id, "INFO", log_msg))
    
    try:
        import subprocess
        
        for run_idx, (level, _, buffer, weight_method, weight_func) in enumerate(combinations, 1):
            log_msg = f"Job {job_id}: Run {run_idx}/{len(combinations)}: " \
                     f"level={level} buffer={buffer} weight_method={weight_method} " \
                     f"weight_func='{weight_func}'"
            log_queue.put((job_id, "INFO", log_msg))
            
            # Build command
            cmd = [
                sys.executable, "-m", "research_code.create_voronoi",
                str(level), version, str(buffer), weight_method, weight_func,
                "--approach", approach
            ]
            
            # Run subprocess
            result = subprocess.run(cmd, capture_output=False, text=True)
            if result.returncode != 0:
                log_msg = f"Job {job_id}: Run {run_idx} FAILED with return code {result.returncode}"
                log_queue.put((job_id, "ERROR", log_msg))
            else:
                log_msg = f"Job {job_id}: Run {run_idx} completed successfully"
                log_queue.put((job_id, "DEBUG", log_msg))
        
        log_msg = f"Job {job_id}: Completed all {len(combinations)} combinations"
        log_queue.put((job_id, "INFO", log_msg))
        
    except Exception as e:
        log_msg = f"Job {job_id}: EXCEPTION: {str(e)}"
        log_queue.put((job_id, "ERROR", log_msg))


def log_queue_monitor(log_queue: Queue, logger: logging.Logger) -> None:
    """Monitor and process log messages from worker jobs."""
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    
    while True:
        try:
            job_id, level, msg = log_queue.get(timeout=1)
            if msg is None:  # Sentinel value
                break
            logger.log(level_map.get(level, logging.INFO), f"[Job {job_id}] {msg}")
        except Exception:
            continue


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parallel parameter sweep executor for create_voronoi"
    )
    parser.add_argument("task_id", nargs="?", type=int, default=None,
                       help="SLURM array task ID (0-9)")
    parser.add_argument("version", nargs="?", default="",
                       help="Optional config version")
    parser.add_argument("--approach", type=str, default="1",
                       help="Approach to run (default: 1)")
    parser.add_argument("--num-jobs", type=int, default=4,
                       help="Number of parallel jobs (default: 4)")
    
    args = parser.parse_args()
    
    # Get task ID from environment or argument
    task_id = args.task_id
    if task_id is None:
        task_id = int(os.environ.get("SLURM_ARRAY_TASK_ID", "0"))
    
    if not (0 <= task_id <= 9):
        print(f"ERROR: TASK_ID must be between 0 and 9 (got {task_id})", file=sys.stderr)
        sys.exit(1)
    
    # Setup paths and logging
    project_root = os.getcwd()
    log_dir = os.path.join(project_root, "logs")
    logger = setup_logging(log_dir, task_id)
    
    logger.info(f"Starting parallel Voronoi sweep (task {task_id}/9, approach={args.approach})")
    logger.info(f"Parallel jobs: {args.num_jobs} (each with ~16 CPUs available)")
    
    try:
        # Install editable package
        import subprocess
        logger.debug("Installing research_code module (editable)")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", project_root],
            capture_output=True,
            check=False
        )
        
        # Generate and filter combinations
        all_combinations = generate_parameter_combinations()
        task_combinations = filter_combinations_by_task(all_combinations, task_id, num_tasks=10)
        logger.info(f"Task {task_id}: {len(task_combinations)} combinations to process")
        
        # Split into jobs
        job_combinations = split_combinations_into_jobs(task_combinations, num_jobs=args.num_jobs)
        
        for job_id, combos in enumerate(job_combinations):
            logger.info(f"Job {job_id}: {len(combos)} combinations")
        
        # Create queue for logging
        log_queue = Queue()
        
        # Start queue monitor so child-process logs are consumed and emitted.
        monitor_thread = threading.Thread(
            target=log_queue_monitor,
            args=(log_queue, logger),
            name="VoronoiLogMonitor",
            daemon=True,
        )
        monitor_thread.start()

        # Start worker processes
        processes = []
        for job_id, combos in enumerate(job_combinations):
            if not combos:
                logger.debug(f"Job {job_id}: No combinations to process, skipping")
                continue
            
            p = Process(
                target=run_voronoi_job,
                args=(job_id, combos, args.approach, log_queue, project_root, args.version),
                name=f"VoronoiJob-{job_id}"
            )
            p.start()
            processes.append(p)
            logger.info(f"Started Job {job_id} (PID: {p.pid})")
        
        # Wait for all processes to complete
        logger.info(f"Waiting for {len(processes)} parallel jobs to complete...")
        for p in processes:
            p.join()
            logger.info(f"Job completed (PID: {p.pid})")
        
        # Sentinel to stop log monitor
        log_queue.put((None, None, None))
        monitor_thread.join(timeout=10)
        
        logger.info(f"Task {task_id}: All parallel jobs completed successfully")
        
    except Exception as e:
        logger.error(f"Task {task_id}: FAILED with exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
