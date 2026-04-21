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
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

try:
    from ..starter import load_config
    from ..pipelines import create_output_paths
except ImportError:
    from research_code.starter import load_config
    from research_code.pipelines import create_output_paths

# Setup logging
def setup_logging(log_dir: str, task_id: int) -> logging.Logger:
    """Configure stdout-only logging; the shell wrapper owns the log file."""
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
    logger.addHandler(handler)

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
    num_tasks: int = 10,
    shuffle_seed: int = 42,
) -> List[Tuple[int, str, int, str, str]]:
    """Deterministically shuffle then assign combinations to this task ID."""
    shuffled = list(combinations)
    random.Random(shuffle_seed).shuffle(shuffled)
    return [combo for idx, combo in enumerate(shuffled) if idx % num_tasks == task_id]


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
    logger: logging.Logger,
    project_root: str,
    version: str = "",
    max_retries: int = 2,
) -> List[Tuple[int, str, int, str, str]]:
    """
    Worker function: Run a subset of parameter combinations.
    
    Parameters:
        job_id: Identifier for this job (0-3)
        combinations: List of (level, version, buffer, weight_method, weight_func) tuples
        approach: Approach ID to run
        logger: Logger instance
        project_root: Root project directory
        version: Optional version override
        max_retries: Number of retries per failed run

    Returns:
        List of parameter combinations that failed after retries
    """
    os.chdir(project_root)
    sys.path.insert(0, project_root)

    log_msg = f"Job {job_id}: Starting with {len(combinations)} parameter combinations"
    logger.info(f"[Job {job_id}] {log_msg}")

    failed_combinations: List[Tuple[int, str, int, str, str]] = []
    output_exists_cache: dict[Tuple[int, str, int, str, str], bool] = {}

    def output_exists_for_combo(combo: Tuple[int, str, int, str, str]) -> bool:
        if combo in output_exists_cache:
            return output_exists_cache[combo]

        level, combo_version, buffer, weight_method, weight_func = combo
        version_override = version or combo_version or None

        cfg = load_config(
            level=str(level),
            version=version_override,
            buffer=int(buffer),
            weight_method=weight_method,
            weight_func=weight_func,
        )
        output_path = create_output_paths(cfg)["voronoi"][approach]
        exists = os.path.exists(output_path)
        output_exists_cache[combo] = exists
        return exists

    try:
        import subprocess

        for run_idx, (level, _, buffer, weight_method, weight_func) in enumerate(combinations, 1):
            log_msg = f"Job {job_id}: Run {run_idx}/{len(combinations)}: " \
                     f"level={level} buffer={buffer} weight_method={weight_method} " \
                     f"weight_func='{weight_func}'"
            logger.debug(f"[Job {job_id}] {log_msg}")

            # Build command
            cmd = [
                sys.executable, "-m", "research_code.create_voronoi",
                str(level), version, str(buffer), weight_method, weight_func,
                "--approach", approach
            ]
            env = os.environ.copy()
            env["PLANT_CAPACITY_LOG_LEVEL"] = "INFO"

            attempt = 0
            run_succeeded = False
            while attempt <= max_retries and not run_succeeded:
                result = subprocess.run(cmd, capture_output=False, text=True, env=env)
                if result.returncode == 0:
                    run_succeeded = True
                    log_msg = f"Job {job_id}: Run {run_idx} completed successfully"
                    logger.debug(f"[Job {job_id}] {log_msg}")
                    break

                attempt += 1
                if attempt <= max_retries:
                    backoff_seconds = min(60, 5 * (2 ** (attempt - 1)))
                    logger.warning(
                        "[Job %s] Run %s failed with return code %s; retrying in %ss "
                        "(%s/%s)",
                        job_id,
                        run_idx,
                        result.returncode,
                        backoff_seconds,
                        attempt,
                        max_retries,
                    )
                    time.sleep(backoff_seconds)

            if not run_succeeded:
                combo = (level, "", buffer, weight_method, weight_func)
                if output_exists_for_combo(combo):
                    logger.warning(
                        "[Job %s] Run %s returned non-zero but output file exists; skipping retry mark",
                        job_id,
                        run_idx,
                    )
                else:
                    failed_combinations.append(combo)
                    log_msg = (
                        f"Job {job_id}: Run {run_idx} FAILED after {max_retries + 1} attempts"
                    )
                    logger.error(f"[Job {job_id}] {log_msg}")

        log_msg = f"Job {job_id}: Completed all {len(combinations)} combinations"
        logger.info(f"[Job {job_id}] {log_msg}")

    except Exception as e:
        log_msg = f"Job {job_id}: EXCEPTION: {str(e)}"
        logger.error(f"[Job {job_id}] {log_msg}")
        for combo in combinations:
            if output_exists_for_combo(combo):
                logger.warning(
                    "[Job %s] Exception path: output exists for combo %s; skipping retry mark",
                    job_id,
                    combo,
                )
                continue
            failed_combinations.append(combo)

    return failed_combinations


def execute_with_job_count(
    combinations: List[Tuple[int, str, int, str, str]],
    num_jobs: int,
    approach: str,
    logger: logging.Logger,
    project_root: str,
    version: str,
    retry_failed_runs: int,
) -> List[Tuple[int, str, int, str, str]]:
    """Execute a batch with a fixed number of jobs and return failed combinations."""
    job_combinations = split_combinations_into_jobs(combinations, num_jobs=num_jobs)
    non_empty_jobs = [
        (job_id, combos)
        for job_id, combos in enumerate(job_combinations)
        if combos
    ]
    for job_id, combos in enumerate(job_combinations):
        logger.info(f"Job {job_id}: {len(combos)} combinations")

    if not non_empty_jobs:
        return []

    failed_combinations: List[Tuple[int, str, int, str, str]] = []
    with ThreadPoolExecutor(max_workers=num_jobs, thread_name_prefix="VoronoiJob") as executor:
        futures = {
            executor.submit(
                run_voronoi_job,
                job_id,
                combos,
                approach,
                logger,
                project_root,
                version,
                retry_failed_runs,
            ): job_id
            for job_id, combos in non_empty_jobs
        }

        for future in as_completed(futures):
            job_id = futures[future]
            try:
                failed = future.result()
                failed_combinations.extend(failed)
                logger.info(f"Job {job_id} completed (failed runs: {len(failed)})")
            except Exception as exc:
                logger.error(f"Job {job_id} failed with exception: {exc}", exc_info=True)
                failed_combinations.extend(job_combinations[job_id])

    return failed_combinations


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
    parser.add_argument("--retry-failed-runs", type=int, default=2,
                       help="Retries per failed parameter run (default: 2)")
    parser.add_argument("--shuffle-seed", type=int, default=42,
                       help="Seed for deterministic random assignment across 10 tasks")
    
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
    logger.info(f"Initial parallel jobs: {args.num_jobs}")
    logger.info(f"Shuffle seed: {args.shuffle_seed}")
    
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
        task_combinations = filter_combinations_by_task(
            all_combinations,
            task_id,
            num_tasks=10,
            shuffle_seed=args.shuffle_seed,
        )
        logger.info(f"Task {task_id}: {len(task_combinations)} combinations to process")

        # Start from num-jobs=4 (default) and progressively reduce to 1 if failures persist.
        current_jobs = max(1, args.num_jobs)
        remaining = list(task_combinations)
        while remaining and current_jobs >= 1:
            logger.info(
                "Executing %s combinations with num-jobs=%s",
                len(remaining),
                current_jobs,
            )
            failed = execute_with_job_count(
                combinations=remaining,
                num_jobs=current_jobs,
                approach=args.approach,
                logger=logger,
                project_root=project_root,
                version=args.version,
                retry_failed_runs=args.retry_failed_runs,
            )

            if not failed:
                remaining = []
                break

            logger.warning(
                "num-jobs=%s left %s failed combinations",
                current_jobs,
                len(failed),
            )
            remaining = failed
            if current_jobs == 1:
                break
            current_jobs -= 1
            logger.warning("Reducing num-jobs and retrying with num-jobs=%s", current_jobs)

        if remaining:
            logger.error(
                "Task %s completed with %s failed parameter runs even at num-jobs=1",
                task_id,
                len(remaining),
            )
            sys.exit(2)

        logger.info(f"Task {task_id}: All parallel jobs completed successfully")
        
    except Exception as e:
        logger.error(f"Task {task_id}: FAILED with exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
