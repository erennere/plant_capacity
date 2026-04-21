# sensitivity_analysis_scripts

This folder is for stress-testing the pipeline, not for one-off production runs. It helps you answer: how much do results change if we vary level, buffer, weighting method, and weighting function? Instead of manually rerunning many commands, these scripts distribute combinations across SLURM arrays. Use this folder when you need confidence that your conclusions are stable and not just one parameter choice.

## What You Need Before Running

- A working Voronoi/population pipeline environment in research_code/
- Valid config paths in research_code/config.yaml
- SLURM access for array execution (recommended)
- Enough compute for sweeps (especially the parallel variant)

## How This Folder Is Run

From research_code/:

```bash
sbatch sensitivity_analysis_scripts/create_voronoi_param_sweep.sh
sbatch sensitivity_analysis_scripts/add_pop_param_sweep.sh
sbatch sensitivity_analysis_scripts/create_voronoi_param_sweep_parallel.sh
```

Direct single-task debug run:

```bash
python -m research_code.sensitivity_analysis_scripts.create_voronoi_parallel_sweep 0 2 --approach 1 --num-jobs 4
```

## Python Scripts (Logic)

### create_voronoi_parallel_sweep.py
Aim: Execute one sweep shard in parallel worker processes for faster turnaround. Inputs: task_id, optional version, approach, and num-jobs. Outputs: logs plus the same Voronoi artifacts produced by repeated create_voronoi runs. How: it filters parameter combinations assigned to the task, splits them across workers, and launches python -m research_code.create_voronoi subprocess calls.

## Shell Scripts (Entry Points)

### create_voronoi_param_sweep.sh
Aim: Standard 10-array sweep for create_voronoi with approach fixed to 1. Inputs: optional version argument and SLURM_ARRAY_TASK_ID. Outputs: Voronoi outputs for assigned combinations and per-task logs. How: the script loops over LEVELS x WEIGHT_FUNCS x WEIGHT_METHODS x BUFFERS and runs combinations where combo_index % 10 equals task id.

### add_pop_param_sweep.sh
Aim: Standard 10-array sweep for add_pop using task id as Voronoi file index. Inputs: optional version and SLURM_ARRAY_TASK_ID. Outputs: population-attachment outputs and per-task logs. How: it maps voronoi_file_index from task id and executes all matching parameter combinations for that shard.

### create_voronoi_param_sweep_parallel.sh
Aim: High-resource variant where each array task runs four internal jobs in parallel. Inputs: optional approach and version, plus SLURM task id. Outputs: parallelized sweep logs and full Voronoi outputs for assigned combinations. How: it allocates 64 CPUs and 234 GB memory per array task and calls create_voronoi_parallel_sweep.py with --num-jobs 4.

## Shell -> Python Flow Diagram

```text
create_voronoi_param_sweep.sh -> python -m research_code.create_voronoi
add_pop_param_sweep.sh -> python -m research_code.add_pop
create_voronoi_param_sweep_parallel.sh
  -> python -m research_code.sensitivity_analysis_scripts.create_voronoi_parallel_sweep
    -> python -m research_code.create_voronoi
```

## How To Change Sweep Behavior

The sweep grid is defined directly in the shell scripts:

- LEVELS=(...)
- WEIGHT_FUNCS=(...)
- WEIGHT_METHODS=(...)
- BUFFERS=(...)

To test a narrower or wider grid:

1. Edit these arrays in the relevant .sh file.
2. Keep --array=0-9 aligned with modulo logic unless you also change sharding logic.
3. If you increase combinations significantly, revisit time/memory/cpu SLURM headers.

## Practical Tips

- Start with create_voronoi_param_sweep.sh before using the high-resource parallel variant.
- Keep one run unchanged as a baseline so you can compare sensitivity outputs fairly.
- Archive logs per experiment label so you can trace which parameter grid produced each output set.
