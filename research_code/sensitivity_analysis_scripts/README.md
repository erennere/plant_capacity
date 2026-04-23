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
sbatch sensitivity_analysis_scripts/industrial_analysis_sweep.sh
```

Direct single-task debug run:

```bash
python -m research_code.sensitivity_analysis_scripts.create_voronoi_parallel_sweep 0 2 "" "" --approach 1 --num-jobs 4
```

## Python Scripts (Logic)

### create_voronoi_parallel_sweep.py
Aim: Execute one sweep shard in parallel worker processes for faster turnaround. Inputs: task_id, optional version, optional backward-compatible dynamic positional args, approach, and num-jobs. Outputs: logs plus the same Voronoi artifacts produced by repeated create_voronoi runs. How: it filters parameter combinations assigned to the task, splits them across workers, and launches python -m research_code.create_voronoi subprocess calls.

## Shell Scripts (Entry Points)

### create_voronoi_param_sweep.sh
Aim: Standard 10-array sweep for create_voronoi with approach fixed to 1. Inputs: optional version/dynamic args and SLURM_ARRAY_TASK_ID. Outputs: Voronoi outputs for assigned combinations and per-task logs. How: it generates rigid-buffer and dynamic-buffer parameter regimes, shuffles deterministically, and runs combinations where combo_index % 10 equals task id.

### add_pop_param_sweep.sh
Aim: Standard 10-array sweep for add_pop using task id as Voronoi file index. Inputs: optional version/dynamic args and SLURM_ARRAY_TASK_ID. Outputs: population-attachment outputs and per-task logs. How: it iterates rigid-buffer and dynamic-buffer combinations, discovers available Voronoi files per combo, and runs add_pop on each file index.

### create_voronoi_param_sweep_parallel.sh
Aim: High-resource variant where each array task runs many internal jobs in parallel. Inputs: optional approach, version, and dynamic args, plus SLURM task id. Outputs: parallelized sweep logs and full Voronoi outputs for assigned combinations. How: it allocates high resources and calls create_voronoi_parallel_sweep.py with --num-jobs 16 by default.

### industrial_analysis_sweep.sh
Aim: Parameter sweep for industrial analysis stages. Inputs: SLURM task id and internal grids for level, weighting, rigid buffering, and dynamic buffering k-values. Outputs: vectorized industrial layers and unconnected-industrial outputs per assigned combination, plus logs. How: each task runs download_and_vectorize then find_unconnected_industrial_areas for combinations where combo_index % 10 equals task id.

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
- BUFFERS=(...) for rigid buffering
- DYNAMIC_K_VALUES=(...) for dynamic buffering

Current defaults include:

- LEVELS=(6 7 8 9)
- Both rigid (dynamic_buffering=false) and dynamic (dynamic_buffering=true) regimes
- Canonical dedup for empty weight_func so redundant method permutations are skipped

All sweep runners now pass/create combinations over this shared positional override layout:

```bash
[level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
```

To test a narrower or wider grid:

1. Edit these arrays in the relevant .sh file.
2. Keep --array=0-9 aligned with modulo logic unless you also change sharding logic.
3. If you increase combinations significantly, revisit time/memory/cpu SLURM headers.

## Practical Tips

- Start with create_voronoi_param_sweep.sh before using the high-resource parallel variant.
- Keep one run unchanged as a baseline so you can compare sensitivity outputs fairly.
- Archive logs per experiment label so you can trace which parameter grid produced each output set.
