# Repository Inventory

## 1. Main Pipeline Order

- Recommended end-to-end order, explicitly documented in `README.md` and `research_code/README.md`:
- `research_code/data_merge/combine_locations.sh`: canonical WWTP point build chain.
- `research_code/combine_watersheds.sh`: combined watershed layer build.
- `research_code/annotation_scripts/grid_generation_and_osm_extract.sh`: annotation grids plus OSM context.
- `research_code/annotation_scripts/run_download_bing_annotate_array.sh`: imagery and annotation asset generation.
- `research_code/annotation_scripts/merge_annotations.sh`: annotation merge back into the WWTP dataset.
- `research_code/download_pop.sh`: population raster download and preparation.
- `research_code/create_voronoi.sh`: Voronoi service-area generation.
- `research_code/add_pop.sh`: population attachment to one Voronoi file index per run.
- `research_code/pop_at_risk_river_calculations/create_rasters.sh`: raster preparation for non-served analysis.
- `research_code/pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh`: non-served, difference, river-linking, and impact-polygon stages.
- `research_code/pop_at_risk_river_calculations/find_pop_in_danger_pop.sh`: final population-at-risk aggregation.
- `research_code/industrial_analysis/industrial_analysis.sh`: industrial-land branch.
- `research_code/pop_validation_scripts/comparison.sh` and the figure scripts under `research_code/figures_scripts`: validation and visualization after generation stages.
- Adjacent but not part of the documented main order:
- `research_code/annotation_scripts/annotations_inspection.sh`: annotation QA sampling.
- `research_code/annotation_scripts/copy_falsy_images.sh`: annotation QA helper.
- `research_code/figures_scripts/pop_at_risk_figures.sh`: risk-result visualization.
- The sensitivity-analysis branch is separate from the baseline production order and is documented in `research_code/sensitivity_analysis_scripts/README.md`.

## 2. Major Directories And Purpose

- Top-level `data/`: stores raw inputs, intermediates, and generated outputs, as described in `README.md`.
- `data/boundaries/`: Natural Earth country-boundary shapefile components.
- `data/DL_results/`: DL or segmentation result artifacts referenced by config keys `dl_dir`, `dl_zipfile`, and `dl_mapfile`.
- `data/extra_points/`: extra country-specific WWTP source inputs referenced by `research_code/config.yaml`.
- `data/final_data_source/`: final or base country-specific WWTP source datasets referenced by `research_code/config.yaml`.
- `data/figures/`: generated figure outputs and interactive HTML map artifacts.
- Top-level `research_code/`: executable codebase, shell wrappers, shared orchestration, and configuration.
- `research_code/data_merge/`: canonical WWTP point dataset construction and harmonization.
- `research_code/annotation_scripts/`: annotation-grid generation, OSM extraction, imagery rendering, annotation merging, and QA.
- `research_code/industrial_analysis/`: industrial raster download/vectorization and uncovered-industrial-area analysis.
- `research_code/figures_scripts/`: static and interactive communication outputs.
- `research_code/pop_at_risk_river_calculations/`: non-served population, river linkage, downstream impact, and final population-at-risk outputs.
- `research_code/pop_validation_scripts/`: validation and comparison against HydroWaste and EU references.
- `research_code/sensitivity_analysis_scripts/`: parameter sweeps and sweep-result comparison tooling.

## 3. Python Entrypoints

### Top-level

- `research_code/download_pop.py`: population download and raster preparation.
- `research_code/combine_watersheds.py`: watershed archive merge.
- `research_code/create_voronoi.py`: main Voronoi generation entrypoint.
- `research_code/add_pop.py`: attach population to a selected Voronoi output.

### Data merge

- `research_code/data_merge/correct_locations_w_OSM.py`
- `research_code/data_merge/merge_seg_results.py`
- `research_code/data_merge/final_data_merge.py`

### Annotation

- `research_code/annotation_scripts/NEW_01_GENERATEGRIDS.py`
- `research_code/annotation_scripts/NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py`
- `research_code/annotation_scripts/NEW_03_WASTEWATERJOIN_GEOJSON.py`
- `research_code/annotation_scripts/NEW_04_EXPORTGEOTIFF.py`
- `research_code/annotation_scripts/download_bing_annotate.py`
- `research_code/annotation_scripts/merge_annotations.py`
- `research_code/annotation_scripts/annotations_inspection.py`
- `research_code/annotation_scripts/copy_falsy_images.py`
- `research_code/annotation_scripts/README.md` explicitly says `NEW_03_WASTEWATERJOIN_GEOJSON.py` is currently not used, and `NEW_04_EXPORTGEOTIFF.py` is not used and not tested.

### Industrial analysis

- `research_code/industrial_analysis/download_and_vectorize.py`
- `research_code/industrial_analysis/find_unconnected_industrial_areas.py`

### Figures

- `research_code/figures_scripts/convert_voronoi_to_geojson_for_map.py`
- `research_code/figures_scripts/composite_area_population_plots.py`
- `research_code/figures_scripts/piechart_figure.py`
- `research_code/figures_scripts/piechart_interactive.py`
- `research_code/figures_scripts/pop_at_risk_figures.py`

### Population-at-risk and river calculations

- `research_code/pop_at_risk_river_calculations/create_rasters.py`
- `research_code/pop_at_risk_river_calculations/find_unserved_pop.py`
- `research_code/pop_at_risk_river_calculations/find_diff_pop.py`
- `research_code/pop_at_risk_river_calculations/assign_rivers_to_basin.py`
- `research_code/pop_at_risk_river_calculations/find_intersection_river.py`
- `research_code/pop_at_risk_river_calculations/impact_polygons_pop.py`
- `research_code/pop_at_risk_river_calculations/find_pop_in_danger_pop.py`

### Validation

- `research_code/pop_validation_scripts/verification_script.py`
- `research_code/pop_validation_scripts/hw_comparison.py`
- `research_code/pop_validation_scripts/eu_comparison.py`

### Sensitivity analysis

- `research_code/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py`
- `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py`

## 4. Bash Scripts And Which Python Files They Execute

### Top-level

- `research_code/download_pop.sh` -> `research_code/download_pop.py`
- `research_code/combine_watersheds.sh` -> `research_code/combine_watersheds.py`
- `research_code/create_voronoi.sh` -> `research_code/create_voronoi.py`, either once for all approaches or once per selected approach depending on `execution.mode`
- `research_code/add_pop.sh` -> `research_code/add_pop.py`

### Data merge

- `research_code/data_merge/combine_locations.sh` -> `research_code/data_merge/correct_locations_w_OSM.py` -> `research_code/data_merge/merge_seg_results.py` with `--variant old` -> `research_code/data_merge/final_data_merge.py` -> `research_code/data_merge/merge_seg_results.py` with `--variant new`

### Annotation

- `research_code/annotation_scripts/grid_generation_and_osm_extract.sh` -> `research_code/annotation_scripts/NEW_01_GENERATEGRIDS.py` -> `research_code/annotation_scripts/NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py`
- `research_code/annotation_scripts/run_download_bing_annotate_array.sh` -> `research_code/annotation_scripts/download_bing_annotate.py` with `instance_id`, `--num-instances`, and `--split-seed`
- `research_code/annotation_scripts/merge_annotations.sh` -> `research_code/annotation_scripts/merge_annotations.py`
- `research_code/annotation_scripts/annotations_inspection.sh` -> `research_code/annotation_scripts/annotations_inspection.py`
- `research_code/annotation_scripts/copy_falsy_images.sh` -> `research_code/annotation_scripts/copy_falsy_images.py`

### Industrial

- `research_code/industrial_analysis/industrial_analysis.sh` -> `research_code/industrial_analysis/download_and_vectorize.py` -> `research_code/industrial_analysis/find_unconnected_industrial_areas.py`

### Figures

- `research_code/figures_scripts/convert_voronoi_to_geojson_for_map.sh` -> `research_code/figures_scripts/convert_voronoi_to_geojson_for_map.py`
- `research_code/figures_scripts/composite_area_population_plots.sh` -> `research_code/figures_scripts/composite_area_population_plots.py`
- `research_code/figures_scripts/pop_at_risk_figures.sh` -> `research_code/figures_scripts/pop_at_risk_figures.py`

### Population-at-risk and river calculations

- `research_code/pop_at_risk_river_calculations/create_rasters.sh` -> `research_code/pop_at_risk_river_calculations/create_rasters.py`
- `research_code/pop_at_risk_river_calculations/find_unserved_pop.sh` -> `research_code/pop_at_risk_river_calculations/find_unserved_pop.py`
- `research_code/pop_at_risk_river_calculations/assign_rivers_to_basin.sh` -> `research_code/pop_at_risk_river_calculations/assign_rivers_to_basin.py`, with a leading worker-count argument of `2`
- `research_code/pop_at_risk_river_calculations/find_intersection_river.sh` -> `research_code/pop_at_risk_river_calculations/find_intersection_river.py`, with a leading worker-count argument of `32`
- `research_code/pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh` -> `research_code/pop_at_risk_river_calculations/find_unserved_pop.py` -> `research_code/pop_at_risk_river_calculations/find_diff_pop.py` with leading arguments `0` and `true` -> `research_code/pop_at_risk_river_calculations/assign_rivers_to_basin.py` with leading argument `2` -> `research_code/pop_at_risk_river_calculations/find_intersection_river.py` with leading argument `32` -> `research_code/pop_at_risk_river_calculations/impact_polygons_pop.py` with leading argument `64`
- `research_code/pop_at_risk_river_calculations/find_pop_in_danger_pop.sh` -> `research_code/pop_at_risk_river_calculations/find_pop_in_danger_pop.py`

### Validation

- `research_code/pop_validation_scripts/comparison.sh` -> `research_code/pop_validation_scripts/verification_script.py` -> `research_code/pop_validation_scripts/hw_comparison.py` -> `research_code/pop_validation_scripts/eu_comparison.py`

### Sensitivity analysis

- `research_code/sensitivity_analysis_scripts/create_voronoi_param_sweep.sh` -> repeated runs of `research_code/create_voronoi.py` with `--approach 1`
- `research_code/sensitivity_analysis_scripts/add_pop_param_sweep.sh` -> repeated runs of `research_code/add_pop.py` across discovered file indexes
- `research_code/sensitivity_analysis_scripts/create_voronoi_param_sweep_parallel.sh` -> `research_code/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py` -> subprocess calls to `research_code/create_voronoi.py`
- `research_code/sensitivity_analysis_scripts/industrial_analysis_sweep.sh` -> `research_code/industrial_analysis/download_and_vectorize.py` -> `research_code/industrial_analysis/find_unconnected_industrial_areas.py`
- `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.sh` -> `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py`

## 5. Configuration Systems And Parameter Interfaces

- `research_code/config.yaml` is the single repository-wide default configuration source. The top-level and research-code READMEs both state that defaults live there rather than inside the Python modules.
- `research_code/config.yaml` contains these explicit configuration sections: `arguments`, `paths`, `s3`, `params`, `annotations`, `booleans`, `figures`, `credentials`, `execution`, and `impact_polygons_pop_params`.
- `research_code/starter.py` is the central config loader. It normalizes optional CLI values, parses optional integers, floats, and booleans, expands path templates, derives `weight_type` and `weight_func_suffix`, resolves dynamic-buffer path tokens, and returns the flattened runtime cfg dictionary.
- The shared positional override interface, parsed by `research_code/starter.py`, is: `level`, `version`, `buffer`, `weight_method`, `weight_func`, `dynamic_buffering`, `dynamic_buffer_k`.
- The config explicitly supports swappable callable names through `calculate_area_fn`, `calculate_buffer_fn`, and `prepare_data_fn` in `research_code/config.yaml`, with deferred resolution handled in `research_code/pipelines.py`.
- `research_code/create_voronoi.py` adds `--approach`, `--only_round`, and `--verbose` on top of the shared positional overrides.
- `research_code/industrial_analysis/find_unconnected_industrial_areas.py` adds `--approach`, `--only_round`, and `--verbose`, but only accepts approach `0` or `1`.
- `research_code/data_merge/merge_seg_results.py` adds `--variant` with choices `old` and `new`.
- `research_code/annotation_scripts/download_bing_annotate.py` adds `instance_id`, `--num-instances`, and `--split-seed`.
- `research_code/pop_at_risk_river_calculations/create_rasters.py` adds `job_index` and `total_jobs` before the shared overrides.
- `research_code/pop_at_risk_river_calculations/find_diff_pop.py` adds `index` and `is_parallel` before the shared overrides.
- `research_code/figures_scripts/composite_area_population_plots.py` adds `--approach`, `--color-col`, `--zonal-col`, `--hist-lower-q`, and `--hist-upper-q`.
- `research_code/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py` adds `task_id`, `version`, `--approach`, `--num-jobs`, `--retry-failed-runs`, and `--shuffle-seed`. Its positional `dynamic_buffering` and `dynamic_buffer_k` arguments are explicitly accepted only for backward compatibility and ignored.
- `research_code/create_voronoi.sh` reads `execution.mode` from `research_code/config.yaml` to choose array, sequential, or parallel wrapper behavior.
- `research_code/pop_at_risk_river_calculations/create_rasters.sh` reads `annotations.default_mode` from `research_code/config.yaml` to choose array, sequential, or parallel wrapper behavior.
- The sweep wrappers use `SLURM_ARRAY_TASK_ID` and `SHUFFLE_SEED` as execution and sharding interfaces.
- `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.sh` exports `COMPARE_POP_SWEEP_MAX_WORKERS` from `SLURM_CPUS_PER_TASK`.
- Non-YAML config surface: `research_code/annotation_scripts/download_bing_annotate.py` contains a module-level `BING_API_KEY` constant.

## 6. Model Architectures Available In The Repository

- No trainable ML architecture implementation is explicitly present in the repository code. The codebase contains no PyTorch, TensorFlow, Keras, XGBoost, LightGBM, CatBoost, or sklearn training imports, and no `train`, `fit`, `epoch`, `optimizer`, or `loss` workflow was found in the executable Python modules.
- External model outputs are consumed as inputs rather than implemented here:
- `research_code/data_merge/merge_seg_results.py`: merges segmentation outputs into geospatial datasets.
- `research_code/annotation_scripts/merge_annotations.py`: parses annotation model text responses.
- `research_code/data_merge/final_data_merge.py`: merges corrected points with country-specific datasets and model outputs.
- The explicit implemented algorithmic model families are:
- `research_code/create_voronoi.py`: Voronoi approach `0`, approach `1`, and approach `2`; optional `only_round` mode.
- `research_code/config.yaml`: `weight_method` choices `linear`, `logarithmic`, `square_root`, `sigmoid`.
- `research_code/config.yaml`: `weight_func` choices `mult`, `add`, or empty.
- `research_code/config.yaml`: rigid buffering and dynamic buffering through `dynamic_buffering` and `dynamic_buffer_k`.
- `research_code/pop_at_risk_river_calculations/impact_polygons_pop.py`: a configurable downstream impact model with `org_per_pop`, `width`, `c_limit`, `base_k`, `theta`, `step_m`, `least_discharge_cms`, and `impact_radii`.

## 7. Training, Evaluation, Merging, And Visualization Stages

- Training stage: no explicit training stage or model-training code was found.

### Merging stages

- `research_code/data_merge/combine_locations.sh`: OSM correction, segmentation merge, and final WWTP merge.
- `research_code/combine_watersheds.sh`: watershed merge.
- `research_code/annotation_scripts/merge_annotations.sh`: annotation merge back into the WWTP dataset.

### Evaluation and validation stages

- `research_code/pop_validation_scripts/comparison.sh`: verification split, HydroWaste comparison, EU comparison.
- `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.sh`: cross-file HW and EU sensitivity diagnostics over all population-enriched outputs.
- `research_code/annotation_scripts/annotations_inspection.sh`: annotation QA sampling and class-distribution inspection.

### Visualization stages

- `research_code/figures_scripts/convert_voronoi_to_geojson_for_map.sh`: map-ready GeoJSON export.
- `research_code/figures_scripts/composite_area_population_plots.sh`: composite histogram and scatter diagnostics.
- `research_code/figures_scripts/piechart_figure.py`: static world map with donut markers.
- `research_code/figures_scripts/piechart_interactive.py`: interactive Folium summary map.
- `research_code/figures_scripts/pop_at_risk_figures.sh`: risk-result visualization.

### Generation stages feeding those outputs

- `research_code/download_pop.sh`
- `research_code/create_voronoi.sh`
- `research_code/add_pop.sh`
- `research_code/pop_at_risk_river_calculations/create_rasters.sh`
- `research_code/pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh`
- `research_code/pop_at_risk_river_calculations/find_pop_in_danger_pop.sh`
- `research_code/industrial_analysis/industrial_analysis.sh`

## 8. Parameter Sweep Functionality And Hyperparameter Exploration

- Sweep-generation entrypoints:
- `research_code/sensitivity_analysis_scripts/create_voronoi_param_sweep.sh`
- `research_code/sensitivity_analysis_scripts/add_pop_param_sweep.sh`
- `research_code/sensitivity_analysis_scripts/create_voronoi_param_sweep_parallel.sh`
- `research_code/sensitivity_analysis_scripts/industrial_analysis_sweep.sh`
- Shared explicit sweep grid in the shell scripts:
- levels: `6`, `7`, `8`, `9`
- weight funcs: `mult`, `add`, empty
- weight methods: `linear`, `logarithmic`, `square_root`, `sigmoid`
- rigid buffers: `9000`, `11000`, `13000`, `15000`
- dynamic k values: `0.6`, `0.7`, `0.8`
- Shared explicit sweep logic:
- rigid-buffer regime adds combinations with `dynamic_buffering=false` and each buffer value
- dynamic-buffer regime adds combinations with `dynamic_buffering=true`, buffer fixed at `9000`, and each dynamic k value
- when `weight_func` is empty, only the canonical `weight_method=linear` is kept to avoid redundant runs
- combinations are deterministically shuffled with `random.Random(seed).shuffle(combos)`
- combinations are sharded by modulo across 10 SLURM array tasks using `idx % 10 == task_id`
- `research_code/sensitivity_analysis_scripts/create_voronoi_param_sweep.sh`: runs `research_code/create_voronoi.py` with approach fixed to `1`
- `research_code/sensitivity_analysis_scripts/add_pop_param_sweep.sh`: discovers available Voronoi output files at runtime, then runs `research_code/add_pop.py` once per discovered file index for each assigned parameter combination
- `research_code/sensitivity_analysis_scripts/create_voronoi_param_sweep_parallel.sh`: delegates to `research_code/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py`, which further splits one task's assigned combinations across internal jobs and retries failed runs with progressively fewer parallel jobs
- `research_code/sensitivity_analysis_scripts/industrial_analysis_sweep.sh`: reuses the same grid and runs the two industrial-analysis stages per combination
- Sweep-result evaluation:
- `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.sh` is not a generator sweep; it evaluates all parseable population-enriched GPKGs, deduplicates empty-weight-func duplicates, computes HW and EU metrics, writes alias maps and ranking CSVs, and saves sensitivity-score figures

## 9. Dependencies Between Scripts

### Output and stage dependencies

- `research_code/data_merge/combine_locations.sh` produces the canonical WWTP datasets consumed by later WWTP-based stages, including `research_code/create_voronoi.py`, `research_code/annotation_scripts/grid_generation_and_osm_extract.sh`, and the industrial-analysis branch.
- `research_code/combine_watersheds.sh` produces the combined watershed dataset consumed by approach-1 Voronoi, industrial boundary enrichment, and river-risk stages.
- `research_code/annotation_scripts/merge_annotations.sh` writes annotation-derived fields back into the main corrected points layer that later WWTP stages use.
- `research_code/download_pop.sh` produces country TIFF inputs consumed by `research_code/add_pop.py` and `research_code/pop_at_risk_river_calculations/create_rasters.py`.
- `research_code/create_voronoi.sh` produces Voronoi layers consumed by `research_code/add_pop.sh`.
- `research_code/add_pop.sh` produces population-enriched Voronoi outputs consumed by `research_code/pop_at_risk_river_calculations/create_rasters.sh`, `research_code/pop_validation_scripts/comparison.sh`, the figure scripts, and `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py`.
- `research_code/pop_at_risk_river_calculations/create_rasters.sh` feeds `research_code/pop_at_risk_river_calculations/find_unserved_pop.py`.
- `research_code/pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh` encodes an explicit serial dependency chain: `find_unserved_pop` -> `find_diff_pop` -> `assign_rivers_to_basin` -> `find_intersection_river` -> `impact_polygons_pop`.
- `research_code/pop_at_risk_river_calculations/find_pop_in_danger_pop.sh` depends on the impact outputs produced by the previous chain.
- `research_code/industrial_analysis/download_and_vectorize.py` writes the merged industrial layer consumed by `research_code/industrial_analysis/find_unconnected_industrial_areas.py`.

### Shared code dependencies

- `research_code/starter.py` is the central dependency for runtime configuration; nearly every executable module imports `load_config` and `parse_config_overrides`.
- `research_code/pipelines.py` is the shared orchestration dependency for output-path generation, data preparation, and Voronoi execution; it is reused by `research_code/create_voronoi.py`, `research_code/industrial_analysis/find_unconnected_industrial_areas.py`, `research_code/figures_scripts/convert_voronoi_to_geojson_for_map.py`, `research_code/figures_scripts/composite_area_population_plots.py`, `research_code/figures_scripts/piechart_figure.py`, `research_code/figures_scripts/piechart_interactive.py`, `research_code/pop_at_risk_river_calculations/create_rasters.py`, and `research_code/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py`.
- `research_code/create_voronoi.py` also acts as a shared utility module; helpers from it are imported by `research_code/download_pop.py`, `research_code/combine_watersheds.py`, the data-merge scripts, annotation scripts, validation scripts, industrial-analysis scripts, and population-at-risk scripts.
- `research_code/annotation_scripts/merge_annotations.py` provides `decode_gen_text`, which is reused by `research_code/annotation_scripts/annotations_inspection.py`.
- `research_code/pop_validation_scripts/hw_comparison.py` provides `ndvi`, `multiples`, and `replace_inf`, which are reused by `research_code/pop_validation_scripts/eu_comparison.py` and `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py`.
- `research_code/data_merge/merge_seg_results.py` provides `assign_to_nearest`, which is reused by `research_code/pop_validation_scripts/eu_comparison.py` and `research_code/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py`.