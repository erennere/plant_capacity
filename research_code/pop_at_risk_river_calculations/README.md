# pop_at_risk_river_calculations

This folder computes downstream population risk once Voronoi and population attachment are complete. Conceptually, it identifies non-served population, links those areas to river systems, propagates impact downstream, and then aggregates final at-risk results. You can run scripts independently, but production runs usually use the orchestration shell wrappers. Inputs come from previous pipeline stages and HydroSHEDS layers.

## How This Folder Is Run

From `research_code/`:

```bash
bash pop_at_risk_river_calculations/create_rasters.sh
bash pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh
bash pop_at_risk_river_calculations/find_pop_in_danger_pop.sh
```

## Python Scripts (Logic)

### create_rasters.py
Aim: Build raster inputs for non-served analysis. Inputs: Voronoi/population outputs and country raster paths. Outputs: Served/non-served raster products. How: It prepares geospatial inputs, performs raster operations, and writes stage outputs.

### find_unserved_pop.py
Aim: Extract non-served population areas from raster outputs. Inputs: Raster products and configured thresholds. Outputs: Vectorized non-served area layers. How: It thresholds raster values, vectorizes candidates, and writes geospatial outputs.

### find_diff_pop.py
Aim: Compute population differences used by downstream impact logic. Inputs: Served/non-served products and baseline references. Outputs: Difference layers/tables. How: It aligns data products and calculates per-unit differences.

### assign_rivers_to_basin.py
Aim: Assign river segments to basin identifiers. Inputs: River and basin datasets. Outputs: Basin-linked river layer. How: It runs spatial intersection/join logic and writes enriched river data.

### find_intersection_river.py
Aim: Link non-served areas to nearest/intersecting rivers. Inputs: Non-served areas and basin-linked rivers. Outputs: Non-served features with river linkage fields. How: It uses spatial indexing and nearest/intersection calculations.

### impact_polygons_pop.py
Aim: Propagate downstream impact and build impact polygons. Inputs: River-linked non-served features and propagation parameters. Outputs: Impact polygons and impact population summaries. How: It traverses connected structures, applies propagation rules, and exports impact layers.

### find_pop_in_danger_pop.py
Aim: Aggregate final population-at-risk outputs. Inputs: Impact outputs from previous step. Outputs: Final at-risk population tables/files. How: It aggregates impacted populations by configured units and writes final deliverables.

## Shell Scripts (Entry Points)

### create_rasters.sh
Aim: Launcher for raster-creation stage with mode handling. Inputs: Config defaults and optional mode overrides. Outputs: Raster stage logs and outputs. How: It dispatches `create_rasters.py` in configured mode.

### find_unserved_pop.sh
Aim: Launcher for non-served area extraction. Inputs: Config defaults and optional overrides. Outputs: Stage logs and non-served outputs. How: It executes `python -m ...find_unserved_pop`.

### assign_rivers_to_basin.sh
Aim: Launcher for basin assignment stage. Inputs: Config defaults and optional overrides. Outputs: Assignment logs and basin-linked river output. How: It executes `python -m ...assign_rivers_to_basin`.

### find_intersection_river.sh
Aim: Launcher for river intersection stage. Inputs: Config defaults and optional overrides. Outputs: Intersection logs and linked outputs. How: It executes `python -m ...find_intersection_river`.

### pop_differences_and_impact_polygons.sh
Aim: One-command orchestration for middle risk stages. Inputs: Outputs from create_rasters stage and config settings. Outputs: Difference products, river-linked products, and impact polygons. How: It runs find_unserved_pop, find_diff_pop, assign_rivers_to_basin, find_intersection_river, and impact_polygons_pop in sequence.

### find_pop_in_danger_pop.sh
Aim: Launcher for final risk aggregation stage. Inputs: Impact outputs and config defaults. Outputs: Final population-at-risk logs and result files. How: It executes `python -m ...find_pop_in_danger_pop`.

## Shell -> Python Flow Diagram

```text
create_rasters.sh -> create_rasters.py
pop_differences_and_impact_polygons.sh
	-> find_unserved_pop.py
	-> find_diff_pop.py
	-> assign_rivers_to_basin.py
	-> find_intersection_river.py
	-> impact_polygons_pop.py
find_pop_in_danger_pop.sh -> find_pop_in_danger_pop.py
```
