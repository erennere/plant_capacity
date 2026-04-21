# annotation_scripts

This folder handles annotation preparation and post-processing around the WWTP points. In simple terms, it creates map tiles and OSM context that help manual or model-assisted labeling, then merges annotation results back into the core WWTP dataset. You typically run these scripts after data merge and before Voronoi/population stages. All file locations and operational settings come from `research_code/config.yaml`.

## How This Folder Is Run

From `research_code/`:

```bash
bash annotation_scripts/grid_generation_and_osm_extract.sh
sbatch annotation_scripts/run_download_bing_annotate_array.sh
sbatch annotation_scripts/merge_annotations.sh
sbatch annotation_scripts/annotations_inspection.sh
```

## Python Scripts (Logic)

### NEW_01_GENERATEGRIDS.py
Aim: Build annotation grids around WWTP points. Inputs: Corrected points and grid parameters from config. Outputs: Grid files keyed by tile/index. How: It computes tile geometry from points and writes normalized grid layers.

### NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py
Aim: Collect OSM context for each generated grid tile. Inputs: Grid geometries and OSM query settings. Outputs: Tile-level OSM GeoJSON features. How: It loops through grids, queries OSM, harmonizes schema, and saves outputs.

### NEW_03_WASTEWATERJOIN_GEOJSON.py
Aim: Join OSM wastewater context back to WWTP/grid records. Inputs: OSM extraction outputs and WWTP/grid identifiers. Outputs: Joined geospatial metadata tables. How: It performs key/spatial joins and writes merged records. This script is currently not used and commented out. Annotations work directly with the GeoJSONs file above without merging them into a single file.

### NEW_04_EXPORTGEOTIFF.py
Aim: Export selected geospatial products as GeoTIFF. Inputs: Prepared geospatial layers and export parameters. Outputs: GeoTIFF files. How: It transforms geometry/raster settings and writes raster outputs. This script at the moment is not used and needed. It is also not tested. 

### download_bing_annotate.py
Aim: Download imagery for each tile and prepare annotation assets. Inputs: Grid layer plus imagery source settings. Outputs: Image tiles and tile metadata logs. How: It iterates tiles, requests imagery, saves files, and stores index mappings.

### merge_annotations.py
Aim: Merge annotation outputs into the main corrected points layer. Inputs: Annotation CSV/text outputs and corrected points file. Outputs: Updated corrected points with annotation-derived fields. How: It parses annotation data, matches by index/tile identifiers, and writes merged geospatial output.

### annotations_inspection.py
Aim: Create QA sampling artifacts for human review. Inputs: Annotation outputs and image folders. Outputs: QA tables, distributions, and copied sample images. How: It computes class summaries, samples stratified examples, and exports review artifacts.

## Shell Scripts (Entry Points)

### grid_generation_and_osm_extract.sh
Aim: One-command orchestration for grid creation and OSM extraction. Inputs: Config defaults and optional overrides. Outputs: Grid/OSM artifacts and logs. How: It runs NEW_01 then NEW_02 in sequence.

### run_download_bing_annotate_array.sh
Aim: Cluster launcher for large image-download jobs. Inputs: SLURM array task context and config. Outputs: Scheduler logs and imagery outputs. How: It dispatches `download_bing_annotate.py` shards using array jobs.

### merge_annotations.sh
Aim: Reproducible launcher for annotation merge stage. Inputs: Config and optional overrides. Outputs: Merge logs and updated corrected points. How: It executes `python -m research_code.annotation_scripts.merge_annotations`.

### annotations_inspection.sh
Aim: Reproducible launcher for QA sampling stage. Inputs: Config and optional overrides. Outputs: QA logs and inspection artifacts. How: It executes `python -m research_code.annotation_scripts.annotations_inspection`.

## Shell -> Python Flow Diagram

```text
grid_generation_and_osm_extract.sh -> NEW_01_GENERATEGRIDS.py -> NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py
run_download_bing_annotate_array.sh -> download_bing_annotate.py
merge_annotations.sh -> merge_annotations.py
annotations_inspection.sh -> annotations_inspection.py
```
