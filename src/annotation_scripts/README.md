# annotation_scripts

## Sections
| Section | Purpose |
| --- | --- |
| `What This Module Does` | Explains the aim of the annotation stage |
| `How It Fits In` | Shows where this stage sits in the wider workflow |
| `Scripts in This Folder` | Summarises the role, inputs, and outputs of each script |
| `Execution Flow` | Gives a compact visual view of the stage |
| `Run Instructions` | Lists the commands most users will actually run |
| `Smart Behaviors` | Notes implementation details that matter in practice |
| `Parameters` | Collects the configuration settings specific to this stage |
| `Known Issues / TODOs` | Flags current caveats and limitations |

## What This Module Does
This module prepares and merges annotation context around WWTP points. It creates grids, extracts OSM features, renders annotation imagery, and then merges the annotation results back into the corrected WWTP dataset.

## How It Fits In
It runs after the main WWTP merge and before the Voronoi and population stages when annotation-derived context is needed. Its outputs are used for QA, downstream filtering, and explanation of WWTP type labels.

## Scripts in This Folder
| Script | Role | What it does | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `grid_generation_and_osm_extract.sh` | shell launcher | Runs grid generation and OSM extraction in sequence | config values and positional overrides | grid files and OSM context files |
| `NEW_01_GENERATEGRIDS.py` | Python worker | Builds annotation grids around corrected WWTP points | corrected WWTP layer, grid parameters | grid GeoPackages |
| `NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py` | Python worker | Extracts OSM context for each grid | grid files, OSM settings, retries | per-grid OSM GeoJSON |
| `run_download_bing_annotate_array.sh` | shell launcher | Launches image rendering as a SLURM array | array task id, split seed, config overrides | per-task logs and annotated images |
| `download_bing_annotate.py` | Python worker | Downloads imagery and creates annotation overlays | imagery directory, OSM context, tile ids | annotated image assets |
| `merge_annotations.sh` | shell launcher | Runs the merge-back stage | config values and positional overrides | merge logs and updated corrected-all file |
| `merge_annotations.py` | Python worker | Parses annotation text and merges labels into WWTP points | annotation CSV, annotated images, corrected layer | updated corrected-all GeoPackage |
| `annotations_inspection.sh` | shell launcher | Runs QA sampling and inspection | config values and positional overrides | QA logs and review images |
| `annotations_inspection.py` | Python worker | Builds class summaries and inspection samples | annotated images and annotation results | QA sample set |
| `copy_falsy_images.sh` | shell launcher | Copies selected images for review | config values and positional overrides | copied review images |
| `copy_falsy_images.py` | Python worker | Copies a filtered image subset for QA | annotated images and selection criteria | copied image subset |
| `NEW_03_WASTEWATERJOIN_GEOJSON.py` | Python worker | Optional join helper for GeoJSON/parquet intermediates | OSM outputs and temp parquet files | joined geodata |
| `NEW_04_EXPORTGEOTIFF.py` | Python worker | Optional geotiff export helper | prepared geospatial layers | GeoTIFF outputs |

## Execution Flow
```mermaid
graph TD
  A([grid_generation_and_osm_extract.sh]) --> B[NEW_01_GENERATEGRIDS.py]
  B --> C[NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py]
  C --> D[(data/annotations/osm_by_idx/*)]

  E([run_download_bing_annotate_array.sh]) --> F[download_bing_annotate.py]
  F --> G[(annotated image outputs)]

  H([merge_annotations.sh]) --> I[merge_annotations.py]
  I --> J[(annotation-enriched WWTP layer)]

  K([annotations_inspection.sh]) --> L[annotations_inspection.py]
  M([copy_falsy_images.sh]) --> N[copy_falsy_images.py]
```

## Run Instructions
### Core flow
```bash
cd src
bash annotation_scripts/grid_generation_and_osm_extract.sh
sbatch annotation_scripts/run_download_bing_annotate_array.sh
bash annotation_scripts/merge_annotations.sh
```

### QA utilities
```bash
cd src
bash annotation_scripts/annotations_inspection.sh
bash annotation_scripts/copy_falsy_images.sh
```

A successful run produces grid files, OSM GeoJSON, annotated images, and a merged corrected-all file. The QA scripts write sampled review outputs and logs under `logs/`.

## Smart Behaviors
- `annotations_inspection.py` uses `annotations.n_sample_size` with a default fallback of `1000` and saves figures in headless runs instead of relying on an interactive display.
- `merge_annotations.py` parses the image index defensively from filenames and drops any existing annotation columns before re-merging to avoid duplicate `_x`/`_y` columns.
- `download_bing_annotate.py` reads `annotations_images_dir` and `annotated_images_output_dir` from config, so local imagery paths can be swapped without touching code.
- The SLURM array wrapper partitions rendering work by array task and split seed.

Delete the grid, annotation, or merged outputs to force a rerun of the corresponding stage.

## Parameters
| Config key | Default | Effect |
| --- | --- | --- |
| `NEW_01_GENERATEGRIDS.annotations.cell_size` | `3072` | Grid cell size |
| `NEW_01_GENERATEGRIDS.annotations.factor` | `1.194` | Grid expansion factor |
| `NEW_02_EXTRACTOSMDATAFULL_GEOJSON.annotations.max_workers` | `8` | OSM query parallelism |
| `NEW_02_EXTRACTOSMDATAFULL_GEOJSON.annotations.overwrite` | `false` | Overwrite existing OSM outputs |
| `NEW_02_EXTRACTOSMDATAFULL_GEOJSON.annotations.retries` | `5` | OSM retry count |
| `annotations_inspection.annotations.n_sample_size` | `1000` | QA sample size |
| `annotations_inspection.annotations.random_seed` | `42` | QA sampling seed |
| `download_bing_annotate.paths.annotations_images_dir` | ⚠️ path | Source imagery directory |
| `download_bing_annotate.paths.annotated_images_output_dir` | ⚠️ path | Annotated image output directory |
| `merge_annotations.paths.annotations_results_filepath` | ⚠️ path | Annotation results CSV |
| `annotations_inspection.paths.annotations_verf_image_outpath_dir` | ⚠️ path | QA image output directory |

## Known Issues / TODOs
- `NEW_03_WASTEWATERJOIN_GEOJSON.py` and `NEW_04_EXPORTGEOTIFF.py` exist as optional helpers but are not part of the primary shell chain.
- No explicit `TODO` or `FIXME` markers were found in this module.
