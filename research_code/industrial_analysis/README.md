# industrial_analysis

This folder contains the industrial-coverage branch of the pipeline. It identifies where industrial land is present and then evaluates which industrial areas are not covered by industrial or mixed WWTP service regions.

## How This Folder Is Run

Run from research_code/:

```bash
bash industrial_analysis/industrial_analysis.sh
```

This wrapper runs two Python stages in sequence:

1. `python -m research_code.industrial_analysis.download_and_vectorize`
2. `python -m research_code.industrial_analysis.find_unconnected_industrial_areas`

## Scripts

### download_and_vectorize.py
Aim: Build industrial-land polygons and enrich them with boundary attributes.

What it does:
- Downloads and extracts the industrial raster archive.
- Vectorizes raster pixels to polygons in parallel.
- Filters out tiny connected components using `params.industrial_min_cells`.
- Writes a cached pre-enrichment layer at `data/industrial_analysis/industrial_areas_mp{industrial_min_cells}.gpkg`.
- Adds `ISO_2` and watershed basin attributes and writes the final configured output (`paths.industrial_merged_gpkg`).

Caching behavior:
- If `paths.industrial_merged_gpkg` already exists and `params.industrial_vectorize_overwrite=false`, the stage exits early.
- If the cached `industrial_areas_mp...gpkg` exists and overwrite is false, vectorization is skipped and the workflow resumes from boundary enrichment.

Raster persistence behavior:
- `params.industrial_persist_rasters=false` (default): download/extract happens in a temporary directory.
- `params.industrial_persist_rasters=true`: rasters are kept under `paths.industrial_raster_persistent_dir` and reused on later runs when overwrite is false.

### find_unconnected_industrial_areas.py
Aim: Identify industrial polygons not served by industrial/mixed WWTP Voronoi service areas.

What it does:
- Loads industrial polygons from `paths.industrial_merged_gpkg`.
- Loads WWTP points and filters industrial/mixed facilities via `params.industrial_category_numbers`.
- Builds service regions using Voronoi approach 0 or 1.
- Performs spatial overlap checks and exports uncovered polygons to `paths.industrial_unconnected_output`.

## Key Parameters

Configured in research_code/config.yaml:

- `params.industrial_zenodo_url`: industrial raster source archive.
- `params.industrial_min_cells`: minimum connected pixels retained during vectorization.
- `params.industrial_persist_rasters`: enables persistent raster cache.
- `params.industrial_vectorize_overwrite`: controls rerun behavior for vectorization stage.
- `params.industrial_unconnected_overwrite`: controls rerun behavior for unconnected-areas stage.
- `params.industrial_category_numbers`: categories treated as industrial/mixed WWTPs.
- `params.basin_column_name`: watershed basin key used in intersections.

## Outputs

- `paths.industrial_merged_gpkg`: enriched industrial-land layer.
- `paths.industrial_unconnected_output`: uncovered industrial polygons.
- `data/industrial_analysis/industrial_areas_mp{industrial_min_cells}.gpkg`: cached intermediate layer for fast reruns.
