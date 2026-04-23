# figures_scripts

This folder produces human-facing outputs from pipeline data, especially charts and map-ready layers. Think of it as the communication layer of the project: the data science is done earlier, and this folder packages results into visuals for review and sharing. Inputs mostly come from Voronoi/population outputs. Outputs are static images, HTML charts, and map GeoJSON files.

## How This Folder Is Run

From `research_code/`:

```bash
bash figures_scripts/convert_voronoi_to_geojson_for_map.sh
bash figures_scripts/composite_area_population_plots.sh
python -m research_code.figures_scripts.piechart_figure
python -m research_code.figures_scripts.piechart_interactive
```

## Python Scripts (Logic)

### convert_voronoi_to_geojson_for_map.py
Aim: Convert Voronoi layers into lightweight map-friendly GeoJSON. Inputs: Voronoi geospatial layers and export settings from config. Outputs: GeoJSON files for web map consumption. How: It selects required fields, normalizes geometry/CRS, and writes map-ready outputs.

### piechart_figure.py
Aim: Generate static pie-chart summaries for reporting. Inputs: Aggregated pipeline outputs, plotting settings, and `paths.raster_country_stats_filepath` from config. Outputs: PNG figure files. How: It computes grouped shares and renders styled static charts.

### piechart_interactive.py
Aim: Generate interactive pie-chart visualizations. Inputs: Aggregated category data, interactive layout settings, and `paths.raster_country_stats_filepath` from config. Outputs: HTML interactive charts. How: It builds interactive traces/layout and writes standalone HTML pages.

### composite_area_population_plots.py
Aim: Generate two composite diagnostic figures for area/population ratios. Inputs: Pop-Voronoi layer selected via create_pop_output_paths, country boundaries, configured zonal-sum default column, and plotting options. Outputs: One histogram composite and one scatter composite image in the standard figures output directory scheme. How: It merges by ISO_2/ISO_A2, computes facility-level and country-level ratios, trims histogram outliers by quantiles, and renders gridded scatter plots with 1:1 dashed lines and ISO labels.

## Shell Scripts (Entry Points)

### convert_voronoi_to_geojson_for_map.sh
Aim: Reproducible command wrapper for map GeoJSON export. Inputs: Config defaults and optional overrides. Outputs: Conversion logs and map GeoJSON outputs. How: It executes `python -m research_code.figures_scripts.convert_voronoi_to_geojson_for_map`.

### composite_area_population_plots.sh
Aim: HPC-friendly wrapper for composite histogram/scatter figure generation. Inputs: Standard config overrides plus optional approach and boundary color column. Outputs: Composite plot logs and PNG files under data/figures. How: It executes `python -m research_code.figures_scripts.composite_area_population_plots`.

## Shell -> Python Flow Diagram

```text
convert_voronoi_to_geojson_for_map.sh -> convert_voronoi_to_geojson_for_map.py
composite_area_population_plots.sh -> composite_area_population_plots.py
```
