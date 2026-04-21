# figures_scripts

This folder produces human-facing outputs from pipeline data, especially charts and map-ready layers. Think of it as the communication layer of the project: the data science is done earlier, and this folder packages results into visuals for review and sharing. Inputs mostly come from Voronoi/population outputs. Outputs are static images, HTML charts, and map GeoJSON files.

## How This Folder Is Run

From `research_code/`:

```bash
bash figures_scripts/convert_voronoi_to_geojson_for_map.sh
python -m research_code.figures_scripts.piechart_figure
python -m research_code.figures_scripts.piechart_interactive
```

## Python Scripts (Logic)

### convert_voronoi_to_geojson_for_map.py
Aim: Convert Voronoi layers into lightweight map-friendly GeoJSON. Inputs: Voronoi geospatial layers and export settings from config. Outputs: GeoJSON files for web map consumption. How: It selects required fields, normalizes geometry/CRS, and writes map-ready outputs.

### piechart_figure.py
Aim: Generate static pie-chart summaries for reporting. Inputs: Aggregated pipeline outputs and plotting settings. Outputs: PNG figure files. How: It computes grouped shares and renders styled static charts.

### piechart_interactive.py
Aim: Generate interactive pie-chart visualizations. Inputs: Aggregated category data and interactive layout settings. Outputs: HTML interactive charts. How: It builds interactive traces/layout and writes standalone HTML pages.

## Shell Scripts (Entry Points)

### convert_voronoi_to_geojson_for_map.sh
Aim: Reproducible command wrapper for map GeoJSON export. Inputs: Config defaults and optional overrides. Outputs: Conversion logs and map GeoJSON outputs. How: It executes `python -m research_code.figures_scripts.convert_voronoi_to_geojson_for_map`.

## Shell -> Python Flow Diagram

```text
convert_voronoi_to_geojson_for_map.sh -> convert_voronoi_to_geojson_for_map.py
```
