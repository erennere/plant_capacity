"""Convert Voronoi polygons into centroid points for lightweight web mapping.

The output GeoJSON keeps selected attributes but replaces each polygon geometry
with its centroid so large global layers can be visualized efficiently in Leaflet.
"""

import os
import re
import geopandas as gpd
import pandas as pd
try:
    from ..starter import load_config, parse_config_overrides
    from ..pipelines import create_pop_output_paths
    from ..create_voronoi import ensure_output_dir_for_file
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.pipelines import create_pop_output_paths
    from research_code.create_voronoi import ensure_output_dir_for_file

def main():
    """Load the configured Voronoi layer, convert polygons to centroids, and export GeoJSON."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(**overrides)

    approach = str(cfg['figures']['approach'])
    input_filepath = create_pop_output_paths(cfg)['voronoi'][approach]
    gdf = gpd.read_file(input_filepath, columns=['geometry', 'total_area', 'round_area'])
    gdf['geometry'] = gdf.geometry.apply(lambda geom: geom.centroid if pd.notna(geom) else None)
    ensure_output_dir_for_file(cfg['paths']['leaflet_geojson_filepath'])
    gdf.to_file(cfg['paths']['leaflet_geojson_filepath'], driver='GeoJSON', index=False)

    # Keep the static leaflet demo in sync with the configured GeoJSON output.
    html_path = os.path.join(cfg['paths']['data_dir'], 'figures', 'sizes_interactive_map.html')
    if os.path.exists(html_path):
        with open(html_path, 'r', encoding='utf-8') as file:
            html = file.read()

        rel_geojson = os.path.relpath(
            cfg['paths']['leaflet_geojson_filepath'],
            start=os.path.dirname(html_path)
        ).replace('\\', '/')
        if not rel_geojson.startswith('.'):
            rel_geojson = f'./{rel_geojson}'

        updated_html = re.sub(
            r'fetch\("[^"]+"\)',
            f'fetch("{rel_geojson}")',
            html,
            count=1
        )

        with open(html_path, 'w', encoding='utf-8') as file:
            file.write(updated_html)

if __name__ == "__main__":
    main()