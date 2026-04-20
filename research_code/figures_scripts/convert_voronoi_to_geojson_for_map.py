"""Convert Voronoi polygons into centroid points for lightweight web mapping.

The output GeoJSON keeps selected attributes but replaces each polygon geometry
with its centroid so large global layers can be visualized efficiently in Leaflet.
"""

import os
import geopandas as gpd
import pandas as pd
try:
    from ..starter import load_config, parse_config_overrides
    from ..pipelines import create_pop_output_paths
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.pipelines import create_pop_output_paths

def main():
    """Load the configured Voronoi layer, convert polygons to centroids, and export GeoJSON."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(**overrides)

    approach = cfg['figures']['approach']
    input_filepath = create_pop_output_paths(cfg)['voronoi'][approach]
    gdf = gpd.read_file(input_filepath, columns=['geometry', 'total_area', 'round_area'])
    gdf['geometry'] = gdf.geometry.apply(lambda geom: geom.centroid if pd.notna(geom) else None)
    gdf.to_file(cfg['paths']['leaflet_geojson_filepath'], driver='GeoJSON', index=False)

if __name__ == "__main__":
    main()