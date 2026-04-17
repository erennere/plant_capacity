import os
import geopandas as gpd
import pandas as pd
try:
    from ..starter import load_config
    from ..pipelines import create_pop_output_paths
except ImportError:
    from research_code.starter import load_config
    from research_code.pipelines import create_pop_output_paths

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()

    approach = cfg['figures']['approach']
    input_filepath = create_pop_output_paths(cfg)['voronoi'][approach]
    gdf = gpd.read_file(input_filepath, columns=['geometry', 'total_area', 'round_area'])
    gdf['geometry'] = gdf.geometry.apply(lambda geom: geom.centroid if pd.notna(geom) else None)
    gdf.to_file(cfg['paths']['leaflet_geojson_filepath'], driver='GeoJSON', index=False)

if __name__ == "__main__":
    main()