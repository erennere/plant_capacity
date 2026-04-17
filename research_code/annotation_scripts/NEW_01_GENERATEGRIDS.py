import logging
import os
import sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely import Point

try:
    from ..starter import load_config
except ImportError:
    from research_code.starter import load_config

def point_to_square(geom, half):
    if geom is None or geom.is_empty:
        return None
    return box(
        geom.x - half, geom.y - half,
        geom.x + half, geom.y + half
    )

def main(cell_size, half, points_path, output_path):
    # === Step 1. Read point layer ===
    gdf = gpd.read_file(points_path)
    gdf = gdf[gdf['geometry'].map(lambda geom: pd.notna(geom) and isinstance(geom, Point))].reset_index(drop=True)
    if not all(gdf.geometry.geom_type == "Point"):
        raise ValueError("Input layer must contain only Point geometries")

    # === Step 2. Reproject to Web Mercator ===
    gdf_3857 = gdf.to_crs(epsg=3857)
    gdf_3857["geometry"] = gdf_3857.geometry.apply(lambda geom: point_to_square(geom, half))
    gdf_3857.set_crs(epsg=3857, inplace=True)

    # === Step 5. Write to disk ===
    gdf_3857.to_file(
        output_path,
        driver="GPKG"
    )
    print(f"✅ Created {cell_size:.2f} m × {cell_size:.2f} m grid squares (Zoom 17, 3072×3072 px).")

if __name__ == '__main__':
    logging.info("Starting Bing annotation pipeline")
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    logging.info("Configuration loaded")

    cell_size = int(cfg["annotations"]["cell_size"])
    factor = float(cfg["annotations"]["factor"])
    cell_size = cell_size * factor   # meters (~3667.97 m)
    half = cell_size / 2

    points_path = cfg["paths"]["corrected_all_filepath"]
    #points_path = './annotation_scripts/ref.geojson'
    output_dir = cfg["paths"]["annotations_grid_dir"]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'grids_{os.path.basename(points_path)}')
    main(cell_size, half, points_path, output_path)

