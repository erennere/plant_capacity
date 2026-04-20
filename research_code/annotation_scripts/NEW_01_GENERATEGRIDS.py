"""Build square annotation tiles around WWTP point features.

The output grid is derived from the current `corrected_all_filepath` point layer
and uses the configured annotation cell size and scale factor.
"""

import logging
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely import Point

try:
    from ..starter import load_config, parse_config_overrides
    from ..create_voronoi import ensure_output_dir_for_file
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.create_voronoi import ensure_output_dir_for_file

def point_to_square(geom, half):
    """Return a square polygon centered on a point geometry."""
    if geom is None or geom.is_empty:
        return None
    return box(
        geom.x - half, geom.y - half,
        geom.x + half, geom.y + half
    )

def main(cell_size, half, points_path, output_path):
    """Read points, expand them to square tiles, and write a GeoPackage grid.

    Parameters
    ----------
    cell_size : float
        Full annotation tile width in meters.
    half : float
        Half-width used to build centered square polygons.
    points_path : str
        Input point layer.
    output_path : str
        Output GeoPackage path for the generated grid.
    """
    gdf = gpd.read_file(points_path)
    gdf = gdf[gdf['geometry'].map(lambda geom: pd.notna(geom) and isinstance(geom, Point))].reset_index(drop=True)
    if not all(gdf.geometry.geom_type == "Point"):
        raise ValueError("Input layer must contain only Point geometries")

    # Create the square tiles in Web Mercator so the cell size is interpreted in meters.
    gdf_3857 = gdf.to_crs(epsg=3857)
    gdf_3857["geometry"] = gdf_3857.geometry.apply(lambda geom: point_to_square(geom, half))
    gdf_3857.set_crs(epsg=3857, inplace=True)

    ensure_output_dir_for_file(output_path)
    gdf_3857.to_file(
        output_path,
        driver="GPKG"
    )
    print(f"✅ Created {cell_size:.2f} m × {cell_size:.2f} m grid squares (Zoom 17, 3072×3072 px).")

if __name__ == '__main__':
    logging.info("Starting Bing annotation pipeline")
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(**overrides)
    logging.info("Configuration loaded")

    cell_size = int(cfg["annotations"]["cell_size"])
    factor = float(cfg["annotations"]["factor"])
    cell_size = cell_size * factor   # meters (~3667.97 m)
    half = cell_size / 2

    points_path = cfg["paths"]["corrected_all_filepath"]
    output_dir = cfg["paths"]["annotations_grid_dir"]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'grids_{os.path.basename(points_path)}')
    main(cell_size, half, points_path, output_path)

