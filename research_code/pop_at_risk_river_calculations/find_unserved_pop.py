"""Filter non-served population polygons above a threshold and export to GeoPackage.

This script reads a CSV of non-served polygons (WKT geometry), filters rows by
`pop_sum > threshold`, and writes the result to a GPKG file.
"""

import duckdb
import os
import logging
import geopandas as gpd
from shapely import from_wkt
try:
    from ..starter import load_config
except ImportError:
    from research_code.starter import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_unserved_pop(filepath, threshold, output_filepath):
    """Create a GeoPackage with non-served polygons above a population threshold.

    Parameters
    ----------
    filepath : str
        Input CSV path containing at least `pop_sum` and `geometry` columns.
    threshold : int
        Minimum population sum to keep a polygon.
    output_filepath : str
        Output GPKG path.

    Returns
    -------
    int
        Number of output rows written.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Input CSV not found: {filepath}")

    logger.info("Filtering non-served polygons from %s with threshold=%s", filepath, threshold)

    query = f"""
    SELECT *
    FROM read_csv('{filepath}',
        header=True,
        all_varchar=True,
        max_line_size=10000000
        )
    WHERE TRY_CAST(pop_sum AS BIGINT) > {int(threshold)}
    """
    df = duckdb.sql(query).df()

    if df.empty:
        logger.warning("No rows matched threshold=%s. Writing empty output file.", threshold)
    if "geometry" not in df.columns:
        raise ValueError("Input CSV is missing required 'geometry' column")

    df['geometry'] = df.geometry.apply(from_wkt)
    df = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    df.to_file(output_filepath, driver='GPKG', index=False)
    logger.info("Wrote %s polygons to %s", len(df), output_filepath)
    return len(df)

def main():
    """Load config and run threshold-based non-served population extraction."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()

    figures_cfg = cfg.get("figures") or {}
    if "pop_threshold" not in figures_cfg:
        raise KeyError("Missing 'pop_threshold' in config under 'figures'")

    threshold = int(figures_cfg["pop_threshold"])
    non_served_outpath = os.path.abspath(cfg["paths"]["non_served_outpath"].replace('.gpkg', '.csv'))
    non_served_above_threshold_outpath = os.path.abspath(cfg["paths"]["non_served_above_threshold_outpath"])
    rows_written = create_unserved_pop(
        non_served_outpath,
        threshold,
        non_served_above_threshold_outpath,
    )
    logger.info("Completed non-served population extraction. rows_written=%s", rows_written)

if __name__ == "__main__":
    main()