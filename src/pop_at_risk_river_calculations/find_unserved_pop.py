"""Filter non-served population polygons above a threshold and export to GeoPackage.

This script reads a CSV of non-served polygons (WKT geometry), filters rows by
`pop_sum > threshold`, and writes the result to a GPKG file.
"""
import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd
from shapely import from_wkt, to_wkt

from ..starter import add_standard_override_arguments, load_config, parse_config_overrides
from ..geo_utils import ensure_duckdb_spatial
from ..utils import configure_logging, duckdb_connection, ensure_output_dir_for_file
from .find_pop_in_danger_pop import finding_tiles

logger = logging.getLogger(__name__)

def read_urban_areas(filepath):
    try:
        return gpd.read_file(filepath)
    except Exception as e:
        logger.error("Failed to read urban areas file: %s", e)
        raise Exception(f"Failed to read urban areas file: {filepath}") from e
    
def add_bbox(gdf, zoom_level, max_workers, is_parallel):
    def chunk_find_bbox(chunk, zoom_level):
        chunk['tiles'] = chunk['geometry'].map(lambda geom: finding_tiles(geom, zoom_level))
        return chunk

    if is_parallel:
        gdf['indx'] = range(len(gdf))
        chunks = [gdf[['indx', 'geometry']].iloc[i:i + max_workers] for i in range(0, len(gdf), max_workers)]
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(chunk_find_bbox, chunk, zoom_level) for chunk in chunks]
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error("Error processing chunk: %s", e)
                    raise

        results = pd.concat(results).drop(columns=['geometry'])
        gdf = gdf.merge(results, on='indx', how='left').drop(columns=['indx'])
    else:
        gdf['tiles'] = gdf['geometry'].map(lambda geom: finding_tiles(geom, zoom_level))
    return gdf

def add_buffers_to_WWTP(gdf, wwtp_filepath, wwtp_buffer, zoom_level, 
                        max_workers, is_parallel, wwtp_country_col):
    logger.info(
        "Starting add_buffers_to_WWTP: input_rows=%s wwtp_path=%s wwtp_buffer=%s zoom_level=%s max_workers=%s is_parallel=%s",
        len(gdf),
        wwtp_filepath,
        wwtp_buffer,
        zoom_level,
        max_workers,
        is_parallel,
    )

    wwtp_gdf = gpd.read_file(wwtp_filepath)
    logger.info(
        "WWTP read complete: rows=%s cols=%s crs=%s",
        len(wwtp_gdf),
        len(wwtp_gdf.columns),
        wwtp_gdf.crs,
    )

    if 'geometry' not in wwtp_gdf.columns:
        logger.error("WWTP file has no geometry column: %s", wwtp_filepath)
        raise ValueError(f"WWTP file has no geometry column: {wwtp_filepath}")
    if wwtp_country_col not in wwtp_gdf.columns:
        logger.error("WWTP file is missing configured country column '%s': %s", wwtp_country_col, wwtp_filepath)
        raise KeyError(f"WWTP file is missing configured country column '{wwtp_country_col}': {wwtp_filepath}")

    wwtp_geom = wwtp_gdf['geometry']
    wwtp_null_count = int(wwtp_geom.isna().sum())
    wwtp_invalid_count = int((~wwtp_geom.is_valid.fillna(False)).sum())
    countries_before = sorted(wwtp_gdf[wwtp_country_col].dropna().astype(str).str.strip().unique().tolist())
    logger.info(
        "WWTP geometry quality before filtering: null=%s invalid=%s",
        wwtp_null_count,
        wwtp_invalid_count,
    )
    logger.info(
        "WWTP countries before filtering (%s): %s",
        len(countries_before),
        countries_before,
    )
    before_filter_rows = len(wwtp_gdf)
    wwtp_gdf = wwtp_gdf[(wwtp_gdf['geometry'].notna()) 
                        & (wwtp_gdf['geometry'].is_valid)]
    dropped_rows = before_filter_rows - len(wwtp_gdf)
    countries_after = sorted(wwtp_gdf[wwtp_country_col].dropna().astype(str).str.strip().unique().tolist())
    logger.info(
        "WWTP filtering complete: kept_rows=%s dropped_rows=%s",
        len(wwtp_gdf),
        dropped_rows,
    )
    logger.info(
        "WWTP countries after filtering (%s): %s",
        len(countries_after),
        countries_after,
    )
    if wwtp_gdf.empty:
        logger.warning("All WWTP geometries were filtered out before buffering.")
    
    wwtp_gdf = add_bbox(wwtp_gdf, zoom_level=zoom_level, max_workers=max_workers, is_parallel=is_parallel)
    if not ('tiles' in wwtp_gdf.columns and not wwtp_gdf.empty):
        logger.warning("WWTP tile assignment produced no rows or missing tiles column.")

    wwtp_gdf = wwtp_gdf.to_crs(3857)
    logger.info("WWTP CRS transformed to EPSG:3857")
    wwtp_gdf['geometry'] = wwtp_gdf.geometry.buffer(wwtp_buffer)
    wwtp_buffer_null_count = int(wwtp_gdf['geometry'].isna().sum())
    wwtp_buffer_empty_count = int(wwtp_gdf['geometry'].is_empty.fillna(False).sum())
    logger.info(
        "WWTP buffering complete: rows=%s null_after_buffer=%s empty_after_buffer=%s",
        len(wwtp_gdf),
        wwtp_buffer_null_count,
        wwtp_buffer_empty_count,
    )
    wwtp_gdf['geometry'] = wwtp_gdf['geometry'].map(to_wkt)
    logger.info("WWTP geometry converted to WKT for DuckDB join")
    logger.info("Input non-served frame before WWTP join prep: rows=%s crs=%s", len(gdf), gdf.crs)

    gdf = add_bbox(gdf, zoom_level=zoom_level, max_workers=max_workers, is_parallel=is_parallel)
    gdf = gdf.to_crs(3857)
    logger.info("Input non-served frame CRS transformed to EPSG:3857")
    gdf['geometry'] = gdf.geometry.buffer(wwtp_buffer).map(to_wkt)
    logger.info("Input non-served geometries buffered and converted to WKT for DuckDB join")

    country_col_quoted = wwtp_country_col.replace('"', '""')
    
    query = f"""
    WITH wwtp AS (
        SELECT
            row_number() OVER () AS wwtp_idx,
            * REPLACE (ST_GeomFromText(geometry) AS geometry),
            CAST("{country_col_quoted}" AS VARCHAR) AS "{country_col_quoted}",
            UNNEST(tiles) AS tile
        FROM wwtp_gdf
    ),
    data_gdf AS (
        SELECT
            row_number() OVER () AS data_idx,
            * REPLACE (ST_GeomFromText(geometry) AS geometry)
        FROM gdf
    ),
    data_tiles AS (
        SELECT *, UNNEST(tiles) AS tile
        FROM data_gdf
    ),
    matches AS (
        SELECT DISTINCT
            a.data_idx,
            b."{country_col_quoted}" AS "{country_col_quoted}"
        FROM data_tiles a
        JOIN wwtp b
        ON CAST(a.tile AS VARCHAR) = CAST(b.tile AS VARCHAR)
        AND ST_Intersects(a.geometry, b.geometry)
    )
    SELECT d.* REPLACE (ST_AsText(d.geometry) AS geometry), m."{country_col_quoted}"
    FROM data_gdf d
    JOIN matches m
    ON CAST(d.data_idx AS BIGINT) = CAST(m.data_idx AS BIGINT)
    """
    try:
        # Shared helper owns the scratch DB: the previous fixed 'temp_duckdb.db'
        # was reused by every concurrent worker in this module.
        with duckdb_connection() as conn:
            ensure_duckdb_spatial(conn)
            gdf = conn.execute(query).df()
            logger.info(
                "DuckDB WWTP intersection complete: rows=%s cols=%s",
                len(gdf),
                len(gdf.columns),
            )
            if wwtp_country_col in gdf.columns:
                matched_countries = sorted(gdf[wwtp_country_col].dropna().astype(str).str.strip().unique().tolist())
                logger.info(
                    "WWTP countries matched by query (%s): %s",
                    len(matched_countries),
                    matched_countries,
                )
                gdf = gdf.drop(columns=[wwtp_country_col])
            if gdf.empty:
                logger.warning("DuckDB WWTP intersection returned zero rows.")
            gdf = gpd.GeoDataFrame(gdf, geometry=gdf['geometry'].map(from_wkt), crs=3857).to_crs(4326)
            logger.info("WWTP output GeoDataFrame prepared: rows=%s crs=%s", len(gdf), gdf.crs)
            return gdf
    except Exception as e:
        logger.error("Error during buffer addition: %s", e)
        raise Exception("Error during buffer addition") from e
    
def orchestrate_urban_intersection(unserved_gdf, urban_filepath, zoom_level=8, max_workers=32, is_parallel=True,
                                   urban_buffer=10000, tolerance=0.0001):
    if int(max_workers) < 1:
        raise ValueError("max_workers must be >= 1")

    if unserved_gdf.empty:
        return unserved_gdf

    unserved_gdf['idx'] = range(len(unserved_gdf))

    unserved_gdf = add_bbox(unserved_gdf, zoom_level=zoom_level, max_workers=max_workers, is_parallel=is_parallel)
    urban_areas_gdf = read_urban_areas(urban_filepath)
    urban_areas_gdf = urban_areas_gdf.to_crs(3857)
    urban_areas_gdf['geometry'] = urban_areas_gdf.geometry.buffer(urban_buffer)
    urban_areas_gdf = urban_areas_gdf.to_crs(4326)
    urban_areas_gdf = add_bbox(urban_areas_gdf, zoom_level=zoom_level, max_workers=max_workers, is_parallel=False)

    urban_areas_gdf = urban_areas_gdf.to_crs(3857)
    urban_areas_gdf['geometry'] = urban_areas_gdf['geometry'].map(to_wkt)
    unserved_gdf = unserved_gdf.to_crs(3857)
    unserved_gdf['geometry'] = unserved_gdf['geometry'].map(to_wkt)

    duckdb_query = f"""
        WITH urban_areas AS (
            SELECT *
            FROM urban_areas_gdf
        ),
        unserved AS (
            SELECT *, UNNEST(tiles) AS tile
            FROM unserved_gdf
        ),
        clipped AS (
            SELECT
                a.idx,
                ST_Intersection(
                    ST_GeomFromText(a.geometry),
                    ST_GeomFromText(b.geometry)
                ) AS geom
            FROM unserved a
            INNER JOIN urban_areas b
                ON list_contains(b.tiles, a.tile)
               AND ST_Intersects(
                    ST_GeomFromText(a.geometry),
                    ST_GeomFromText(b.geometry)
               )
        ),
        random_tile AS (
            SELECT idx, tile
            FROM (
                SELECT
                    idx,
                    tile,
                    ROW_NUMBER() OVER (PARTITION BY idx ORDER BY RANDOM()) AS row_num
                FROM unserved
            )
            WHERE row_num = 1
        ),
        aggregated AS (
            SELECT
                idx,
                ST_AsText(
                    ST_SimplifyPreserveTopology(
                        ST_Union_Agg(geom),
                        {tolerance}
                    )
                ) AS geometry
            FROM clipped
            GROUP BY idx
        )
        SELECT u.* REPLACE (a.geometry AS geometry), t.tile
        FROM unserved_gdf u
        INNER JOIN aggregated a
            ON u.idx = a.idx
        INNER JOIN random_tile t
            ON u.idx = t.idx
    """

    try:
        with duckdb_connection() as conn:
            ensure_duckdb_spatial(conn)
            df = conn.execute(duckdb_query).df()
            if df.empty:
                empty_columns = [col for col in unserved_gdf.columns if col not in {'idx', 'tiles'}] + ['tile']
                return gpd.GeoDataFrame(columns=empty_columns, geometry='geometry', crs=4326)
            df = df.sort_values('idx').drop(columns=['tiles', 'idx'])
            df['geometry'] = df['geometry'].map(from_wkt)
            return gpd.GeoDataFrame(df, geometry='geometry', crs=3857).to_crs(4326)
    except Exception as e:
        logger.error("Error during urban intersection: %s", e)
        raise Exception("Error during urban intersection") from e

def parse_args():
    """Parse optional config override flags for this step."""
    parser = argparse.ArgumentParser(
        description="Filter non-served polygons above threshold and export to GPKG."
    )
    add_standard_override_arguments(parser)
    return parser.parse_args()

def create_unserved_pop(filepath, threshold, output_filepath,
                        urban_filepath, wwtp_filepath,
                        max_workers=32,
                        is_parallel=True,
                        zoom_level=8,
                        urban_buffer=10000,
                        wwtp_buffer=10000,
                        wwtp_country_col='ISO_2',
                        tolerance=0.0001):
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

    try:
        threshold_value = int(threshold)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"threshold must be an integer-like value, got: {threshold}") from exc
    if threshold_value < 0:
        raise ValueError("threshold must be >= 0")

    logger.info("Filtering non-served polygons from %s with threshold=%s", filepath, threshold_value)

    query = f"""
    SELECT *
    FROM read_csv('{filepath}',
        header=True,
        all_varchar=True,
        max_line_size=10000000
        )
    WHERE TRY_CAST(pop_sum AS BIGINT) > {threshold_value}
    """
    # duckdb_connection owns the scratch database: unique name, always removed.
    with duckdb_connection() as conn:
        df = conn.sql(query).df()

    if df.empty:
        logger.warning("No rows matched threshold=%s. Writing empty output file.", threshold)
    if "geometry" not in df.columns:
        raise ValueError("Input CSV is missing required 'geometry' column")

    df['geometry'] = df.geometry.apply(from_wkt)
    df = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)
    df = orchestrate_urban_intersection(
        df,
        urban_filepath,
        zoom_level=zoom_level,
        max_workers=max_workers,
        is_parallel=is_parallel,
        urban_buffer=urban_buffer,
        tolerance=tolerance
    )
    df = add_buffers_to_WWTP(df, 
                            wwtp_filepath=wwtp_filepath,
                            wwtp_buffer=wwtp_buffer,
                            zoom_level=zoom_level,
                            max_workers=max_workers,
                            is_parallel=is_parallel,
                            wwtp_country_col=wwtp_country_col)

    ensure_output_dir_for_file(output_filepath)
    df.to_file(output_filepath, driver='GPKG', index=False)
    logger.info("Wrote %s polygons to %s", len(df), output_filepath)
    return len(df)

def main():
    """Load config and run threshold-based non-served population extraction."""
    args = parse_args()
    overrides = parse_config_overrides(args=args)
    cfg = load_config(script_name="find_unserved_pop", **overrides)

    threshold = int(cfg["threshold_value"])
    max_workers = int(cfg["max_workers"])
    is_parallel = str(cfg["is_parallel"]).strip().lower() in {"1", "true", "yes", "y", "on"}
    zoom_level = int(cfg["zoom_level"])
    urban_buffer = int(cfg["urban_buffer"])
    tolerance = float(cfg["tolerance"])
    wwtp_buffer = int(cfg["wwtp_buffer"])
    wwtp_country_col = cfg["country_output_column"]
    wwtp_filepath = os.path.abspath(cfg['paths']['annotated_all_filepath'])
    
    non_served_outpath = os.path.abspath(cfg["paths"]["non_served_outpath"].replace('.gpkg', '.csv'))
    non_served_above_threshold_outpath = os.path.abspath(cfg["paths"]["non_served_above_threshold_outpath"])
    urban_filepath = os.path.abspath(cfg["paths"]["urban_areas_filepath"])
    rows_written = create_unserved_pop(
        non_served_outpath,
        threshold,
        non_served_above_threshold_outpath,
        urban_filepath,
        wwtp_filepath, 
        max_workers=max_workers,
        is_parallel=is_parallel,
        zoom_level=zoom_level,
        urban_buffer=urban_buffer,
        wwtp_buffer = wwtp_buffer,
        wwtp_country_col=wwtp_country_col,
        tolerance=tolerance
    )
    logger.info("Completed non-served population extraction. rows_written=%s", rows_written)

if __name__ == "__main__":
    configure_logging()
    main()