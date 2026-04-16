"""Compute watershed minus served-population differences and annotate raster stats.

This script selects one population GeoPackage by index, computes geometric
differences against watersheds per UTM group, intersects the result with TIFF
data, and writes a diff GeoPackage.
"""

import os
import logging
import random
import argparse
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import from_wkt, to_wkt
from concurrent.futures import ProcessPoolExecutor, as_completed
from ..starter import load_config
from ..create_voronoi import estimate_utm_epsg
from ..add_pop import intersect_all_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def find_difference(watershed_gdf, pop_gdf):
    """Compute geometric difference watershed - population geometry by HYBAS_ID.

    Parameters
    ----------
    watershed_gdf : geopandas.GeoDataFrame
        Watershed polygons for a selected EPSG subset.
    pop_gdf : geopandas.GeoDataFrame
        Population polygons for the same EPSG subset.

    Returns
    -------
    pandas.DataFrame | None
        Difference dataframe with WKT converted back to shapely geometry.
    """
    watershed_local = watershed_gdf.copy()
    pop_local = pop_gdf.copy()

    watershed_local["geometry"] = watershed_local["geometry"].map(to_wkt)
    pop_local["geometry"] = pop_local["geometry"].map(to_wkt)
    temp_file = f'temp_{str(int(random.randint(0, int(1e12))))}.db'
    conn = None
    try:
        conn = duckdb.connect(temp_file)
        conn.execute('INSTALL SPATIAL; LOAD SPATIAL;')
        conn.register("watershed_gdf", watershed_local)
        conn.register("pop_gdf", pop_local)

        query = f"""
        SELECT a.*,
        ST_AsText(ST_Difference(ST_GEOMFROMTEXT(a.geometry), ST_GEOMFROMTEXT(b.geometry))) as geometry
        FROM watershed_gdf AS a
        LEFT JOIN pop_gdf AS b
        ON a.HYBAS_ID = b.HYBAS_ID
        WHERE b.HYBAS_ID IS NOT NULL
        """
        df = conn.execute(query).df()
        df = df[df["geometry"].notna()].copy()
        df['geometry'] = df['geometry'].map(from_wkt)
        logger.info("Computed %s difference rows", len(df))
        return df
    except Exception as e:
        logger.exception("Error while computing differences: %s", e)
        return None
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_file):
            os.remove(temp_file)

def process_epsg_group(epsg, watershed_gdf, pop_gdf):
    """Process one EPSG bucket and return differences in EPSG:4326."""
    subset_pop_gdf = pop_gdf[pop_gdf['epsg'] == epsg]
    subset_watershed_gdf = watershed_gdf[watershed_gdf['HYBAS_ID'].isin(subset_pop_gdf['HYBAS_ID'].unique())]

    if subset_pop_gdf.empty or subset_watershed_gdf.empty:
        logger.info("EPSG %s skipped because one subset is empty", epsg)
        return None

    subset_pop_gdf = subset_pop_gdf.to_crs(epsg)
    subset_watershed_gdf = subset_watershed_gdf.to_crs(epsg)
    diff_gdf = find_difference(subset_watershed_gdf, subset_pop_gdf)
    if diff_gdf is not None:
        diff_gdf = gpd.GeoDataFrame(diff_gdf, geometry='geometry', crs=epsg).to_crs(4326)
    return diff_gdf

def find_differences(watershed_gdf, pop_gdf, max_workers=None, is_parallel=True):
    """Compute all differences by grouping inputs in local UTM EPSG zones."""
    pop_local = pop_gdf.copy()
    pop_local['epsg'] = pop_local['geometry'].apply(
        lambda geom: estimate_utm_epsg(geom.centroid.x, geom.centroid.y)
    )
    gdf_list = []

    if is_parallel:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_epsg_group, epsg, watershed_gdf, pop_local): epsg
                for epsg in pop_local['epsg'].unique()
            }
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    gdf_list.append(result)
    else:
        for epsg in pop_local['epsg'].unique():
            result = process_epsg_group(epsg, watershed_gdf, pop_local)
            if result is not None:
                gdf_list.append(result)
    
    if gdf_list:
        gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), geometry='geometry', crs=4326)
        logger.info("Total difference features: %s", len(gdf))
        return gdf
    return gpd.GeoDataFrame(columns=watershed_gdf.columns, geometry='geometry', crs=4326)

def parse_bool(value):
    """Convert common textual boolean values to bool."""
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")

def parse_args():
    """Parse CLI args for input index and parallel execution mode."""
    parser = argparse.ArgumentParser(
        description="Compute population difference polygons for one input file index."
    )
    parser.add_argument("index", type=int, help="0-based file index from filtered pop output files")
    parser.add_argument(
        "is_parallel",
        nargs="?",
        default="true",
        help="Whether to process EPSG groups in parallel (true/false)",
    )
    args = parser.parse_args()
    args.is_parallel = parse_bool(args.is_parallel)
    return args
        
def main():
    """Load config, select one population file, compute differences, and save output."""
    args = parse_args()
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()

    watershed_filepath = os.path.abspath(cfg['paths']['hydrowaste'])
    max_workers = cfg['params']['max_workers']
    pop_output_dir = os.path.abspath(cfg['paths']['pop_output_dir'])
    tif_dir = os.path.abspath(cfg['paths']['pop_tif_dir'])
    pop_dif_output_dir = os.path.abspath(cfg['paths']['pop_dif_output_dir'])

    filenames = sorted([
        x for x in os.listdir(pop_output_dir)
        if (('appr_1' in x) or ('appr_3' in x)) and x.endswith('.gpkg')
    ])
    if not filenames:
        raise FileNotFoundError(f"No matching input .gpkg files found in {pop_output_dir}")
    if args.index < 0 or args.index >= len(filenames):
        raise IndexError(f"index must be in [0, {len(filenames) - 1}], got {args.index}")

    filename = filenames[args.index]
    logger.info("Selected input file %s (%s/%s)", filename, args.index, len(filenames))

    pop_gdf = gpd.read_file(os.path.join(pop_output_dir, filename))
    watershed_gdf = gpd.read_file(watershed_filepath)
    logger.info("Loaded %s population features and %s watershed features", len(pop_gdf), len(watershed_gdf))

    diff_gdf = find_differences(watershed_gdf, pop_gdf, max_workers=max_workers, is_parallel=args.is_parallel)
    diff_gdf = intersect_all_files(diff_gdf, tif_dir, max_workers=max_workers)
    logger.info("Post-intersection features: %s", len(diff_gdf))
    
    if not os.path.exists(pop_dif_output_dir):
        os.makedirs(pop_dif_output_dir, exist_ok=True)

    output_filepath = os.path.join(pop_dif_output_dir, f'diff_{filename}')
    diff_gdf.to_file(output_filepath, driver='GPKG', index=False)
    logger.info("Wrote output to %s", output_filepath)

if __name__ == '__main__':
    main()














