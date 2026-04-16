"""Attach nearest river-system intersection metadata to non-served polygons.

Workflow:
1. Spatially match polygons to nearby rivers within a search distance.
2. Assign each polygon a river-system ID (`MAIN_RIV`).
3. Compute the common downstream juncture (`NXT_DIS`) from matched rivers.
4. Write enriched polygons to output.
"""

import os
import sys
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely import box
from tqdm import tqdm

from ..starter import load_config
from ..create_voronoi import estimate_utm_epsg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_graph(df):
    """Build HYRIV_ID -> NEXT_DOWN adjacency map for downstream traversal."""
    return dict(zip(df['HYRIV_ID'], df['NEXT_DOWN']))

def find_intersection_id(id1, id2, graph):
    """Find first common downstream node between two river IDs."""
    visited = set()
    while id1:
        visited.add(id1)
        id1 = graph.get(id1)
    while id2:
        if id2 in visited:
            return id2
        id2 = graph.get(id2)
    return None

def find_common_intersection(ids, graph):
    """Find common downstream intersection for a list of river IDs."""
    if not ids:
        return None
    current = ids[0]
    for id_ in ids[1:]:
        current = find_intersection_id(current, id_, graph)
        if current is None:
            return None
    return current

def optimize_river_lookup(polygons_gdf, rivers_gdf, x_distance, utm_epsg):
    """Match polygons to nearby rivers (same HYBAS_ID) in one UTM zone."""
    # Project rivers once
    rivers_gdf = rivers_gdf.to_crs(utm_epsg)
    polygons_gdf = polygons_gdf.to_crs(utm_epsg)

    poly_temp = polygons_gdf.copy()
    poly_temp['geometry'] = polygons_gdf.geometry.buffer(x_distance)
    
    joined = gpd.sjoin(
        rivers_gdf[['geometry', 'HYBAS_ID', 'HYRIV_ID']],
        poly_temp[['geometry', 'HYBAS_ID']],
        how='inner',
        predicate='intersects'
    )
    matched = joined[joined['HYBAS_ID_left'] == joined['HYBAS_ID_right']]
    river_lists = matched.groupby('index_right')['HYRIV_ID'].apply(list)
    polygons_gdf['river_list'] = river_lists.reindex(polygons_gdf.index)
    polygons_gdf['river_list'] = polygons_gdf['river_list'].apply(
        lambda x: x if isinstance(x, list) else []
    )

    return polygons_gdf


def orchestrate_settlement_river_intersections(polygons_gdf, rivers_gdf, x_distance, max_workers=4):
    """Run spatial river lookup by UTM zone in parallel."""
    if 'utm' not in polygons_gdf.columns:
        logger.info("Estimating UTM zones")
        polygons_gdf['utm'] = polygons_gdf.geometry.centroid.apply(
            lambda geom: estimate_utm_epsg(geom.x, geom.y)
        )

    gdfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        # Wrap UTM zones in tqdm for progress bar
        for utm in tqdm(polygons_gdf['utm'].unique(), desc="Processing UTM zones"):
            poly_subset = polygons_gdf[polygons_gdf['utm'] == utm].copy()
            bbox_geom = box(*poly_subset.total_bounds)

            possible_idx = list(rivers_gdf.sindex.intersection(bbox_geom.bounds))
            river_subset = rivers_gdf.iloc[possible_idx]

            future = executor.submit(
                optimize_river_lookup,
                poly_subset,
                river_subset,
                x_distance,
                utm
            )
            futures[future] = utm

        for future in as_completed(futures):
            utm_zone = futures[future]
            try:
                result = future.result()
                if result is not None and not result.empty:
                    gdfs.append(result.to_crs(4326))
            except Exception as err:
                logger.exception("Worker failed for UTM %s: %s", utm_zone, err)

    if gdfs:
        logger.info("Spatial matching complete for %s UTM chunks", len(gdfs))
        return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=4326)

    logger.warning("No river matches were found for any UTM chunk")
    empty = polygons_gdf.copy()
    empty['river_list'] = [[] for _ in range(len(empty))]
    return empty

def assign_main_riv(polygon_gdf, rivers_gdf):
    """Assign MAIN_RIV for each polygon using the first matched river ID."""
    # 1. Create the hash map (O(N) to build, O(1) to look up)
    # Using zip is the most memory-efficient way to build this in Python
    my_dict = dict(zip(rivers_gdf['HYRIV_ID'], rivers_gdf['MAIN_RIV']))
    
    # 2. Use .apply with a lambda
    # We use .get(l[0]) to handle cases where the ID might be missing gracefully
    polygon_gdf['MAIN_RIV'] = polygon_gdf['river_list'].apply(
        lambda l: my_dict.get(l[0]) if (isinstance(l, list) and len(l) > 0) else None
    )
    return polygon_gdf

def assign_river_juncture(polygons_batch, rivers_batch):
    """Assign downstream junction ID (NXT_DIS) for one polygon batch."""
    if polygons_batch.empty or rivers_batch.empty:
        polygons_batch['NXT_DIS'] = None
        return polygons_batch

    graph = build_graph(rivers_batch)

    polygons_batch['NXT_DIS'] = polygons_batch['river_list'].apply(
        lambda rivs: find_common_intersection(rivs, graph) if rivs else None
    )

    return polygons_batch


def orchestrate_river_assignment(polygons_gdf, rivers_gdf, max_workers=8):
    """Compute river-juncture assignment grouped by MAIN_RIV in parallel."""
    work_todo = polygons_gdf[polygons_gdf['river_list'].map(len) > 0].copy()
    work_done_empty = polygons_gdf[polygons_gdf['river_list'].map(len) == 0].copy()
    work_done_empty['NXT_DIS'] = None

    gdfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        # Wrap MAIN_RIV groups in tqdm for progress bar
        for riv, group in tqdm(work_todo.groupby('MAIN_RIV'), desc="Processing MAIN_RIV groups"):
            river_subset = rivers_gdf[rivers_gdf['MAIN_RIV'] == riv].copy()
            future = executor.submit(assign_river_juncture, group.copy(), river_subset)
            futures[future] = riv

        for future in as_completed(futures):
            riv_id = futures[future]
            try:
                result = future.result()
                if result is not None:
                    gdfs.append(result)
            except Exception as err:
                logger.exception("Error processing River System %s: %s", riv_id, err)

    if gdfs:
        final = pd.concat(gdfs + [work_done_empty], ignore_index=True)
        logger.info("Assigned NXT_DIS for %s polygons", len(final))
        return gpd.GeoDataFrame(final, crs=polygons_gdf.crs, geometry='geometry')

    return work_done_empty

def main():
    """Load inputs, perform river matching + juncture assignment, and save output."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()

    polygons_path = os.path.abspath(cfg['paths']['non_served_above_threshold_outpath'])
    rivers_path = os.path.abspath(cfg['paths']['rivershed_output_path'])
    output_path = os.path.abspath(cfg['paths']['non_served_nxt_river_outpath'])
    x_distance = 5000  # meters
    #max_workers = int(cfg['params'].get('max_workers', 8))
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 32

    logger.info("Loading data")
    polygons_gdf = gpd.read_file(polygons_path)
    rivers_gdf = gpd.read_file(
        rivers_path,
        columns=['HYRIV_ID', 'NEXT_DOWN', 'MAIN_RIV', 'HYBAS_ID', 'geometry']
    )
    logger.info("Loaded %s polygons and %s river segments", len(polygons_gdf), len(rivers_gdf))

    # Ensure both are in EPSG:4326
    if polygons_gdf.crs is None:
        raise ValueError("polygons_gdf has no CRS defined.")
    if rivers_gdf.crs is None:
        raise ValueError("rivers_gdf has no CRS defined.")
    
    polygons_gdf = polygons_gdf.to_crs(4326)
    rivers_gdf = rivers_gdf.to_crs(4326)
    polygons_gdf['HYBAS_ID'] = polygons_gdf['HYBAS_ID'].astype(np.int64)
    rivers_gdf['HYBAS_ID'] = rivers_gdf['HYBAS_ID'].astype(np.int64)

    logger.info("Running spatial river matching")
    polygons_gdf = orchestrate_settlement_river_intersections(
        polygons_gdf, rivers_gdf, x_distance, max_workers=max_workers
    )
    
    logger.info("Adding MAIN_RIV to polygons")
    polygons_gdf = assign_main_riv(polygons_gdf, rivers_gdf)

    logger.info("Assigning river junctures")
    polygons_gdf = orchestrate_river_assignment(
        polygons_gdf, rivers_gdf, max_workers=max_workers
    )

    logger.info("Writing output to %s", output_path)
    polygons_gdf.to_file(output_path, driver='GPKG', index=False)
    logger.info("Done")


if __name__ == '__main__':
    main()