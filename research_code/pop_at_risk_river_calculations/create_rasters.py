"""Create signed population rasters and extract unserved-population islands by basin.

This script combines:
1. Raster sign assignment (+ inside WWTP Voronoi polygons, - outside), and
2. Island extraction/statistics for non-served populated areas by watershed basin.

It is intended for country-wise batch processing of WorldPop rasters.
"""

import os
import random
import logging
import gc
import argparse
from tqdm import tqdm

from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import pandas as pd
import geopandas as gpd

import rasterio
from rasterio import windows
from rasterio.features import shapes, geometry_mask, rasterize
from exactextract import exact_extract
from scipy.ndimage import label
from shapely.geometry import shape, box
from shapely import to_wkt
from shapely.ops import unary_union

try:
    from ..add_pop import get_iso_codes
    from ..starter import load_config
    from ..create_voronoi import download_overture_maps, duckdb_intersect
    from ..pipelines import create_pop_output_paths
    from .find_pop_in_danger_pop import find_bbox, finding_tiles
except ImportError:
    from research_code.add_pop import get_iso_codes
    from research_code.starter import load_config
    from research_code.create_voronoi import download_overture_maps, duckdb_intersect
    from research_code.pipelines import create_pop_output_paths
    from research_code.pop_at_risk_river_calculations.find_pop_in_danger_pop import find_bbox, finding_tiles

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def geotiff_exists_and_valid(path):
    """Return True when a GeoTIFF exists and raster metadata can be read."""
    if not os.path.exists(path):
        return False
    try:
        with rasterio.open(path) as src:
            _ = src.count        # forces metadata read
        return True
    except Exception:
        return False
    
""" def extract_worldpop_optimized_v2(raster_path, exclude_gdf):
    temp_polygons = []
    temp_sums = []

    with rasterio.open(raster_path) as src:
        nodata = src.nodata
        crs = src.crs
        transform = src.transform
        
        for _, window in src.block_windows(1):
            # 1. Read and Clean (Requirement 1 & 2)
            data = src.read(1, window=window)
            if nodata is not None:
                data[np.isclose(data, nodata)] = 0
            data = np.nan_to_num(data, nan=0)
            data[data < 0] = 0
            data = data.astype(np.int64)

            # 2. Masking (Requirement 3)
            window_transform = rasterio.windows.transform(window, transform)
            burn_mask = geometry_mask(
                exclude_gdf.geometry,
                out_shape=(window.height, window.width),
                transform=window_transform,
                invert=True
            )
            data[burn_mask] = 0
            
            # 3. Polygonize and Sum (Requirement 5)
            # We use an internal labeling step ONLY for the current window 
            # to sum the population before creating the polygon.
            pop_mask = (data > 0)
            if not np.any(pop_mask):
                continue

            # This generates polygons where each 'value' is the population of that cell
            # But we want the sum of the whole connected region in this block.
            labeled_array, num_features = label(pop_mask)
            
            for i in range(1, num_features + 1):
                feature_mask = (labeled_array == i)
                # Calculate sum for this specific cluster in this block
                cluster_sum = np.sum(data[feature_mask])
                
                # Convert this specific cluster to a polygon
                # We use a dummy constant (1) for 'shapes' because we already have the mask
                shape_gen = shapes(feature_mask.astype(np.uint8), mask=feature_mask, transform=window_transform)
                for geom, _ in shape_gen:
                    temp_polygons.append(shape(geom))
                    temp_sums.append(cluster_sum)

    if not temp_polygons:
        return gpd.GeoDataFrame(columns=['geometry', 'pop_sum'], crs=crs)

    # 4. Dissolve and Aggregate
    # This is the "Big Data" step. We dissolve geometries and SUM their pop_sum tags.
    gdf = gpd.GeoDataFrame({'pop_sum': temp_sums, 'geometry': temp_polygons}, crs=crs)
    
    # We use 'cluster' logic to group touching polygons
    # spatial_index makes this fast even for Russia
    sindex = gdf.sindex
    # Grouping polygons that touch
    gdf['group'] = -1
    group_id = 0
    
    # This is a high-speed way to find which block-fragments belong together
    for i in range(len(gdf)):
        if gdf.iloc[i]['group'] == -1:
            possible_matches = list(sindex.intersection(gdf.iloc[i].geometry.bounds))
            precise_matches = gdf.iloc[possible_matches][gdf.iloc[possible_matches].intersects(gdf.iloc[i].geometry)]
            
            # Assign all connected pieces the same group ID
            existing_groups = precise_matches['group'][precise_matches['group'] != -1]
            if not existing_groups.empty:
                this_group = existing_groups.iloc[0]
            else:
                this_group = group_id
                group_id += 1
            
            gdf.loc[precise_matches.index, 'group'] = this_group

    # Final Dissolve: Merge geometries and SUM the population
    final_gdf = gdf.dissolve(by='group', aggfunc={'pop_sum': 'sum'})
    return final_gdf.reset_index(drop=True) """

def extract_worldpop_universal(raster_path, hybas_gdf, exclude_gdf, min_pixels=9, zoom_level=8):
    """
    Extracts population islands from WorldPop rasters with strict RAM management.
    Designed for 64GB / 4 processes (16GB per worker).

    Parameters
    ----------
    raster_path : str
        Input population raster path.
    hybas_gdf : geopandas.GeoDataFrame
        Watershed polygons that provide basin IDs and metadata.
    exclude_gdf : geopandas.GeoDataFrame
        Polygons to exclude (served areas).
    min_pixels : int, default=9
        Minimum island size in pixels, unless touching window boundaries.

    Returns
    -------
    geopandas.GeoDataFrame | None
        Extracted islands with basin metadata and zonal statistics.
    """
    geom_registry = {}
    country_code = os.path.basename(raster_path)
    MERGE_THRESHOLD = 200  # Merge shards into the registry every 200 items

    try:
        with rasterio.open(raster_path) as src:
            crs = src.crs
            res = src.res[0]
            transform = src.transform

            logger.info("[%s] Aligning CRS and building spatial indices", country_code)
            hybas_gdf = hybas_gdf.to_crs(crs)
            exclude_gdf = exclude_gdf.to_crs(crs)

            # Pre-trigger spatial index creation
            _ = hybas_gdf.sindex
            _ = exclude_gdf.sindex

            # Iterate windows
            for i, (index, window) in enumerate(src.block_windows(1)):
                if i % 250 == 0:
                    logger.info("[%s] Processing window %s", country_code, i)

                w_bounds = windows.bounds(window, transform)
                w_transform = windows.transform(window, transform)
                w_poly_boundary = box(*w_bounds).boundary

                # Spatial intersection check
                possible_idx = list(hybas_gdf.sindex.intersection(w_bounds))
                if not possible_idx:
                    continue

                # 1. READ AND BINARIZE IN-PLACE (Save RAM)
                binary_data = src.read(1, window=window)
                np.nan_to_num(binary_data, copy=False)
                binary_data[binary_data <= 0] = 0
                binary_data[binary_data > 0] = 1
                binary_data = binary_data.astype(np.uint8, copy=False)

                # 2. EXCLUSION MASKING
                excl_idx = list(exclude_gdf.sindex.intersection(w_bounds))
                if excl_idx:
                    rel_excl = exclude_gdf.iloc[excl_idx]
                    excl_mask = geometry_mask(
                        rel_excl.geometry,
                        (window.height, window.width),
                        w_transform,
                        invert=True
                    )
                    binary_data[excl_mask] = 0
                    del excl_mask, rel_excl

                # 3. BASIN PROCESSING
                for idx in possible_idx:
                    row = hybas_gdf.iloc[idx]
                    h_id = row.get('HYBAS_ID')

                    # Create basin mask
                    h_mask = geometry_mask(
                        [row.geometry],
                        (window.height, window.width),
                        w_transform,
                        invert=False
                    )

                    # Combine masks: population pixels inside basin
                    h_mask = (binary_data == 1) & (~h_mask)
                    if not h_mask.any():
                        continue

                    # Vectorize shards
                    shape_gen = shapes(
                        h_mask.astype(np.uint8, copy=False),
                        mask=h_mask,
                        transform=w_transform,
                        connectivity=8
                    )

                    window_shards = []
                    for geom, _ in shape_gen:
                        poly = shape(geom).buffer(0)
                        shard_pixels = round(poly.area / (res * res))
                        
                        # Filter noise unless it touches window boundary (potential split island)
                        if shard_pixels < min_pixels and not poly.intersects(w_poly_boundary):
                            continue
                        window_shards.append(poly)

                    if not window_shards:
                        continue

                    # Update Registry with Deferred Unioning
                    if h_id not in geom_registry:
                        geom_registry[h_id] = {
                            "geom_list": window_shards,
                            "meta": {k: row.get(k) for k in ["NEXT_DOWN", "NEXT_SINK", "MAIN_BAS"]}
                        }
                    else:
                        geom_registry[h_id]["geom_list"].extend(window_shards)
                        
                        # Merge if list is too long to prevent row-count bloat
                        if len(geom_registry[h_id]["geom_list"]) >= MERGE_THRESHOLD:
                            merged = unary_union(geom_registry[h_id]["geom_list"])
                            geom_registry[h_id]["geom_list"] = [merged]

                del binary_data
                gc.collect()

        # 4. FINAL MERGE AND EXPLODE
        logger.info("[%s] Loop complete. Exploding MultiPolygons", country_code)
        final_rows = []

        for h_id, content in geom_registry.items():
            # Final merge of all shards for this Basin ID
            merged = unary_union(content["geom_list"])
            geoms = getattr(merged, "geoms", None)
            islands = list(geoms) if geoms is not None else [merged]

            for island in islands:
                if not island.is_empty:
                    tiles = finding_tiles(island, zoom_level=zoom_level)  # Example zoom level for tile assignment
                    for tile in tiles:
                        final_rows.append({
                        "geometry": island.intersection(find_bbox(tile)),  
                        "HYBAS_ID": h_id,
                        "tile": tile,
                        **content["meta"]
                    })
                                        
        del geom_registry
        gc.collect()

        if not final_rows:
            return None

        final_gdf = gpd.GeoDataFrame(final_rows, crs=crs)

        del final_rows
        logger.info("[%s] Total islands to check: %s", country_code, len(final_gdf))

        # 5. CHUNKED ZONAL STATS (The OOM-Killer Prevention)
        sums, counts = [], []
        chunk_size = 100000

        for start_idx in range(0, len(final_gdf), chunk_size):
            end_idx = min(start_idx + chunk_size, len(final_gdf))
            logger.info("[%s] Calculating stats for chunk %s-%s", country_code, start_idx, end_idx)

            # exact_extract is faster and more memory-efficient than rasterstats
            chunk = final_gdf.iloc[start_idx:end_idx][["geometry"]]
            stats_df = exact_extract(
                rast=raster_path,
                vec=chunk,
                ops=["sum", "count"],
                output="pandas"
            )

            for _, r in stats_df.iterrows():
                sums.append(int(np.round(r["sum"] or 0)))
                counts.append(int(r["count"] or 0))

            del stats_df, chunk

            del stats
            gc.collect()

        final_gdf["pop_sum"] = sums
        final_gdf["pixel_count"] = counts
        del sums, counts

        # 6. FINAL CLEANUP
        logger.info("[%s] Filtering and resetting index", country_code)
        final_gdf = final_gdf[
            (final_gdf["pop_sum"] > 0) & 
            (final_gdf["pixel_count"] >= min_pixels)
        ].copy().reset_index(drop=True)

        final_gdf["pop_sum"] = final_gdf["pop_sum"].astype(np.int64)
        final_gdf["pixel_count"] = final_gdf["pixel_count"].astype(np.int64)

        logger.info("[%s] SUCCESS. Final islands: %s", country_code, len(final_gdf))
        return final_gdf

    except Exception as e:
        logger.exception("[%s] CRITICAL FAILURE: %s", country_code, str(e))
        return None
    
""" def extract_worldpop_universal(raster_path, hybas_gdf, exclude_gdf, min_pixels=9):
    geom_registry = {}
    country_code = os.path.basename(raster_path)
    
    try:
        with rasterio.open(raster_path) as src:
            crs = src.crs
            res = src.res[0]
            transform = src.transform
            
            logging.warning(f"[{country_code}] Aligning CRS...")
            hybas_gdf = hybas_gdf.to_crs(crs)
            exclude_gdf = exclude_gdf.to_crs(crs)
            
            # Using list(src.block_windows) consumes RAM, so we just iterate
            for i, (index, window) in enumerate(src.block_windows(1)):
                if i % 500 == 0:
                    logging.warning(f"[{country_code}] Window {i} processing...")

                w_bounds = rasterio.windows.bounds(window, transform)
                w_transform = rasterio.windows.transform(window, transform)
                w_poly_boundary = box(*w_bounds).boundary
                
                possible_idx = list(hybas_gdf.sindex.intersection(w_bounds))
                if not possible_idx: continue

                data = src.read(1, window=window)
                binary_data = (np.nan_to_num(data) > 0).astype(np.uint8)
                del data 

                rel_excl = exclude_gdf.iloc[list(exclude_gdf.sindex.intersection(w_bounds))]
                if not rel_excl.empty:
                    excl_mask = geometry_mask(rel_excl.geometry, (window.height, window.width), 
                                             w_transform, invert=True)
                    binary_data[excl_mask] = 0

                for idx in possible_idx:
                    row = hybas_gdf.iloc[idx]
                    h_id = row.get('HYBAS_ID')
                    
                    h_mask = geometry_mask([row.geometry], (window.height, window.width), 
                                           w_transform, invert=False)
                    
                    target_binary = binary_data.copy()
                    target_binary[h_mask] = 0
                    
                    if not np.any(target_binary > 0): continue

                    shape_gen = shapes(target_binary, mask=target_binary, 
                                       transform=w_transform, connectivity=8)

                    window_shards = []
                    for geom, _ in shape_gen:
                        poly = shape(geom)
                        shard_pixels = round(poly.area / (res * res))
                        if shard_pixels < min_pixels and not poly.intersects(w_poly_boundary):
                            continue
                        window_shards.append(poly.buffer(0))

                    if not window_shards: continue

                    if h_id not in geom_registry:
                        geom_registry[h_id] = {
                            'geom': unary_union(window_shards),
                            'meta': {k: row.get(k) for k in ['NEXT_DOWN', 'NEXT_SINK', 'MAIN_BAS']}
                        }
                    else:
                        geom_registry[h_id]['geom'] = unary_union([geom_registry[h_id]['geom']] + window_shards)

                del binary_data
                gc.collect()

        # STEP 6: Exploding (Memory-Sensitive)
        logging.warning(f"[{country_code}] Exploding {len(geom_registry)} Basins...")
        final_rows = []
        for h_id, content in geom_registry.items():
            merged = content['geom']
            islands = merged.geoms if hasattr(merged, 'geoms') else [merged]
            for island in islands:
                if not island.is_empty:
                    final_rows.append({'geometry': island, 'HYBAS_ID': h_id, **content['meta']})
        
        # KEY: Clear registry before making GDF
        del geom_registry
        gc.collect()
        
        final_gdf = gpd.GeoDataFrame(final_rows, crs=crs)
        del final_rows
        
        logging.warning(f"[{country_code}] Total islands: {len(final_gdf)}")

        # STEP 7: Chunked Zonal Stats
        sums, counts = [], []
        chunk_size = 100000 
        
        for start_idx in range(0, len(final_gdf), chunk_size):
            end_idx = min(start_idx + chunk_size, len(final_gdf))
            logging.warning(f"[{country_code}] Stats chunk: {start_idx}-{end_idx}")
            
            # Using gen_zonal_stats on just the geometry series is the lightest possible call
            stats = gen_zonal_stats(final_gdf.geometry.iloc[start_idx:end_idx], 
                                   raster_path, stats=["sum", "count"], nodata=0)
            
            for r in stats:
                sums.append(int(np.round(r['sum'] or 0)))
                counts.append(int(r['count'] or 0))
            
            del stats
            gc.collect()
        
        final_gdf['pop_sum'] = sums
        final_gdf['pixel_count'] = counts
        del sums, counts

        # STEP 8: Final Cleanup
        final_gdf = final_gdf[
            (final_gdf['pop_sum'] > 0) & (final_gdf['pixel_count'] >= min_pixels)
        ].copy().reset_index(drop=True)
        
        final_gdf['pop_sum'] = final_gdf['pop_sum'].astype(np.int64)
        final_gdf['pixel_count'] = final_gdf['pixel_count'].astype(np.int64)
        
        logging.warning(f"[{country_code}] SUCCESS. Saved {len(final_gdf)} islands.")
        return final_gdf

    except Exception as e:
        logging.warning(f"[{country_code}] FAILED: {str(e)}")
        return None """

def polygon_raster_sign_from_gdf(raster_path, polygons_gdf, output_path):
    """
    Optimized for continental-scale rasters (e.g., WorldPop Russia 100m).
    Uses spatial indexing to filter polygons per window.

    Parameters
    ----------
    raster_path : str
        Input population raster.
    polygons_gdf : geopandas.GeoDataFrame
        Served area polygons used to assign positive signs.
    output_path : str
        Output signed raster path.

    Returns
    -------
    tuple[str, int | None, int | None]
        Output path, positive sum, negative sum.
    """
    try:
        # 1. Build a spatial index for the GDF (CRITICAL for speed)
        sindex = polygons_gdf.sindex

        with rasterio.open(raster_path) as src:
            logger.info("Raster block shapes for %s: %s", raster_path, src.block_shapes)

            profile = src.profile.copy()
            transform = src.transform
            nodata_val = src.nodata

            profile.update(
                dtype="int32",
                count=1,
                nodata=None,
                compress="lzw",
                tiled=True,
                blockxsize=256, # Standardizing block sizes for better I/O
                blockysize=256
            )

            sum_positive = np.int64(0)
            sum_negative = np.int64(0)

            with rasterio.open(output_path, "w", **profile) as dst:
                for _, window in src.block_windows(1):
                    # Get geographic bounds of the current window
                    win_bounds = windows.bounds(window, transform)
                    
                    # 2. Filter polygons: Only those inside/touching this window
                    possible_idx = list(sindex.intersection(win_bounds))
                    
                    # Read the data block
                    data = src.read(1, window=window)
                    
                    # Convert nodata/NaN to 0 and cast to int32
                    if nodata_val is not None:
                        data = np.where(np.isclose(data, nodata_val), 0, data)
                    data = np.nan_to_num(data, nan=0).astype(np.int32)
                    
                    # Force strictly positive to avoid sign errors
                    data = np.abs(data)

                    # 3. Conditional Rasterization
                    if possible_idx:
                        # Extract the actual geometries for this window
                        window_shapes = [(geom, 1) for geom in polygons_gdf.iloc[possible_idx].geometry]
                        
                        mask = rasterize(
                            window_shapes,
                            out_shape=(window.height, window.width),
                            transform=windows.transform(window, transform),
                            fill=0,
                            dtype=np.uint8,
                            all_touched=True
                        )
                        
                        # Within polygons = positive, Outside = negative
                        signed = np.where(mask == 1, data, -data)
                    else:
                        # No polygons in this window? Everything is negative
                        signed = -data

                    # 4. Global Statistics (using int64 to prevent overflow)
                    sum_positive += signed[signed > 0].astype(np.int64).sum()
                    sum_negative += signed[signed < 0].astype(np.int64).sum()

                    dst.write(signed.astype(np.int32), 1, window=window)
        return output_path, int(sum_positive), int(sum_negative)

    except Exception as err:
        logger.exception("Error processing raster %s: %s", raster_path, err)
        return output_path, None, None

def find_the_newest_tif_files(countries, tif_dir):
    """Map ISO-2 country codes to the newest available TIFF file in country subfolders."""
    alpha_3_to_2, alpha_2_to_3, alpha_3_to_names, alpha_2_to_names = get_iso_codes()
    tif_filepaths = {}
    my_dict = {}
    for iso_2 in countries:
        if iso_2 in alpha_2_to_3:
            iso_3 = alpha_2_to_3[iso_2].lower()
            temp_dir = os.path.join(tif_dir, iso_3)
            if os.path.exists(temp_dir):
                tif_filepath = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.tif')]
                if tif_filepath:
                    tif_filepaths[iso_2] = tif_filepath
            else:
                tif_filepaths[iso_2] = None
        else:
            tif_filepaths[iso_2] = None

    for country, files in tif_filepaths.items():
        if files is None:
            continue 
        my_dict[country] = []
        year_file = {}

        for file in files:
            if os.path.exists(file):
                tokens = os.path.basename(file).replace('.tif', '').split('_')
                parts = []
                for token in tokens:
                    if token.startswith('20') and token.isdigit():
                        parts.append(int(token))
                year = None
                if parts:
                    try:
                        year = int(parts[0])
                        my_dict[country].append(year)
                        year_file[year] = file
                    except ValueError:
                        logger.warning("Could not parse year from filename: %s", file)
        if my_dict[country]:
            latest_year = max(my_dict[country])
            my_dict[country] = year_file[latest_year]
        else:
            del my_dict[country]
    return my_dict

def orchestrate_country_intersection(raster_path, polygons_gdf, watershed_gdf, output_path, min_pixels=9, zoom_level=8): 
    """Process one country raster: create signed raster and extract unserved islands."""
    filepath, sum_pos, sum_neg = polygon_raster_sign_from_gdf(raster_path, polygons_gdf, output_path)
    #gdf = extract_worldpop_optimized_v2(raster_path, polygons_gdf)
    gdf = extract_worldpop_universal(raster_path, watershed_gdf, polygons_gdf, min_pixels=min_pixels, zoom_level=zoom_level)
    return filepath, sum_pos, sum_neg, gdf

def orchestrate_intersections(tif_dict, gdf, watershed_gdf, output_dir, csv_output_filepath, non_served_outpath, max_workers=4,
                              min_pixels=9, zoom_level=8):
    """
    Process multiple raster files in parallel, intersecting with polygons
    and creating signed rasters (+ inside polygons, - outside).

    Parameters
    ----------
    tif_dict : dict
        Dictionary mapping country codes to raster filepaths.
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing polygons with 'ISO_2' column.
    output_dir : str
        Directory where output rasters will be saved.
    max_workers : int
        Maximum number of parallel processes.
    """
    logger.info("Starting orchestration for %s countries", len(tif_dict))
    results = {}

    # Create a mapping: Future -> country
    future_to_country = {}

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        countries = list(tif_dict.keys())  # convert dict keys to a list
        random.shuffle(countries)          # shuffle in-place

        if os.path.exists(csv_output_filepath):
            stats = pd.read_csv(csv_output_filepath).country.unique()
            countries = [c for c in countries if c not in stats]
            logger.info("Skipping %s previously processed countries", len(tif_dict) - len(countries))
        
        for country in countries:
            tif_filepath = tif_dict[country]
            country_dir = os.path.join(output_dir, country)
            os.makedirs(country_dir, exist_ok=True)

            output_filepath = os.path.join(
                country_dir, f'WWTP_{os.path.basename(tif_filepath)}'
            )

            future = executor.submit(
                orchestrate_country_intersection,
                tif_filepath,
                gdf[gdf['ISO_2'] == country],
                watershed_gdf[watershed_gdf['ISO_2'] == country],
                output_filepath,
                min_pixels=min_pixels,
                zoom_level=zoom_level
            )
            future_to_country[future] = country
        # Collect results with a progress bar

        for future in tqdm(as_completed(future_to_country),
                           total=len(future_to_country),
                           desc="Processing countries"):
            country = future_to_country[future]
            try:
                _, sum_pos, sum_neg, gdf = future.result()  # Raises exception if failed

                if gdf is not None and not gdf.empty:
                    gdf['country'] = country
                    if gdf.crs != 4326:
                        gdf = gdf.to_crs(4326)
                    gdf['geometry'] = gdf.geometry.apply(to_wkt)                
                elif gdf is None:
                    logger.warning("[%s] No island dataframe returned", country)

                if sum_pos is None:
                    logger.warning("[%s] Skipping stats write due to missing signed sums", country)
                    continue
                
                stats = {
                    'country' : [country],
                    'population_served' : [sum_pos],
                    'population_unserved': [abs(sum_neg)],
                    'population_total': [sum_pos + abs(sum_neg)],
                    'population_served_index': [sum_pos/(sum_pos + abs(sum_neg) + 0.1)]
                }
                stats = pd.DataFrame(stats)
                if os.path.exists(csv_output_filepath):
                    stats.to_csv(csv_output_filepath, index=False, mode='a', header=False)
                else: 
                    stats.to_csv(csv_output_filepath, index=False, header=True)

                if gdf is not None and not gdf.empty:
                    if os.path.exists(non_served_outpath.replace('.gpkg', '.csv')):
                        gdf.to_csv(non_served_outpath.replace('.gpkg', '.csv'), index=False, mode='a', header=False)
                    else:
                        gdf.to_csv(non_served_outpath.replace('.gpkg', '.csv'), index=False, header=True)
            
                logger.warning("[OK] %s: processed successfully", country)
                results[country] = True
            except Exception as e:
                logger.exception("[FAIL] %s: failed with error: %s", country, e)
                results[country] = False
    return results

def parse_args():
    """Parse optional positional sharding args: job_index and total_jobs."""
    parser = argparse.ArgumentParser(
        description="Create signed rasters and unserved island stats for a country shard."
    )
    parser.add_argument("job_index", nargs="?", type=int, default=0)
    parser.add_argument("total_jobs", nargs="?", type=int, default=1)
    return parser.parse_args()

def shard_tif_dict(tif_dict, job_index, total_jobs, seed):
    """Return a deterministic job shard of country rasters for this worker."""
    if total_jobs < 1:
        raise ValueError(f"total_jobs must be >= 1, got {total_jobs}")
    if job_index < 0 or job_index >= total_jobs:
        raise ValueError(f"job_index must be in [0, {total_jobs - 1}], got {job_index}")

    countries = sorted(tif_dict.keys())
    random.Random(seed).shuffle(countries)
    shard_countries = countries[job_index::total_jobs]
    return {country: tif_dict[country] for country in shard_countries}

def main():
    """Entry point: load configuration, prepare inputs, and run country batch processing."""
    args = parse_args()
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    max_workers = cfg['annotations']['max_workers']
    seed = int(cfg['annotations']['random_seed'])
    min_pixels = int(cfg['min_pixels'])
    tif_dir = cfg['paths']['pop_tif_dir']

    zoom_level = int(cfg['zoom_level'])
    output_tif_dir = cfg['paths']['WWTP_tif_dir']
    non_served_outpath = os.path.abspath(cfg['paths']['non_served_outpath'].replace('.gpkg', '.csv'))
    csv_output_filepath = os.path.abspath(cfg['paths']['csv_output_filepath'].replace('.gpkg', '.csv'))

    approach = cfg['figures']['approach']
    voronoi_3a_filepath = os.path.abspath(create_pop_output_paths['voronoi'][approach])
    if not os.path.exists(output_tif_dir):
        os.makedirs(output_tif_dir, exist_ok=True)
    logger.info("Loading Voronoi polygons from %s", voronoi_3a_filepath)

    gdf = gpd.read_file(voronoi_3a_filepath)
    tif_dict = find_the_newest_tif_files(gdf['ISO_2'].unique(), tif_dir)
    logger.info("Resolved %s newest country TIFF files", len(tif_dict))

    tif_dict = shard_tif_dict(tif_dict, args.job_index, args.total_jobs, seed)
    logger.info(
        "Running shard %s/%s with %s countries",
        args.job_index,
        args.total_jobs,
        len(tif_dict)
    )

    watershed_gdf = gpd.read_file(cfg['paths']['watershed'], crs='epsg:4326').drop_duplicates(subset=['HYBAS_ID', 'geometry'], keep='first').reset_index(drop=True)
    if 'ISO_2' not in watershed_gdf.columns: 
        logger.warning("Watershed ISO_2 missing; running overture enrichment")
        if not os.path.exists(cfg['paths']['overture']):
            download_overture_maps(cfg['paths']['overture_s3_url'], cfg['paths']['overture'])
        watershed_gdf = duckdb_intersect(watershed_gdf, cfg['paths']['overture'])
        watesrhed_gdf.to_file(cfg['paths']['watershed'].replace('.geojson', '.gpkg'), driver='GPKG', index=False)
    
    logger.info("Starting country intersection workflow with max_workers=%s", max_workers)
    orchestrate_intersections(tif_dict, gdf, watershed_gdf, output_tif_dir, csv_output_filepath, non_served_outpath, max_workers, min_pixels=min_pixels, zoom_level=zoom_level)
if __name__ == '__main__':
    main()
    


