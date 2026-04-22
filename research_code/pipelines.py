"""
Pipeline orchestration for Voronoi-based spatial analysis.

Provides high-level workflow functions for different approaches to
Voronoi generation, path management, and data processing pipelines.
"""

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import from_wkt, to_wkt, from_wkb
import shapely
import logging

logger = logging.getLogger(__name__)


def create_output_paths(cfg):
    """Generate canonical output paths for all Voronoi approaches.

    Returns paths for the buffer products plus the approach family used by
    create_voronoi.py. Weighted/unweighted and multiplicative/additive variants
    are encoded in the output directory name via ``weight_type`` and
    ``weight_func`` (resolved from config); the only per-run variant captured
    here is the ``only_round`` flag:

    - `0` / `0_only_round`: WWTP buffer Voronoi (all points / only-round)
    - `1` / `1_only_round`: watershed-constrained Voronoi (all points / only-round)
    - `2`: city-based Voronoi
    """
    version = cfg['version']
    level = cfg['level']
    buffer = cfg['buffer']
    weight_func = cfg['weight_func']
    weight_func_suffix = cfg['weight_func_suffix']
    weight_type = cfg['weight_type']
    buffers_dir = cfg['paths']['buffers_dir']
    voronoi_dir = cfg['paths']['voronoi_dir']
    
    paths = {
        'buffers': {
            'WWTP': os.path.join(buffers_dir, f'dissolved_wwtp_buffers_v{version}_lvl{level}_bf{int(buffer)}.gpkg'),
            'city': os.path.join(buffers_dir, f'dissolved_city_buffers_v{version}_lvl{level}_bf{int(buffer)}.gpkg'),
            'WWTP_convex': os.path.join(buffers_dir, f'dissolved_wwtp_convex_hull_v{version}_lvl{level}_bf{int(buffer)}.gpkg'),
            'city_convex': os.path.join(buffers_dir, f'dissolved_city_convex_hull_v{version}_lvl{level}_bf{int(buffer)}.gpkg'),
        },
        'voronoi': {
            '0': os.path.join(voronoi_dir, f'appr_0_v{version}_lvl{level}_bf{int(buffer)}{weight_func}.gpkg'),
            '0_only_round': os.path.join(voronoi_dir, f'appr_0_only_round_v{version}_lvl{level}_bf{int(buffer)}{weight_func}.gpkg'),
            '1': os.path.join(voronoi_dir, f'appr_1_v{version}_lvl{level}_bf{int(buffer)}{weight_func}.gpkg'),
            '1_only_round': os.path.join(voronoi_dir, f'appr_1_only_round_v{version}_lvl{level}_bf{int(buffer)}{weight_func}.gpkg'),
            '2': os.path.join(voronoi_dir, f'appr_2_v{version}_lvl{level}_bf{int(buffer)}{weight_func}.gpkg'),
        }
        #'voronoi': {
        #    '0': os.path.join(voronoi_dir, f'appr_0_v{version}_lvl{level}_bf{int(buffer)}_{weight_type}{weight_func_suffix}.gpkg'),
        #    '0_only_round': os.path.join(voronoi_dir, f'appr_0_only_round_v{version}_lvl{level}_bf{int(buffer)}_{weight_type}{weight_func_suffix}.gpkg'),
        #    '1': os.path.join(voronoi_dir, f'appr_1_v{version}_lvl{level}_bf{int(buffer)}_{weight_type}{weight_func_suffix}.gpkg'),
        #    '1_only_round': os.path.join(voronoi_dir, f'appr_1_only_round_v{version}_lvl{level}_bf{int(buffer)}_{weight_type}{weight_func_suffix}.gpkg'),
        #    '2': os.path.join(voronoi_dir, f'appr_2_v{version}_lvl{level}_bf{int(buffer)}_{weight_type}{weight_func_suffix}.gpkg'),
        #}
    }
    return paths

def create_pop_output_paths(cfg):
    """Return output paths for population-enriched copies of Voronoi outputs."""
    voronois = create_output_paths(cfg)['voronoi']
    return {
        'voronoi' : {
            k: os.path.abspath(os.path.join(cfg['paths']['pop_output_dir'], f'pop_added_{os.path.basename(v)}')) for k, v in voronois.items()
            }
        }
    

def run_voronoi_approach(approach_id, gdf, clipping_gdf, country_df, cfg, distance_fn, output_path, 
                        buffer_id_col='buffer_id', scale_weights=False, only_round=False, buffering=False, method='linear'):
    """
    Run a single Voronoi generation approach.
    
    Parameters
    ----------
    approach_id : str
        Approach identifier such as ``0``, ``1a``, or ``3d``.
    gdf : geopandas.GeoDataFrame
        Input sites for the selected approach.
    clipping_gdf : geopandas.GeoDataFrame | None
        Optional clipping geometries used to trim Voronoi regions.
    country_df : geopandas.GeoDataFrame
        Country boundaries used for final clipping.
    cfg : dict
        Runtime configuration dictionary.
    distance_fn : callable
        Distance function used by the weighted Voronoi solver.
    output_path : str
        Output file path.
    buffer_id_col : str, default='buffer_id'
        Column used to group features before region generation.
    scale_weights : bool, default=False
        Whether to scale feature weights before Voronoi generation.
    only_round : bool, default=False
        Whether to use round-area weights only.
    buffering : bool, default=False
        Whether to intersect the output with local feature buffers.
    method : str, default='linear'
        Weight-transformation method passed into the Voronoi workflow.

    Returns
    -------
    tuple
        Tuple ``(df_waste, region_df, point_df)`` returned by the Voronoi workflow.

    Notes
    -----
    This function always overwrites ``output_path`` when called. Any skip-if-
    output-exists behavior must be implemented by the caller.
    """
    if os.path.exists(output_path) and not cfg['voronoi_overwrite']:
        logger.info(f"Approach {approach_id}: Output already exists at {output_path} and overwrite is False. Skipping.")
        return None, None, None
    
    try:
        from .create_voronoi import orchestrate_voronoi_weights, drop_duplicates, ensure_output_dir_for_file
    except ImportError:  # Support running as a top-level script
        from create_voronoi import orchestrate_voronoi_weights, drop_duplicates, ensure_output_dir_for_file
    
    logger.info(f"Approach {approach_id}: Running Voronoi generation (scale_weights={scale_weights}, only_round={only_round})")
    
    df_waste, region_df, point_df = orchestrate_voronoi_weights(
        gdf, buffer_id_col, country_df, cfg['max_workers'],
        scale_weights=scale_weights,
        clipping=clipping_gdf,
        n_points=cfg['n_points'],
        distance_fn=distance_fn,
        scipy_true=cfg['scipy_true'],
        cv2_true=cfg['cv2_true'],
        centroid_points=True,
        points_col=None,
        buffering=buffering,
        buffer=cfg['buffer'],
        threshold=cfg['threshold'],
        only_round=only_round,
        sigma=cfg['sigma'],
        percent_threshold=cfg['percent_threshold'],
        method=method
    )
    
    ensure_output_dir_for_file(output_path)
    region_df.to_file(output_path, driver='GPKG', index=False)
    logger.info(f"Approach {approach_id}: Saved {len(region_df)} regions to {output_path}")
    return df_waste, region_df, point_df


def prepare_data(cfg):
    """
    Load and prepare all input data.
    
    Parameters
    ----------
    cfg : dict
        Runtime configuration dictionary.

    Returns
    -------
    tuple
        Tuple ``(gdf_bbox, watershed_gdf, country_df)`` containing the prepared
        WWTP, watershed, and country layers.
    """
    try:
        from .create_voronoi import (
            drop_duplicates, buffer_geometry, duckdb_intersect,
            download_overture_maps, intersect_watershed_sindex,
            orchestrate_overlaps, ensure_output_dir_for_file,
        )
    except ImportError:  # Support running as a top-level script
        from create_voronoi import (
            drop_duplicates, buffer_geometry, duckdb_intersect,
            download_overture_maps, intersect_watershed_sindex,
            orchestrate_overlaps, ensure_output_dir_for_file,
        )
    
    logger.info("Preparing input data...")
    paths = cfg['paths']
    
    # Load WWTP bounding boxes
    if cfg['csv_files']:
        gdf_bbox = pd.read_csv(paths['bboxes'])
        hydrowaste_df = pd.read_csv(paths['hydrowaste'])
        gdf_bbox = pd.merge(gdf_bbox, hydrowaste_df.drop(['LON_WWTP', 'LAT_WWTP', 'geometry', 'POP_SERVED'], axis=1), on=['WASTE_ID'])
        gdf_bbox = gpd.GeoDataFrame(
            gdf_bbox,
            geometry=gdf_bbox['geometry'].map(from_wkt),
            crs='epsg:4326',
        )
    else:
        gdf_bbox = gpd.read_file(paths['corrected_all_filepath'])
        if 'final_geometry' in gdf_bbox.columns:
            gdf_bbox['geometry_wkt'] = gdf_bbox['geometry'].apply(to_wkt)
            gdf_bbox['geometry'] = gdf_bbox['final_geometry']
            gdf_bbox = gdf_bbox.drop(columns=['final_geometry'])
    
    gdf_bbox = drop_duplicates(drop_duplicates(gdf_bbox, 'WASTE_ID'), 'geometry')
    gdf_bbox['geometry'] = pd.Series(
        [buffer_geometry(geom) for geom in gdf_bbox['geometry']],
        index=gdf_bbox.index,
    )
    gdf_bbox['WKT_WWTP'] = gdf_bbox['geometry'].apply(lambda geom: to_wkt(geom))
    gdf_bbox['OLD_WASTE_ID'] = gdf_bbox['WASTE_ID']
    gdf_bbox['WASTE_ID'] = np.arange(len(gdf_bbox))

    if cfg['remove_industrial']:
        if 'category_number' in gdf_bbox.columns:
            initial_count = len(gdf_bbox)
            gdf_bbox = gdf_bbox[~gdf_bbox['category_number'].isin(cfg['industrial_category_numbers'])]
            logger.info(f"Removed {initial_count - len(gdf_bbox)} industrial sites based on category_number")
    
    # Add country codes
    #if 'ISO_2' not in gdf_bbox.columns:
    if True:
        if 'ISO_2' in gdf_bbox.columns:
            gdf_bbox = gdf_bbox.drop(columns=['ISO_2'])
        if not os.path.exists(paths['overture']):
            download_overture_maps(paths['overture_s3_url'], paths['overture'])
        gdf_bbox = duckdb_intersect(gdf_bbox, paths['overture'])
    gdf_bbox.loc[gdf_bbox['ISO_2'].isna(), 'ISO_2'] = 'XX'
    
    # Load watersheds
    watershed_gdf = gpd.read_file(paths['watershed'], crs='epsg:4326')
    watershed_gdf = watershed_gdf.drop_duplicates(subset=['HYBAS_ID', 'geometry']).reset_index(drop=True)
    watershed_gdf['geometry'] = pd.Series(
        [buffer_geometry(geom) for geom in watershed_gdf['geometry']],
        index=watershed_gdf.index,
    )
    
    #if 'ISO_2' not in watershed_gdf.columns:
    if True:
        if 'ISO_2' in watershed_gdf.columns:
            watershed_gdf = watershed_gdf.drop(columns=['ISO_2'])
        if not os.path.exists(paths['overture']):
            download_overture_maps(paths['overture_s3_url'], paths['overture'])
        watershed_gdf = duckdb_intersect(watershed_gdf, paths['overture'])
    watershed_gpkg_filepath = os.path.abspath(paths['watershed'].replace('.geojson', '.gpkg'))
    if not os.path.exists(watershed_gpkg_filepath):
        ensure_output_dir_for_file(watershed_gpkg_filepath)
        watershed_gdf.to_file(watershed_gpkg_filepath, driver='GPKG', index=False)

    # Add watershed information to WWTP
    if 'HYBAS_ID' not in gdf_bbox.columns:
        gdf_bbox = intersect_watershed_sindex(gdf_bbox, watershed_gdf, 'HYBAS_ID', concurrency=cfg['sindex_concurrency'])
        gdf_bbox = drop_duplicates(drop_duplicates(gdf_bbox, 'WASTE_ID'), 'geometry')
        filename = os.path.join(os.path.dirname(paths['bboxes']), f"expanded_{os.path.basename(paths['bboxes'])}")
        if not os.path.exists(f"{filename}"):    
            ensure_output_dir_for_file(filename)
            gdf_bbox.to_csv(f"{filename}", index=False)
        expanded_gpkg = filename.replace('.csv', '.gpkg')
        if not os.path.exists(expanded_gpkg):
            ensure_output_dir_for_file(expanded_gpkg)
            gdf_bbox_export = gpd.GeoDataFrame(gdf_bbox, geometry='geometry', crs='epsg:4326')
            gdf_bbox_export.to_file(expanded_gpkg, index=False, driver='GPKG')
        
    # Load country boundaries
    country_df = pd.read_parquet(paths['overture'])
    country_df['geometry'] = country_df['geometry'].map(lambda geom: from_wkb(geom) if pd.notna(geom) else None)
    country_df = gpd.GeoDataFrame(country_df, geometry='geometry', crs=4326)
    
    logger.info(f"Loaded {len(gdf_bbox)} WWTP sites, {len(watershed_gdf)} watersheds, {len(country_df)} countries")
    return {'gdf_bbox': gdf_bbox, 'watershed_gdf': watershed_gdf, 'country_df': country_df}
