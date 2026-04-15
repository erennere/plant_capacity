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
from shapely.geometry import Point
import logging

logger = logging.getLogger(__name__)


def create_output_paths(cfg):
    """
    Generate all output file paths based on configuration.
    
    Args:
        cfg (dict): Configuration dictionary from load_config()
        
    Returns:
        dict: Dictionary with all output paths organized by approach
    """
    version = cfg['version']
    level = cfg['level']
    buffer = cfg['buffer']
    particle = cfg['particle']
    data_dir = os.path.abspath(cfg['paths']['data_dir'])
    voronoi_dir = os.path.abspath(cfg['paths']['voronoi_dir'])
    
    paths = {
        'buffers': {
            'WWTP': os.path.join(data_dir, f'dissolved_wwtp_buffers_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            'city': os.path.join(data_dir, f'dissolved_city_buffers_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            'WWTP_convex': os.path.join(data_dir, f'dissolved_wwtp_convex_hull_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            'city_convex': os.path.join(data_dir, f'dissolved_city_convex_hull_v{version}_bf{int(buffer)}_{particle}.gpkg'),
        },
        'voronoi': {
            '0': os.path.join(voronoi_dir, f'appr_0_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            '1a': os.path.join(voronoi_dir, f'appr_1_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            '1b': os.path.join(voronoi_dir, f'appr_1_v{version}_only_round_bf{int(buffer)}_{particle}.gpkg'),
            '1c': os.path.join(voronoi_dir, f'appr_1_v{version}_add_bf{int(buffer)}_{particle}.gpkg'),
            '1d': os.path.join(voronoi_dir, f'appr_1_v{version}_only_round_add_bf{int(buffer)}_{particle}.gpkg'),
            '2': os.path.join(voronoi_dir, f'appr_2_lvl_{level}_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            '3a': os.path.join(voronoi_dir, f'appr_3_lvl_{level}_v{version}_bf{int(buffer)}_{particle}.gpkg'),
            '3b': os.path.join(voronoi_dir, f'appr_3_lvl_{level}_v{version}_only_round_bf{int(buffer)}_{particle}.gpkg'),
            '3c': os.path.join(voronoi_dir, f'appr_3_lvl_{level}_v{version}_add_bf{int(buffer)}_{particle}.gpkg'),
            '3d': os.path.join(voronoi_dir, f'appr_3_lvl_{level}_v{version}_only_round_add_bf{int(buffer)}_{particle}.gpkg'),
            '4': os.path.join(voronoi_dir, f'appr_4_v{level}_bf{int(buffer)}_{particle}.gpkg'),
            '5': os.path.join(voronoi_dir, f'appr_5_v{version}_bf{int(buffer)}_{particle}.gpkg'),
        }
    }
    return paths


def run_voronoi_approach(approach_id, gdf, clipping_gdf, country_df, cfg, distance_fn, output_path, 
                        buffer_id_col='buffer_id', scale_weights=False, only_round=False, buffering=False, method='linear'):
    """
    Run a single Voronoi generation approach.
    
    Args:
        approach_id (str): Approach identifier for logging
        gdf (GeoDataFrame): Input sites
        clipping_gdf (GeoDataFrame): Clipping boundary
        country_df (GeoDataFrame): Country boundaries
        cfg (dict): Configuration dictionary
        distance_fn (callable): Distance function for Voronoi weighting
        output_path (str): Output file path
        buffer_id_col (str): Column name for buffer IDs in gdf
        scale_weights (bool): Whether to scale weights
        only_round (bool): Whether to round-only
        buffering (bool): Whether to apply buffer intersection
        
    Returns:
        tuple: (df_waste, region_df, point_df) or None if output exists
    """
    from create_voronoi import orchestrate_voronoi_weights, drop_duplicates
    
    #if os.path.exists(output_path):
    #    logger.info(f"Approach {approach_id}: Output exists at {output_path}, skipping")
    #    return None
    
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
    
    region_df.to_file(output_path, driver='GPKG', index=False)
    logger.info(f"Approach {approach_id}: Saved {len(region_df)} regions to {output_path}")
    return df_waste, region_df, point_df


def prepare_data(cfg):
    """
    Load and prepare all input data.
    
    Args:
        cfg (dict): Configuration dictionary
        
    Returns:
        dict: Dictionary with loaded GeoDataFrames (gdf_bbox, watershed_gdf, country_df)
    """
    from create_voronoi import (
        drop_duplicates, buffer_geometry, duckdb_intersect, 
        download_overture_maps, intersect_watershed_sindex, 
        orchestrate_overlaps
    )
    
    logger.info("Preparing input data...")
    paths = cfg['paths']
    
    # Load WWTP bounding boxes
    if cfg['csv_files']:
        gdf_bbox = pd.read_csv(os.path.abspath(paths['bboxes']))
        hydrowaste_df = pd.read_csv(os.path.abspath(paths['hydrowaste']))
        gdf_bbox = pd.merge(gdf_bbox, hydrowaste_df.drop(['LON_WWTP', 'LAT_WWTP', 'geometry', 'POP_SERVED'], axis=1), on=['WASTE_ID'])
        gdf_bbox = gpd.GeoDataFrame(gdf_bbox, geometry=shapely.wkt.loads(gdf_bbox['geometry']),  crs='epsg:4326')
    else:
        gdf_bbox = gpd.read_file(os.path.abspath(paths['corrected_all_filepath']))
        if 'final_geometry' in gdf_bbox.columns:
            gdf_bbox['geometry_wkt'] = gdf_bbox['geometry'].apply(to_wkt)
            gdf_bbox['geometry'] = gdf_bbox['final_geometry']
            gdf_bbox = gdf_bbox.drop(columns=['final_geometry'])
    
    gdf_bbox = drop_duplicates(drop_duplicates(gdf_bbox, 'WASTE_ID'), 'geometry')
    gdf_bbox['geometry'] = gdf_bbox['geometry'].apply(buffer_geometry)
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
        if not os.path.exists(os.path.abspath(paths['overture'])):
            download_overture_maps(paths['overture_s3_url'], os.path.abspath(paths['overture']))
        gdf_bbox = duckdb_intersect(gdf_bbox, os.path.abspath(paths['overture']))
    gdf_bbox.loc[gdf_bbox['ISO_2'].isna(), 'ISO_2'] = 'XX'
    
    # Load watersheds
    watershed_gdf = gpd.read_file(os.path.abspath(paths['watershed']), crs='epsg:4326')
    watershed_gdf = watershed_gdf.drop_duplicates(subset=['HYBAS_ID', 'geometry']).reset_index(drop=True)
    watershed_gdf['geometry'] = watershed_gdf['geometry'].apply(buffer_geometry)
    
    #if 'ISO_2' not in watershed_gdf.columns:
    if True:
        if 'ISO_2' in watershed_gdf.columns:
            watershed_gdf = watershed_gdf.drop(columns=['ISO_2'])
        if not os.path.exists(os.path.abspath(paths['overture'])):
            download_overture_maps(paths['overture_s3_url'], os.path.abspath(paths['overture']))
        watershed_gdf = duckdb_intersect(watershed_gdf, os.path.abspath(paths['overture']))
    watershed_gpkg_filepath = os.path.abspath(paths['watershed'].replace('.geojson', '.gpkg'))
    if not os.path.exists(watershed_gpkg_filepath):
        watershed_gdf.to_file(watershed_gpkg_filepath, driver='GPKG', index=False)

    # Add watershed information to WWTP
    if 'HYBAS_ID' not in gdf_bbox.columns:
        gdf_bbox = intersect_watershed_sindex(gdf_bbox, watershed_gdf, 'HYBAS_ID', concurrency=cfg['sindex_concurrency'])
        gdf_bbox = drop_duplicates(drop_duplicates(gdf_bbox, 'WASTE_ID'), 'geometry')
        filename = os.path.join(os.path.dirname(os.path.abspath(paths['bboxes'])), f"expanded_{os.path.basename(paths['bboxes'])}")
        if not os.path.exists(f"{filename}"):    
            gdf_bbox.to_csv(f"{filename}", index=False)
        if not os.path.exists(f"{filename.replace('.csv', '.gpkg')}"):
            gdf_bbox.to_file(f"{filename.replace('.csv', '.gpkg')}", index=False, driver='GPKG')
        
    # Load country boundaries
    country_df = pd.read_parquet(os.path.abspath(paths['overture']))
    country_df['geometry'] = country_df['geometry'].map(lambda geom: from_wkb(geom) if pd.notna(geom) else None)
    country_df = gpd.GeoDataFrame(country_df, geometry='geometry', crs=4326)
    
    logger.info(f"Loaded {len(gdf_bbox)} WWTP sites, {len(watershed_gdf)} watersheds, {len(country_df)} countries")
    return {'gdf_bbox': gdf_bbox, 'watershed_gdf': watershed_gdf, 'country_df': country_df}
