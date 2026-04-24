#!/usr/bin/env python
"""
Identify unconnected industrial areas not served by any WWTP.

This script:
1. Loads merged industrial land use data and WWTP locations.
2. Filters WTTPs to those with industrial or mixed usage.
3. Creates Voronoi service areas for those WTTPs.
4. Identifies industrial areas NOT overlapping any WWTP service area.
5. Saves unconnected areas to GeoPackage.

Usage:
    python -m research_code.industrial_analysis.find_unconnected_industrial_areas [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
"""

import sys
import os
import logging
from typing import Optional

import numpy as np
import geopandas as gpd
import pandas as pd

try:
    from ..starter import load_config, parse_config_overrides
    from .. import pipelines as _pipelines_module
    from ..pipelines import run_voronoi_approach, prepare_data, create_output_paths, _resolve_configured_callable
    from ..create_voronoi import intersect_with_polygon_sindex, orchestrate_overlaps, drop_duplicates
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    import research_code.pipelines as _pipelines_module
    from research_code.pipelines import run_voronoi_approach, prepare_data, create_output_paths, _resolve_configured_callable
    from research_code.create_voronoi import intersect_with_polygon_sindex, orchestrate_overlaps, drop_duplicates

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)


def load_industrial_areas(cfg: dict) -> Optional[gpd.GeoDataFrame]:
    """Load merged industrial areas from GeoPackage."""
    path = cfg['paths']['industrial_merged_gpkg']
    
    if not os.path.exists(path):
        logger.warning(f"Industrial areas file not found: {path}")
        return None
    
    logger.info(f"Loading industrial areas from {path}...")
    gdf = gpd.read_file(path, driver='GPKG')
    logger.info(f"Loaded {len(gdf)} industrial features")
    
    return gdf


def load_wwtps(cfg: dict, approach_id: str) -> gpd.GeoDataFrame:
    """Load WWTP data and add basin information if missing."""
    path = cfg['paths']['corrected_all_filepath']
    logger.info(f"Loading WTTPs from {path}...")
    
    gdf = gpd.read_file(path, driver='GPKG')
    logger.info(f"Loaded {len(gdf)} WTTPs")
    
    basin_col = cfg['basin_column_name']
    if approach_id == '1' and (basin_col not in gdf.columns or gdf[basin_col].isna().all()):
        logger.info(f"Basin information '{basin_col}' missing; attempting to add from watershed intersection...")
        watershed_gdf = gpd.read_file(
            cfg['paths']['watershed'],
            driver='GPKG'
        )

        if basin_col not in watershed_gdf.columns:
            raise KeyError(f"Configured basin column '{basin_col}' not found in watershed dataset.")
        
        if gdf.crs != watershed_gdf.crs:
            watershed_gdf = watershed_gdf.to_crs(gdf.crs)
        
        # Use the same watershed intersection method as industrial vectorization.
        gdf = intersect_with_polygon_sindex(
            gdf,
            watershed_gdf[[basin_col, 'geometry']].copy(),
            basin_col,
            concurrency=cfg['sindex_concurrency'],
        )
        logger.info(f"Added basin info to {len(gdf[gdf[basin_col].notna()])} WTTPs")
    
    return gdf


def filter_industrial_wwtps(cfg: dict, wwtps_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Filter WTTPs to those with industrial or mixed usage."""
    industrial_categories = cfg['industrial_category_numbers']
    
    if not industrial_categories:
        logger.warning("No industrial categories configured; using all WTTPs")
        return wwtps_gdf
    
    logger.info(f"Filtering WTTPs by industrial categories: {industrial_categories}")
    
    # category_number may be numeric; cast to str for consistent comparison
    cat_as_str = wwtps_gdf['category_number'].astype(str)
    industrial_as_str = [str(c) for c in industrial_categories]
    mask = cat_as_str.isin(industrial_as_str) | cat_as_str.str.contains('mix', case=False, na=False)
    
    filtered = wwtps_gdf[mask].copy()
    logger.info(f"Filtered to {len(filtered)} industrial/mixed WTTPs (from {len(wwtps_gdf)} total)")
    
    return filtered


def run_voronoi_for_wwtps(
    cfg: dict,
    approach_id: str,
    wwtps_gdf: gpd.GeoDataFrame,
    watershed_gdf: gpd.GeoDataFrame,
    country_gdf: gpd.GeoDataFrame,
    paths_dict: dict,
    output_path: str,
    only_round: bool,
) -> Optional[gpd.GeoDataFrame]:
    """
    Create Voronoi service areas for WTTPs using default approach.
    
    Parameters
    ----------
    cfg : dict
        Configuration dictionary.
    wwtps_gdf : geopandas.GeoDataFrame
        Filtered WWTP locations.
    country_gdf : geopandas.GeoDataFrame
        Country boundary polygons.
    
    Returns
    -------
    geopandas.GeoDataFrame or None
        Voronoi polygons, or None if orchestration failed.
    """
    logger.info(f"Running Voronoi diagram orchestration for filtered WTTPs (approach {approach_id} style)...")

    basin_col = cfg['basin_column_name']
    country_output_col = cfg['country_output_column']
    site_id_col = cfg['site_id_column']
    if basin_col not in wwtps_gdf.columns:
        raise KeyError(f"Expected basin column '{basin_col}' in WWTP dataframe before Voronoi run.")
    if basin_col not in watershed_gdf.columns:
        raise KeyError(f"Expected basin column '{basin_col}' in watershed dataframe.")

    scale_weights = cfg['weight_func'] in {'mult', 'add'}
    if approach_id == '1':
        # Match create_voronoi approach 1 setup.
        run_gdf = wwtps_gdf.copy()
        run_gdf['buffer_id'] = run_gdf[basin_col]
        clipping_gdf = watershed_gdf.copy()
        clipping_gdf['buffer_id'] = clipping_gdf[basin_col]
        buffering = True
    elif approach_id == '0':
        # Match create_voronoi approach 0 setup.
        dissolved_buffers = orchestrate_overlaps(
            wwtps_gdf.copy(),
            cfg['max_workers'],
            paths_dict['buffers']['WWTP'],
            cfg['buffer'],
            country_col=country_output_col,
        )
        dissolved_buffers = drop_duplicates(drop_duplicates(dissolved_buffers, site_id_col), 'geometry')
        dissolved_buffers['buffer_id'] = np.arange(len(dissolved_buffers))

        run_gdf = wwtps_gdf.copy()
        run_gdf = intersect_with_polygon_sindex(
            run_gdf,
            dissolved_buffers,
            'buffer_id',
            concurrency=cfg['sindex_concurrency'],
        )
        run_gdf = drop_duplicates(drop_duplicates(run_gdf, site_id_col), 'geometry')
        clipping_gdf = dissolved_buffers
        buffering = False
    else:
        raise ValueError(f"Unsupported approach '{approach_id}'. Supported: 0, 1")

    result = run_voronoi_approach(approach_id, run_gdf, clipping_gdf, country_gdf, cfg, cfg['distance_fn'],
                                  output_path, buffer_id_col='buffer_id',
                                  scale_weights=scale_weights, only_round=only_round, buffering=buffering,
                                  method=cfg['weight_method'])

    region_df, _ = result
    if region_df is None:
        logger.error("Voronoi orchestration failed")
        return None
    logger.info(f"Got {len(region_df)} Voronoi polygons directly from orchestration")
    return region_df


def find_unconnected_areas(
    industrial_gdf: gpd.GeoDataFrame,
    voronoi_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Identify industrial areas NOT overlapping any WWTP service area.
    
    Parameters
    ----------
    industrial_gdf : geopandas.GeoDataFrame
        Industrial land use areas.
    voronoi_gdf : geopandas.GeoDataFrame
        WWTP service area polygons.
    
    Returns
    -------
    geopandas.GeoDataFrame
        Industrial areas with no WWTP service.
    """
    logger.info("Finding unconnected industrial areas...")
    
    # Ensure same CRS
    if industrial_gdf.crs != voronoi_gdf.crs:
        voronoi_gdf = voronoi_gdf.to_crs(industrial_gdf.crs)
    
    # Spatial join: find industrial areas within Voronoi service zones
    joined = gpd.sjoin(
        industrial_gdf,
        voronoi_gdf[['geometry']],
        how='left',
        predicate='within'
    )
    
    # Filter to those WITHOUT a match (index_right is NaN)
    unconnected = joined[joined['index_right'].isna()].copy()
    unconnected = unconnected.drop(columns=['index_right'])
    
    logger.info(f"Found {len(unconnected)} unconnected industrial areas (from {len(industrial_gdf)} total)")
    
    return unconnected


def main():
    """Identify and save unconnected industrial areas."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Find unconnected industrial areas using Approach 1 Voronoi with industrial-filtered WWTPs'
    )
    parser.add_argument('--approach', nargs='+', type=str, default=None,
                       help='Approach(es) to run: 0 (WWTP no watersheds), 1 (WWTP with watersheds). Default: 1')
    parser.add_argument('--only_round', action='store_true',
                       help='Use only round-area weights (same meaning as create_voronoi).')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('level', nargs='?', default=None, help='Optional config level override')
    parser.add_argument('version', nargs='?', default=None, help='Optional config version override')
    parser.add_argument('buffer', nargs='?', default=None, help='Optional config buffer override')
    parser.add_argument('weight_method', nargs='?', default=None, help='Optional config weight_method override')
    parser.add_argument('weight_func', nargs='?', default=None, help="Optional config weight_func override: 'mult', 'add', or ''")
    parser.add_argument('dynamic_buffering', nargs='?', default=None, help='Optional dynamic buffering override (true/false)')
    parser.add_argument('dynamic_buffer_k', nargs='?', default=None, help='Optional dynamic buffer scaling override')

    args = parser.parse_args()

    requested_approaches = [str(a).lower() for a in args.approach] if args.approach else ['1']
    valid_approaches = {'0', '1'}
    invalid = [a for a in requested_approaches if a not in valid_approaches]
    if invalid:
        parser.error(f"Invalid approach(es): {', '.join(invalid)}. Valid: 0, 1")
    if len(requested_approaches) > 1:
        parser.error("This script accepts one approach per run. Use --approach 0 or --approach 1")
    selected_approach = requested_approaches[0]

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    overrides = parse_config_overrides(args=args)
    cfg = load_config(**overrides)
    only_round = args.only_round
    paths_dict = create_output_paths(cfg)
    
    output_path = cfg['paths']['industrial_unconnected_output']
    overwrite = cfg['industrial_unconnected_overwrite']
    
    # Check if output exists and overwrite is disabled
    if os.path.exists(output_path) and not overwrite:
        logger.info(f"Output file exists and overwrite=false. Skipping.")
        return True
    
    try:
        # Load data
        industrial_gdf = load_industrial_areas(cfg)
        if industrial_gdf is None or industrial_gdf.empty:
            logger.error("No industrial areas available; cannot proceed")
            return False
        
        wwtps_gdf = load_wwtps(cfg, approach_id=selected_approach)
        if wwtps_gdf.empty:
            logger.error("No WTTPs available; cannot proceed")
            return False
        
        # Filter to industrial/mixed WTTPs
        industrial_wwtps = filter_industrial_wwtps(cfg, wwtps_gdf)
        if industrial_wwtps.empty:
            logger.warning("No industrial/mixed WTTPs after filtering")
            unconnected = industrial_gdf.copy()
        else:
            # Load preprocessed watershed + country data
            data = _resolve_configured_callable(
                cfg['prepare_data_fn'], prepare_data, 'prepare_data_fn', _pipelines_module,
            )(cfg)
            country_gdf = data['country_df']
            watershed_gdf = data['watershed_gdf']
            path_key = f"{selected_approach}_only_round" if only_round and selected_approach in {'0', '1'} else selected_approach
            voronoi_output_path = paths_dict['voronoi'][path_key]
            
            # Run Voronoi orchestration
            voronoi_gdf = run_voronoi_for_wwtps(
                cfg,
                selected_approach,
                industrial_wwtps,
                watershed_gdf,
                country_gdf,
                paths_dict,
                voronoi_output_path,
                only_round,
            )
            if voronoi_gdf is None or voronoi_gdf.empty:
                logger.error("Failed to generate Voronoi service areas")
                return False
            
            # Find unconnected areas
            unconnected = find_unconnected_areas(industrial_gdf, voronoi_gdf)
        
        # Save output
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if os.path.exists(output_path):
            os.remove(output_path)
        
        logger.info(f"Writing {len(unconnected)} unconnected areas to {output_path}...")
        unconnected.to_file(output_path, driver='GPKG', index=False)
        logger.info(f"Successfully created {output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
