"""Split Voronoi outputs into verification, non-verification, and single-site groups.

Watersheds are selected for verification when the share of valid WWTP shapes in a
basin exceeds the configured `percent_verification` threshold.
"""
import os
import pandas as pd
import numpy as np
import geopandas as gpd
try:
    from ..starter import load_config, parse_config_overrides
except ImportError:
    from research_code.starter import load_config, parse_config_overrides

def find_verification_watersheds(gdf, percent_verification, watershed_col='HYBAS_ID'):
    """Mark each row with verification flags derived from basin-level validity rates."""
    gdf = gdf.copy()
    gdf['is_single_points'] = (
        gdf.groupby(watershed_col)[watershed_col].transform('size') == 1
    )
    gdf['use_verify'] = (
        (~gdf['is_single_points'])
        & (gdf['total_area'] != 0)
        & (gdf['round_area'] != 0)
    )
    gdf['watershed_fraction_valid'] = (
        gdf.groupby(watershed_col)['use_verify'].transform('mean')
    )
    gdf['watersheds_chosen'] = gdf['watershed_fraction_valid'] >= percent_verification
    return gdf


def main():
    """Split each population output into verification, non-verification, and single-site files."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(**overrides)
    verification_dir = cfg['paths']['verification_dir']
    pop_dir = cfg['paths']['pop_output_dir']
    percent_verification = cfg['percent_verification']

    if not os.path.exists(verification_dir):
        os.makedirs(verification_dir, exist_ok=True)
    filenames = [f for f in os.listdir(pop_dir)]
    
    for filename in filenames:
        if '_add_' in filename:
            print(filename)
            continue
        filepath = os.path.join(pop_dir, filename)
        ver_output_filepath = os.path.join(verification_dir, f'ver_{filename}')
        unver_output_filepath = os.path.join(verification_dir, f'unver_{filename}')
        single_output_filepath = os.path.join(verification_dir, f'single_{filename}')

        gdf = gpd.read_file(filepath)
        gdf = find_verification_watersheds(gdf, percent_verification)

        ver_gdf = gdf[gdf['watersheds_chosen']].reset_index(drop=True)
        un_ver_gdf = gdf[(~gdf['watersheds_chosen']) & (~gdf['is_single_points'])].reset_index(drop=True)
        singles_df = gdf[gdf['is_single_points']].reset_index(drop=True)

        if not ver_gdf.empty:
            ver_gdf.to_file(ver_output_filepath, driver='GPKG', index=False)
        if not un_ver_gdf.empty:
            un_ver_gdf.to_file(unver_output_filepath, driver='GPKG', index=False)
        if not singles_df.empty:
            singles_df.to_file(single_output_filepath, driver='GPKG', index=False)


if __name__ == '__main__':
    main()