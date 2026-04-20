"""Merge segmentation outputs into WWTP geospatial datasets.

Supports two workflows:
- old: zipped tile CSVs plus mapping file
- new: single flat CSV keyed by image filename
"""

import os
import argparse
import pandas as pd
import geopandas as gpd
import zipfile
try:
    from ..starter import load_config, parse_config_overrides
except ImportError:
    from research_code.starter import load_config, parse_config_overrides

def assign_to_nearest(gdf_source, gdf_target):
    """Attach nearest target attributes to each source geometry.

    Source rows without valid geometry or without a successful nearest-neighbor
    lookup are preserved and returned without merged target attributes.
    """
    gdf_source = gdf_source.copy()
    source_crs = gdf_source.crs
    gdf_source = gdf_source.to_crs(gdf_target.crs)
    sindex = gdf_target.sindex

    nearest_matches = []
    for geom in gdf_source.geometry:
        if geom is None or geom.is_empty:
            nearest_matches.append(None)
            continue
        try:
            nearest_idx = list(sindex.nearest(geom))[1][0]
            nearest_matches.append(nearest_idx)
        except Exception:
            nearest_matches.append(None)

    gdf_source['nearest_index'] = nearest_matches
    gdf_source_na = gdf_source[gdf_source['nearest_index'].isna()]
    gdf_source = gdf_source[gdf_source['nearest_index'].notna()].copy()
    gdf_source['nearest_index'] = gdf_source['nearest_index'].astype(int)
    gdf_source = gdf_source.merge(
        gdf_target, left_on='nearest_index', right_index=True, suffixes=('', '_nearest')
    )
    gdf_source = pd.concat([gdf_source, gdf_source_na], ignore_index=True)
    gdf_source = gpd.GeoDataFrame(gdf_source, geometry='geometry', crs=gdf_target.crs)

    if 'nearest_index' in gdf_source.columns:
        gdf_source.drop(columns=['nearest_index'], inplace=True)
    if 'geometry_nearest' in gdf_source.columns:
        gdf_source.drop(columns=['geometry_nearest'], inplace=True)
    return gdf_source.to_crs(source_crs)

def merge_old(cfg):
    """Merge zipped segmentation outputs into the legacy corrected dataset.

    Parameters
    ----------
    cfg : dict
        Runtime configuration dictionary.
    """
    paths = cfg['paths']
    mapping_filepath = paths["dl_mapfile"]
    zip_filepath = paths["dl_zipfile"]
    zip_output_path = os.path.join(paths['data_dir'], paths["dl_dir"], os.path.basename(paths["dl_zipfile"]).split('.')[-2])

    gdf = gpd.read_file(paths["corrected_south"])
    mapping = gpd.read_file(os.path.abspath(mapping_filepath))
    mapping['idx'] = mapping['idx'].astype(int)

    if not os.path.exists(zip_output_path):
        os.makedirs(zip_output_path, exist_ok=True)

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(zip_output_path)

    data = []
    zip_output_path = os.path.join(zip_output_path, os.path.basename(paths["dl_zipfile"]).split('.')[-2])
    for file in [os.path.join(zip_output_path, f) for f in os.listdir(zip_output_path) if f.endswith('.csv')]:
        data.append(pd.read_csv(file))

    data = pd.concat(data, ignore_index=True)
    data['idx'] = data['File Name'].apply(lambda val: int(val.split('.')[0]))
    data['idx'] = data['idx'].astype(int)
    data = data.sort_values(by='idx', ascending=True)
    data = pd.merge(data, mapping, on=['idx'])
    data = gpd.GeoDataFrame(data, geometry='geometry', crs=mapping.crs)
    not_valids = gdf[gdf['geometry'].isna()]
    main_data = assign_to_nearest(gdf[gdf['geometry'].notna()], data)
    main_data = pd.concat([main_data, not_valids], ignore_index=True)
    main_data.to_file(
        os.path.join(paths["data_dir"], paths["seg_corrected_south"]),
        driver='GPKG',
        index=False,
    )

def merge_new(cfg):
    """Merge flat CSV segmentation outputs into the current corrected dataset.

    Parameters
    ----------
    cfg : dict
        Runtime configuration dictionary.
    """
    paths = cfg['paths']
    points_df = gpd.read_file(paths['corrected_all_filepath'])
    points_df['idx'] = points_df['idx'].astype(int)
    seg_results = pd.read_csv(paths['seg_results_filepath'])
    seg_results['idx'] = seg_results['img_name'].apply(lambda x: int(x.split('.')[0]))

    cols = ['num_detection_circle', 'diameters', 'num_detection_rect', 'wwtp_area_rect']
    extra_cols = ['wwtp_area_square', 'num_detection_square']
    cols_to_drop = [col for col in cols + extra_cols if col in points_df.columns]
    points_df = points_df.drop(columns=cols_to_drop)

    merged_df = gpd.GeoDataFrame(
        pd.merge(points_df, seg_results, on='idx', how='left'),
        geometry='geometry',
        crs=points_df.crs,
    )
    merged_df.to_file(index=False, driver='GPKG', filename=paths['corrected_all_filepath'])

def parse_args():
    """Parse command-line arguments for merge workflow selection."""
    parser = argparse.ArgumentParser(description='Merge segmentation outputs into geospatial datasets.')
    parser.add_argument('level', nargs='?', default=None, help='Optional config level override')
    parser.add_argument('version', nargs='?', default=None, help='Optional config version override')
    parser.add_argument('buffer', nargs='?', default=None, help='Optional config buffer override')
    parser.add_argument('weight_method', nargs='?', default=None, help='Optional config weight_method override')
    parser.add_argument('weight_func', nargs='?', default=None, help="Optional config weight_func override: 'mult', 'add', or ''")
    parser.add_argument(
        '--variant',
        choices=['old', 'new'],
        default='old',
        help='Choose the merge workflow: old (zipped tiles + mapping) or new (single CSV results).',
    )
    return parser.parse_args()

def main():
    """Parse CLI options and run the selected segmentation-merge workflow.

    Returns
    -------
    None
        This function dispatches to either the legacy or current merge path.
    """
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parse_args()
    overrides = parse_config_overrides(args=args)
    cfg = load_config(**overrides)

    if args.variant == 'old':
        if cfg['legacy_merge']:
            merge_old(cfg)
        return
    merge_new(cfg)

if __name__ == '__main__':
    main()