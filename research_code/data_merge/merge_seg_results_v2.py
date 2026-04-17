"""
This script merges the segmentation results with the original geospatial dataset. 
It reads the corrected geospatial data and the segmentation results,
merges them based on a common index, and saves the updated geospatial dataset with the new segmentation information."""
import os
import geopandas as gpd
import pandas as pd
try:
    from ..starter import load_config
except ImportError:
    from research_code.starter import load_config

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    points_df = gpd.read_file(cfg['paths']['corrected_all_filepath'])
    points_df['idx'] = points_df['idx'].astype(int)
    seg_results = pd.read_csv(cfg['paths']['seg_results_filepath'])
    seg_results['idx'] = seg_results['img_name'].apply(lambda x: int(x.split('.')[0]))

    cols = ['num_detection_circle', 'diameters', 'num_detection_rect', 'wwtp_area_rect']
    extra_cols = ['wwtp_area_square', 'num_detection_square']
    cols_to_drop = [col for col in cols + extra_cols if col in points_df.columns]
    points_df = points_df.drop(columns=cols_to_drop)

    # Merge the points_df with seg_results on 'idx'
    merged_df = gpd.GeoDataFrame(pd.merge(points_df, seg_results, on='idx', how='left'), geometry='geometry', crs=points_df.crs)
    merged_df.to_file(index=False, driver='GPKG', filename=cfg['paths']['corrected_all_filepath'])

if __name__ == "__main__":
    main() 







