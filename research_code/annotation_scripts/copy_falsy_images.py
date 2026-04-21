

import os
import shutil
import pandas as pd
import geopandas as gpd
from research_code.starter import load_config, parse_config_overrides
from research_code.create_voronoi import ensure_output_dir_for_file

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(**overrides)
    gdf = gpd.read_file(cfg['paths']['corrected_all_filepath'])
    gdf['category_number'] = gdf['category_number'].fillna(-1).astype(int)
    falsy_gdf = gdf[((gdf['category_number'] == -1) & gdf['image'].notna()) | (gdf['category_number'].isin([8, 2, 3, 7, 1]))]
    image_input_dir = cfg['paths']['annotated_images_output_dir']
    falsy_gdf['filepath'] = falsy_gdf['image'].apply(lambda x: os.path.join(image_input_dir, x))
    output_dir = os.path.join(os.path.dirname(cfg['paths']['annotations_verf_image_outpath_dir']), 'falsy_images')
    ensure_output_dir_for_file(os.path.join(output_dir, 'placeholder.txt'))

    for _, row in falsy_gdf.iterrows():
        src_path = row['filepath']
        if pd.isna(src_path):
            continue

        filename = os.path.basename(src_path)
        dest_path = os.path.join(output_dir, filename)
        if os.path.exists(dest_path):
            continue

        try:
            if os.path.exists(src_path):
                shutil.copy2(src_path, dest_path)
            else:
                print(f"File not found: {src_path}")
        except Exception as e:
            print(f"Error copying {filename}: {e}")

if __name__ == "__main__":
    main()
