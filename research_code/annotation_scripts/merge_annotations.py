"""Merge annotation model outputs back into the main WWTP dataset.

The script parses model text responses into structured fields and joins them to
the main geospatial table via the numeric identifier encoded in image names.
"""

import os
import re

import geopandas as gpd
import pandas as pd

from research_code.starter import load_config

def decode_gen_text(text):
    """Parse model output text into category number/name and justification.

    Expected format is roughly:
    "<category_number>.<category_name>: <justification>"

    The parser is tolerant to malformed rows and returns ``None`` values when a
    reliable parse is not possible.
    """
    if not isinstance(text, str):
        return None, None, None

    left_right = text.split(":", 1)
    left = left_right[0].strip()
    justification = left_right[1].strip() if len(left_right) > 1 else None

    category_number = None
    category_name = None
    context = [part.strip() for part in left.split(".", 1)]
    if len(context) == 2:
        category_number, category_name = context

    def _clean_field(value):
        if not isinstance(value, str):
            return None
        value = value.strip()
        if not value or value.startswith("["):
            return None
        return value

    return _clean_field(category_number), _clean_field(category_name), _clean_field(justification)


def parse_idx_from_image_name(image_name):
    """Extract numeric ``idx`` from image filenames like ``tile_123.png``."""
    if not isinstance(image_name, str):
        return None

    stem = os.path.splitext(os.path.basename(image_name))[0]
    match = re.search(r"(\d+)$", stem)
    return int(match.group(1)) if match else None

def main():
    """Load annotations, merge parsed labels onto WWTP points, and overwrite output."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()

    image_input_dir = cfg['paths']['annotated_images_output_dir']
    filepath = cfg['paths']['annotations_results_filepath']

    df = pd.read_csv(filepath)
    required_cols = {'gen_text', 'image'}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        raise KeyError(f"Missing expected annotation columns: {sorted(missing_cols)}")

    df[['category_number', 'category_name', 'justification']] = df['gen_text'].apply(
        lambda x: pd.Series(decode_gen_text(x))
    )
    df['filepath'] = df['image'].apply(lambda x: os.path.join(image_input_dir, x))

    points_df = gpd.read_file(cfg['paths']['corrected_all_filepath'])
    points_df['idx'] = points_df['idx'].astype(int, copy=False)

    # Avoid duplicated *_x/*_y columns on repeated script runs.
    annotation_cols = ['category_number', 'category_name', 'justification']
    points_df = points_df.drop(columns=annotation_cols, errors='ignore')

    df['idx'] = df['image'].apply(parse_idx_from_image_name)
    df = df[df['idx'].notna()].copy()
    df['idx'] = df['idx'].astype(int)
    df.drop(axis=1, inplace=True, columns=['filepath', 'gen_text'])

    # Merge parsed annotations onto the geospatial points table.
    merged_df = gpd.GeoDataFrame(pd.merge(points_df, df, on='idx', how='left'), geometry='geometry', crs=points_df.crs)
    merged_df.to_file(index=False, driver='GPKG', filename=cfg['paths']['corrected_all_filepath'])

if __name__ == "__main__":
    main()  


