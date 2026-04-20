"""Merge watershed archive contents into a single GeoPackage layer.

The script scans a directory of zip archives, extracts the first readable
geospatial layer from each archive, concatenates the results, and writes a
combined watershed dataset for the configured level.
"""

import os
import zipfile
import tempfile
from pathlib import Path
import geopandas as gpd
import pandas as pd
try:
    from .starter import load_config, parse_config_overrides
    from .create_voronoi import ensure_output_dir_for_file
except ImportError:  # Support running as a top-level script
    from starter import load_config, parse_config_overrides
    from create_voronoi import ensure_output_dir_for_file

def extract_and_merge_geodata(zip_dir, output_path, output_filename="merged.gpkg"):
    """Extract readable geospatial files from zip archives and merge them."""
    zip_dir = Path(zip_dir)
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    merged_frames = []

    for zip_file in zip_dir.glob("*.zip"):
        print(f"Processing {zip_file.name}")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            try:
                with zipfile.ZipFile(zip_file, 'r') as z:
                    z.extractall(tmpdir)
            except zipfile.BadZipFile:
                print(f"Skipping bad zip file: {zip_file}")
                continue

            # Walk through extracted contents to find geographic files
            for root, dirs, files in os.walk(tmpdir):
                for file in files:
                    filepath = Path(root) / file
                    try:
                        # Try reading with geopandas
                        gdf = gpd.read_file(filepath)
                        print(f"Opened: {filepath.name}")

                        merged_frames.append(gdf)
                        break  # Stop after successfully reading one file per zip
                    except Exception:
                        continue

    if merged_frames:
        merged_gdf = gpd.GeoDataFrame(
            pd.concat(merged_frames, ignore_index=True),
            geometry="geometry",
            crs=merged_frames[0].crs,
        )
        out_file = output_path / output_filename
        ensure_output_dir_for_file(out_file)
        merged_gdf.to_file(os.path.abspath(out_file), driver="GPKG")
        print(f"\n✅ Merged GeoDataFrame written to {out_file}")
    else:
        print("\n⚠️ No valid geospatial files found.")

def main():
    """Load config overrides and build the configured combined watershed layer."""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(**overrides)
    levels = [k for k in os.listdir(os.path.dirname(cfg["paths"]["watersheds_zip_dir"]))
              if os.path.isdir(os.path.join(os.path.dirname(cfg["paths"]["watersheds_zip_dir"]), k))]
    for level in levels:
        if not level.startswith('lvl'):
            continue
        level = level.replace('lvl', '')
        level_overrides = {
            "level": level,
            "version": overrides["version"],
            "buffer": overrides["buffer"],
            "weight_method": overrides["weight_method"],
            "weight_func": overrides["weight_func"],
        }
        cfg = load_config(**level_overrides)
        extract_and_merge_geodata(
            cfg["paths"]["watersheds_zip_dir"],
            os.path.dirname(cfg['paths']['watershed']),
            output_filename=os.path.basename(cfg['paths']['watershed'])
        )

if __name__ == '__main__':
    main()

