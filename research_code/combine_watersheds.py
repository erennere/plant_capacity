import os
import zipfile
import tempfile
from pathlib import Path
import geopandas as gpd
from starter import load_config

def extract_and_merge_geodata(zip_dir, output_path, output_filename="merged.gpkg"):
    zip_dir = Path(zip_dir)
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    merged_gdf = None

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

                        if merged_gdf is None:
                            merged_gdf = gdf
                        else:
                            merged_gdf = merged_gdf._append(gdf, ignore_index=True)
                        break  # Stop after successfully reading one file per zip
                    except Exception as e:
                        continue

    if merged_gdf is not None:
        out_file = output_path / output_filename
        merged_gdf.to_file(os.path.abspath(out_file), driver="GPKG")
        print(f"\n✅ Merged GeoDataFrame written to {out_file}")
    else:
        print("\n⚠️ No valid geospatial files found.")

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    globals().update(cfg)
    extract_and_merge_geodata(os.path.abspath(paths["watersheds_zip_dir"]), os.path.abspath(paths["data_dir"]), output_filename=f"hydrobase_lvl{str(level)}_combined.gpkg")
if __name__ == '__main__':
    main()

