import os
import pandas as pd
import geopandas as gpd
import zipfile
try:
    from ..starter import load_config
except ImportError:
    from research_code.starter import load_config

def assign_to_nearest(gdf_source, gdf_target):
    gdf_source = gdf_source.copy()
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
    return gdf_source

def main():
    ##os.chdir('D://work-heigit//plant-capacity//research_code')
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    globals().update(cfg)

    mapping_filepath = os.path.abspath(os.path.join(paths["dl_dir"], paths["dl_mapfile"]))
    zip_filepath = os.path.abspath(os.path.join(paths["dl_dir"], paths["dl_zipfile"]))
    zip_output_path = os.path.abspath(os.path.join(paths["dl_dir"], os.path.basename(paths["dl_zipfile"]).split('.')[-2]))
    
    gdf = gpd.read_file(paths["corrected_south"])
    mapping = gpd.read_file(os.path.abspath(mapping_filepath))
    mapping['idx'] = mapping['idx'].astype(int)

    if not os.path.exists(zip_output_path):
        os.makedirs(zip_output_path, exist_ok=True)

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(zip_output_path)
    
    data = []
    zip_output_path = os.path.join(zip_output_path,  os.path.basename(paths["dl_zipfile"]).split('.')[-2])
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
    main_data.to_file(os.path.join(paths["data_dir"], paths["seg_corrected_south"]),
                       driver='GPKG', index=False)

if __name__ == '__main__':
    main()