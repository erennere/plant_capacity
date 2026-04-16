import os
import re
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import Point, from_wkt, to_wkt
from scipy.spatial import cKDTree
from .correct_locations_w_OSMcorrect_locations_w_OSM import coordinate_corr_locations_wOSM, estimate_utm_epsg
from ..starter import load_config

def cluster_point_indices(geoms, threshold):
    geoms = [from_wkt(g) for g in geoms]
    coords = np.array([(pt.x, pt.y) for pt in geoms])
    tree = cKDTree(coords)
    neighbors = tree.query_ball_point(coords, threshold)

    visited = np.full(len(coords), False)
    clusters = []

    for i in range(len(coords)):
        if visited[i]:
            continue
        # Begin new cluster
        cluster = set()
        queue = [i]
        visited[i] = True
        while queue:
            idx = queue.pop()
            cluster.add(idx)
            for n_idx in neighbors[idx]:
                if not visited[n_idx]:
                    visited[n_idx] = True
                    queue.append(n_idx)
        clusters.append(cluster)
    return clusters

def cluster_points(df, threshold):
    df = df.copy()
    cluster_sets = cluster_point_indices(df['meter_geometry'], threshold)
    rows = []
    for cluster_set in cluster_sets:
        sub_df = df.iloc[list(cluster_set)]
        if len(sub_df) == 1:
            rows.append(sub_df.iloc[0])
            continue

        filled_counts = sub_df.notna().sum(axis=1)
        geom_idx = filled_counts.idxmax()
        merged = sub_df.apply(
            lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA
        )
        if 'POP_SERVED' in sub_df:
            merged['POP_SERVED'] = sub_df['POP_SERVED'].sum()
        if 'wwtp_area_square' in sub_df:
            merged['wwtp_area_square'] = '[' + str(sub_df['wwtp_area_square'].apply(
            lambda x: np.sum([
                float(i) for i in str(x).strip().strip('[]').split() 
                if i and i.lower() != 'none'
            ]) if pd.notnull(x) else 0).sum())+']'
        if 'diameters' in sub_df:
            sub_df['diameters_2'] = sub_df['diameters'].apply(
                lambda x: [float(i) for i in re.findall(r"[-+]?\d*\.\d+|\d+", str(x))])
            sub_df['round_area'] = sub_df['diameters_2'].apply(
                lambda y: np.sum([(d/2)**2 * np.pi for d in y]))
            merged['round_area'] = sub_df['round_area'].sum()
        merged["geometry"] = df.loc[geom_idx, "geometry"]
        rows.append(merged)
    result = pd.DataFrame(rows).reset_index(drop=True)
    return result

def find_unmatched_targets(gdf_source, gdf_target, threshold):
    """
    Returns rows from gdf_target that have no matching feature
    in gdf_source within a given threshold distance.
    """
    # Work in same CRS
    gdf_source = gdf_source.copy().to_crs(gdf_target.crs)
    sindex_source = gdf_source.sindex
    matched_target_indices = set()

    for idx, geom in enumerate(gdf_target.geometry):
        if geom is None or geom.is_empty:
            continue
        try:
            nearest_idx = list(sindex_source.nearest(geom, max_distance=threshold))[1][0]
            matched_target_indices.add(idx)
        except Exception:
            continue

    # Keep only target rows NOT in matched indices
    unmatched_targets = gdf_target[~gdf_target.index.isin(matched_target_indices)].copy()
    return unmatched_targets

def get_best_points(gdf):
    gdf = gdf.copy()
    gdf['geometry'] = gdf.apply(lambda row: Point(row['best_file2_lon'], row['best_file2_lat']) if pd.notna(row['best_file2_lon']) else row['geometry'], axis=1)
    high_conf =  gdf[gdf['detection_flag']].reset_index(drop=True)
    low_conf =  gdf[(gdf['detection_flag'] != True) & (pd.notna(gdf['geometry']))].reset_index(drop=True)
    return high_conf, low_conf

def find_safe_epsg(row):
    if isinstance(row['geometry'], Point):
        return estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
    else:
        return estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y)
    
def find_meter_coordinates(df):
    gdfs = []
    for epsg in df['epsg'].unique():
        subdf = df[df['epsg'] == epsg].copy()
        subdf['meter_geometry'] = subdf.to_crs(epsg).geometry.apply(lambda g: g.wkt)
        gdfs.append(subdf)
    return gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True),
        crs=4326,
        geometry='geometry'
    )

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    globals().update(cfg)

    old_df = gpd.read_file(os.path.abspath(paths["seg_corrected_south"]))

    canada_df = pd.read_csv(os.path.abspath(paths["canada_filepath"]), encoding='latin1')
    canada_df['geometry'] = canada_df.apply(lambda row: Point(row['Longitude/ Longitude'], row['Latitude/ Latitude']), axis=1)
    canada_df = gpd.GeoDataFrame(canada_df, geometry='geometry', crs=4326)

    us_df = gpd.read_file(os.path.abspath(paths['us_new_filepath']))
    high_conf_us, low_conf_us = get_best_points(us_df)
    eu_df = gpd.read_file(os.path.abspath(paths['eu_new_filepath']))
    high_conf_eu, low_conf_eu = get_best_points(eu_df)
    high_conf = gpd.GeoDataFrame(pd.concat([high_conf_eu, high_conf_us], ignore_index=True), geometry='geometry', crs=4326)
    low_conf = gpd.GeoDataFrame(pd.concat([low_conf_eu, low_conf_us], ignore_index=True), geometry='geometry', crs=4326)

    thailand_df = gpd.read_file(os.path.abspath(paths['thailand_filepath']))

    merged_df = gpd.GeoDataFrame(pd.concat([old_df, high_conf, canada_df, thailand_df], axis=0, ignore_index=True), crs=4326, geometry='geometry')

    germany_df = gpd.read_file(os.path.abspath(paths["germany_filepath"]))
    germany_df['geometry'] = germany_df.apply(lambda row: Point(row['neigh_lon'],
                                                                 row['neigh_lat']) if pd.notna(row['neigh_lat'])
                                                                   else row['geometry'], axis=1)
    
    new_ones = find_unmatched_targets(merged_df, germany_df, threshold).to_crs(4326)
    merged_df = pd.concat([merged_df, new_ones], axis=0, ignore_index=True)
    merged_df = gpd.GeoDataFrame(merged_df, crs=4326, geometry='geometry')
    
    low_conf['epsg'] = low_conf.apply(find_safe_epsg, axis=1)
    pdf = gpd.read_file(os.path.abspath(paths["osmgeo_filepath"]))
    pdf['epsg'] = pdf.apply(find_safe_epsg, axis=1)
    
    low_conf = coordinate_corr_locations_wOSM(osm_threshold, pdf, low_conf)
    low_conf['geometry'] = low_conf['matched_osm_geometry']
    low_conf = low_conf[low_conf['geometry'].notna()]

    merged_df = gpd.GeoDataFrame(pd.concat([merged_df, low_conf], axis=0, ignore_index=True), crs=4326, geometry='geometry')
    del low_conf, low_conf_eu,low_conf_us, high_conf, high_conf_eu, high_conf_us, new_ones, germany_df, canada_df, thailand_df, us_df, eu_df

    none_geo_df = merged_df[merged_df.geometry.isna()].reset_index(drop=True)
    merged_df   = merged_df[merged_df.geometry.notna()].reset_index(drop=True)

    merged_df['epsg'] = merged_df.apply(find_safe_epsg, axis=1)
    merged_df = find_meter_coordinates(merged_df)
    merged_df = cluster_points(merged_df, threshold)
    merged_df = gpd.GeoDataFrame(pd.concat([merged_df, none_geo_df], axis=0, ignore_index=True), crs=4326, geometry='geometry')
    merged_df['idx'] = range(0, len(merged_df))

    """     
    merged_df = merged_df[
    merged_df.geometry.notna() & merged_df.is_valid
].reset_index(drop=True) """
    
    if "FID" in merged_df:
        merged_df = merged_df.drop(columns=['FID'], errors='ignore')
    if "fid" in merged_df:
        merged_df = merged_df.drop(columns=['fid'], errors='ignore')

    rest = merged_df[~merged_df['geometry'].isin(set(old_df['geometry'].tolist()))].reset_index(drop=True)
    rest.to_file(os.path.abspath(paths["new_points_filepath"]), driver='GPKG', index=False)
    merged_df.to_file(os.path.abspath(paths["corrected_all_filepath"]), driver='GPKG', index=False)

if __name__ == '__main__':
    main()