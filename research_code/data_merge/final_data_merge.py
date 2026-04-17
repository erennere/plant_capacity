"""Build the final merged WWTP dataset from regional and model outputs.

This module merges corrected points with country-specific datasets,
applies confidence-aware enrichment, and deduplicates nearby facilities.
"""

import os
import re
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import Point, from_wkt, to_wkt
from scipy.spatial import KDTree as cKDTree
try:
    from .correct_locations_w_OSM import coordinate_corr_locations_wOSM, estimate_utm_epsg
    from ..starter import load_config
except ImportError:
    from research_code.data_merge.correct_locations_w_OSM import coordinate_corr_locations_wOSM, estimate_utm_epsg
    from research_code.starter import load_config

def cluster_point_indices(geoms, threshold):
    """Group point geometries into connected components within a distance threshold."""
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
    """Aggregate records whose meter-space geometries fall into the same cluster."""
    df = df.copy()
    cluster_sets = cluster_point_indices(df['meter_geometry'], threshold)
    rows = []
    for cluster_set in cluster_sets:
        sub_df = df.iloc[list(cluster_set)].copy()
        if len(sub_df) == 1:
            rows.append(sub_df.iloc[0])
            continue

        # Keep the row with most non-null attributes as the representative.
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
    """Return target rows that have no nearby source match within `threshold`."""
    # Work in same CRS
    gdf_source = gdf_source.copy().to_crs(gdf_target.crs)
    sindex_source = gdf_source.sindex
    matched_target_indices = set()

    for idx, geom in gdf_target.geometry.items():
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
    """Split points into high- and low-confidence sets after geometry override."""
    gdf = gdf.copy()
    gdf['geometry'] = gdf.apply(lambda row: Point(row['best_file2_lon'], row['best_file2_lat']) if pd.notna(row['best_file2_lon']) else row['geometry'], axis=1)
    high_conf =  gdf[gdf['detection_flag']].reset_index(drop=True)
    low_conf =  gdf[(gdf['detection_flag'] != True) & (pd.notna(gdf['geometry']))].reset_index(drop=True)
    return high_conf, low_conf

def find_safe_epsg(row):
    """Estimate a suitable projected EPSG code for distance-based operations."""
    if isinstance(row['geometry'], Point):
        return estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
    else:
        return estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y)
    
def find_meter_coordinates(df):
    """Create meter-space geometry WKT per EPSG group for clustering."""
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

def old_merging_logic(paths, eu_correction, eu_utm, osm_threshold, threshold):
    """Legacy merge routine retained for compatibility with historical outputs."""
    canada_df = pd.read_csv(paths["canada_filepath"], encoding='latin1')
    canada_df['geometry'] = canada_df.apply(lambda row: Point(row['Longitude/ Longitude'], row['Latitude/ Latitude']), axis=1)
    canada_df = gpd.GeoDataFrame(canada_df, geometry='geometry', crs=4326)

    us_df = pd.read_csv(paths["us_filepath"], encoding='latin1')
    us_df['geometry'] = us_df.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
    us_df = gpd.GeoDataFrame(us_df, geometry='geometry', crs=4326)

    germany_df = gpd.read_file(paths["germany_filepath"])
    germany_df['geometry'] = germany_df.apply(lambda row: Point(row['neigh_lon'],
                                                                 row['neigh_lat']) if pd.notna(row['neigh_lat'])
                                                                   else row['geometry'], axis=1)
    old_df = gpd.read_file(paths["seg_corrected_south"])

    # The EU dataset has to corrected as it seems to contain points that
    #do not correspond to any real location
    eu_df = gpd.read_file(paths["eu_ref_filepath"])
    if eu_correction:
        eu_df['epsg'] = eu_df.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                            if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
        
        pdf = gpd.read_file(paths["osmgeo_filepath"])
        pdf['epsg'] = pdf.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                            if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
        
        eu_df = coordinate_corr_locations_wOSM(osm_threshold, pdf, eu_df)
        eu_df['geometry'] = eu_df['matched_osm_geometry']
        eu_df = eu_df.to_crs(4326)
    eu_df = eu_df.to_crs(eu_utm)

    merged_df = gpd.GeoDataFrame(pd.concat([old_df, germany_df, us_df, canada_df], axis=0, ignore_index=True), crs=4326, geometry='geometry')
    new_ones = find_unmatched_targets(merged_df, eu_df, threshold).to_crs(4326)
    merged_df = pd.concat([merged_df, new_ones], axis=0, ignore_index=True)
    merged_df = gpd.GeoDataFrame(merged_df, crs=4326, geometry='geometry')
    
    #rest = merged_df[~merged_df['geometry'].isin(set(old_df['geometry'].tolist()))].reset_index(drop=True)
    #rest.to_file(paths["new_points_filepath"], driver='GPKG', index=False)
    #merged_df.to_file(paths["corrected_all_filepath"], driver='GPKG', index=False)

def main():
    """Run final merge, confidence handling, OSM correction, and deduplication."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    paths = cfg['paths']
    eu_correction = cfg['eu_correction']
    eu_utm = cfg['eu_utm']
    osm_threshold = cfg['osm_threshold']
    threshold = cfg['threshold']

    # Keep legacy merge logic for compatibility with downstream index assumptions.
    # The newer logic below builds the final dataset used by this project.
    # old merging logic, we keep it here for now, in case we need to go back to it,
    # but the new merging logic is more streamlined and has better handling of the
    # different datasets, so we will use the new merging logic for the final dataset
    # but the old logic has to be run because of paths building on another and index issues
    old_merging_logic(paths, eu_correction, eu_utm, osm_threshold, threshold)

    # reading the old dataset
    old_df = gpd.read_file(paths["seg_corrected_south"])

    # adding extra countries to the merged dataset
    canada_df = pd.read_csv(paths["canada_filepath"], encoding='latin1')
    canada_df['geometry'] = canada_df.apply(lambda row: Point(row['Longitude/ Longitude'], row['Latitude/ Latitude']), axis=1)
    canada_df = gpd.GeoDataFrame(canada_df, geometry='geometry', crs=4326)

    thailand_df = gpd.read_file(paths['thailand_filepath'])
    
    # us and eu have high and low confidence points, we will merge them separately and then combine
    us_df = gpd.read_file(paths['us_new_filepath'])
    high_conf_us, low_conf_us = get_best_points(us_df)

    eu_df = gpd.read_file(paths['eu_new_filepath'])
    high_conf_eu, low_conf_eu = get_best_points(eu_df)

    high_conf = gpd.GeoDataFrame(pd.concat([high_conf_eu, high_conf_us], ignore_index=True), geometry='geometry', crs=4326)
    low_conf = gpd.GeoDataFrame(pd.concat([low_conf_eu, low_conf_us], ignore_index=True), geometry='geometry', crs=4326)

    merged_df = gpd.GeoDataFrame(pd.concat([old_df, high_conf, canada_df, thailand_df], axis=0, ignore_index=True), crs=4326, geometry='geometry')

    # before adding germany, we will find the unmatched points in germany and add them to the merged dataset,
    # this is because the germany dataset have new corrections which we want to add to the merged dataset
    # and we do not want to add the same points twice
    germany_df = gpd.read_file(paths["germany_filepath"])
    germany_df['geometry'] = germany_df.apply(lambda row: Point(row['neigh_lon'],
                                                                 row['neigh_lat']) if pd.notna(row['neigh_lat'])
                                                                   else row['geometry'], axis=1)
    
    new_ones = find_unmatched_targets(merged_df, germany_df, threshold).to_crs(4326)
    merged_df = pd.concat([merged_df, new_ones], axis=0, ignore_index=True)
    merged_df = gpd.GeoDataFrame(merged_df, crs=4326, geometry='geometry')
    
    # for low confidence points, we run a correction with OSM data to try to find better coordinates,
    #  we will add the corrected points to the merged dataset 
    low_conf['epsg'] = low_conf.apply(find_safe_epsg, axis=1)
    pdf = gpd.read_file(paths["osmgeo_filepath"])
    pdf['epsg'] = pdf.apply(find_safe_epsg, axis=1)
    
    low_conf = coordinate_corr_locations_wOSM(osm_threshold, pdf, low_conf)
    low_conf['geometry'] = low_conf['matched_osm_geometry']
    low_conf = low_conf[low_conf['geometry'].notna()]

    merged_df = gpd.GeoDataFrame(pd.concat([merged_df, low_conf], axis=0, ignore_index=True), crs=4326, geometry='geometry')
    del low_conf, low_conf_eu,low_conf_us, high_conf, high_conf_eu, high_conf_us, new_ones, germany_df, canada_df, thailand_df, us_df, eu_df

    # lastly, we do a clustering of nearby points to try to merge duplicates, 
    # we keep the geometry of the point with most filled attributes and 
    # we sum the population served and the wwtp area if they exist
    none_geo_df = merged_df[merged_df.geometry.isna()].reset_index(drop=True)
    merged_df   = merged_df[merged_df.geometry.notna()].reset_index(drop=True)

    merged_df['epsg'] = merged_df.apply(find_safe_epsg, axis=1)
    merged_df = find_meter_coordinates(merged_df)
    merged_df = cluster_points(merged_df, threshold)
    merged_df = gpd.GeoDataFrame(pd.concat([merged_df, none_geo_df], axis=0, ignore_index=True), crs=4326, geometry='geometry')
    merged_df['idx'] = range(0, len(merged_df))

    """     
    merged_df = merged_df[
    merged_df.geometry.notna() & merged_df.is_valid].reset_index(drop=True) 
    """
    
    if "FID" in merged_df:
        merged_df = merged_df.drop(columns=['FID'], errors='ignore')
    if "fid" in merged_df:
        merged_df = merged_df.drop(columns=['fid'], errors='ignore')

    rest = merged_df[~merged_df['geometry'].isin(set(old_df['geometry'].tolist()))].reset_index(drop=True)
    rest.to_file(paths["new_points_filepath"], driver='GPKG', index=False)
    merged_df.to_file(paths["corrected_all_filepath"], driver='GPKG', index=False)

if __name__ == '__main__':
    main()