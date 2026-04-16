import os
import geopandas as gpd
import pandas as pd
from shapely import Point
from .correct_locations_w_OSM import coordinate_corr_locations_wOSM, estimate_utm_epsg
from ..starter import load_config

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

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    globals().update(cfg)

    canada_df = pd.read_csv(os.path.abspath(paths["canada_filepath"]), encoding='latin1')
    canada_df['geometry'] = canada_df.apply(lambda row: Point(row['Longitude/ Longitude'], row['Latitude/ Latitude']), axis=1)
    canada_df = gpd.GeoDataFrame(canada_df, geometry='geometry', crs=4326)

    us_df = pd.read_csv(os.path.abspath(paths["us_filepath"]), encoding='latin1')
    us_df['geometry'] = us_df.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
    us_df = gpd.GeoDataFrame(us_df, geometry='geometry', crs=4326)

    germany_df = gpd.read_file(os.path.abspath(paths["germany_filepath"]))
    germany_df['geometry'] = germany_df.apply(lambda row: Point(row['neigh_lon'],
                                                                 row['neigh_lat']) if pd.notna(row['neigh_lat'])
                                                                   else row['geometry'], axis=1)
    old_df = gpd.read_file(os.path.abspath(paths["seg_corrected_south"]))

    # The EU dataset has to corrected as it seems to contain points that
    #do not correspond to any real location
    eu_df = gpd.read_file(os.path.abspath(paths["eu_ref_filepath"]))
    if eu_correction:
        eu_df['epsg'] = eu_df.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                            if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
        
        pdf = gpd.read_file(os.path.abspath(paths["osmgeo_filepath"]))
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

if __name__ == '__main__':
    main()