import os
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import Point, Polygon, LineString, MultiPolygon, MultiLineString, to_wkt, from_wkt
from shapely.geometry.base import BaseGeometry
import duckdb
import pycountry
from ..create_voronoi import estimate_utm_epsg, duckdb_intersect, download_overture_maps, buffer_geometry
from ..starter import load_config

def corr_locations_wOSM(rad, pdf, df):
    # Set spatial index on the OSM geometry
    sindex = pdf.sindex
    matched_geoms = []

    for i in range(len(df)):
        geometry = df.iloc[i]['geometry']
        # Handle invalid or empty geometries
        if geometry is None or geometry.is_empty:
            matched_geoms.append(np.nan)
            continue

        # Buffer around the geometry
        geometry_buffered = geometry.buffer(rad)

        # Find potential matches using bounding box
        fids = list(sindex.intersection(geometry_buffered.bounds))
        min_dist = rad
        closest_geom = None

        for fid in fids:
            candidate_geom = pdf.iloc[fid]['geometry']
            if candidate_geom is None or candidate_geom.is_empty:
                continue
            distance = geometry.distance(candidate_geom)
            if distance < min_dist:
                min_dist = distance
                closest_geom = candidate_geom
        # Append the closest geometry or NaN
        matched_geoms.append(closest_geom if closest_geom is not None and min_dist <= rad else np.nan)
    # Add as new column to df and return
    df = df.copy()  # in case you want to avoid modifying the original df
    df['matched_osm_geometry'] = matched_geoms
    df['matched_osm_geometry'] = df['matched_osm_geometry'].apply(lambda row: 
                                                                  Point(row.centroid.x, row.centroid.y) 
                                                                  if isinstance(row, (Polygon, LineString, MultiPolygon, MultiLineString))
                                                                  else row)
    return df

def coordinate_corr_locations_wOSM(rad, pdf, df):
    all_epsgs = set(pdf['epsg']).union(set(df['epsg']))
    results = []
    for epsg in all_epsgs:
        sub_pdf = pdf[pdf['epsg'] == epsg]
        sub_df = df[df['epsg'] == epsg]
        data = corr_locations_wOSM(rad, sub_pdf.to_crs(epsg), sub_df.to_crs(epsg))
        data = data.to_crs(4326)

        temp_geometry = data['geometry']
        data['geometry'] = data['matched_osm_geometry']
        data = gpd.GeoDataFrame(data, geometry='geometry', crs=epsg)
        data = data.to_crs(4326)

        data['matched_osm_geometry'] = data['geometry']
        data['geometry'] = temp_geometry
        results.append(data)
    return pd.concat(results, ignore_index=True)

def estimate_utm_epsg(lon, lat):
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        raise ValueError("Invalid longitude or latitude")

    zone = int((lon + 180) // 6) + 1
    hemisphere = 'north' if lat >= 0 else 'south'
    epsg = 32600 + zone if hemisphere == 'north' else 32700 + zone
    return epsg

def duckdb_intersect(df, filepath):
    query = "INSTALL SPATIAL; LOAD SPATIAL;"
    query2 = f"""
    WITH 
    data AS (
        SELECT * REPLACE(ST_GeomFromText(geometry)) AS geometry
        FROM df
    ),
    countries AS (
        SELECT * REPLACE(ST_GeomFromWKB(geometry)) AS geometry
        FROM read_parquet('{filepath}')
    )
    SELECT 
        a.* REPLACE(ST_AsText(a.geometry)) AS geometry, 
        b.country AS ISO_2
    FROM data a 
    LEFT JOIN countries b ON 
        ST_Intersects(a.geometry, b.geometry)
    """
    if df is None or df.empty:
        return df
    crs = df.crs
    if crs is not None and df.crs.to_epsg() != 4326:
        df = df.to_crs(epsg=4326)

    df['geometry'] = df['geometry'].map(lambda x: to_wkt(x) if isinstance(x, (Point, LineString, Polygon, MultiLineString, MultiPolygon)) else None)
    duckdb.sql(query)
    df = duckdb.sql(query2).df()
    df['geometry'] = df['geometry'].map(lambda x: from_wkt(x) if not pd.isna(x) else None)
    df = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)
    df['geometry'] = df['geometry'].apply(buffer_geometry)
    return df

def create_HW_geom(row):
    if row['neigh_lon'] == 1 or row['neigh_lon'] == 2:
        return None
    return Point(row['lon'], row['lat'])

def create_corrected_geom(row):
    if pd.notna(row['neigh_lon']):
        if row['neigh_lon'] == 1 or row['neigh_lon'] == 2:
            return Point(row['lon'], row['lat'])
        return Point(row['neigh_lon'], row['neigh_lat'])
    return None

def country_isos():
    alpha_3_to_2 = {}
    alpha_2_to_3 = {}
    alpha_3_to_names = {}
    alpha_2_to_names = {}
    for country in pycountry.countries:
        alpha_3_to_2[country.alpha_3] = country.alpha_2
        alpha_2_to_3[country.alpha_2] = country.alpha_3
        alpha_3_to_names[country.alpha_3] = country.name
        alpha_2_to_names[country.alpha_2] = country.name
    return alpha_2_to_names, alpha_3_to_names, alpha_2_to_3, alpha_3_to_2

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    globals().update(cfg)
    old_filename =  os.path.abspath(os.path.join(paths['data_dir'], 'corrected_WWTP_enhanced.geojson'))

# =============================================================================
    #Cleaning the corrected WWTP locations
    corrected_WWTPs = gpd.read_file(os.path.abspath(paths["paul_corrected_filepath"]))
    corrected_WWTPs['corrected_geometry'] = corrected_WWTPs.apply(create_corrected_geom, axis=1)
    corrected_WWTPs['HW_geometry'] = corrected_WWTPs.apply(create_HW_geom, axis=1)
    corrected_WWTPs['combined_geometry'] = corrected_WWTPs.apply(lambda row: row['corrected_geometry'] if pd.notna(row['corrected_geometry'])
                                                                  and not row['corrected_geometry'].is_empty else row['HW_geometry'], axis=1)
    corrected_WWTPs['used_corrected_geometry'] = corrected_WWTPs.apply(lambda row: (row['neigh_lon'] != 1 or row['neigh_lon'] != 2), axis=1)
    corrected_WWTPs['newly_found_WWTPs'] = corrected_WWTPs.apply(lambda row: pd.isna(row['WASTE_ID']), axis=1)
    #newly_found_WWTPs = corrected_WWTPs[corrected_WWTPs['newly_found_WWTPs']]
    newly_found_WWTPs = corrected_WWTPs[(corrected_WWTPs['newly_found_WWTPs'] == False) & (pd.isna(corrected_WWTPs['neigh_lon']))]
# =============================================================================
    #read ohsome geojson for whole world
    pdf = gpd.read_file(os.path.abspath(paths['osmgeo_filepath']))
    pdf['epsg'] = pdf.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                        if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
    newly_found_WWTPs['geometry'] = newly_found_WWTPs['combined_geometry']
    newly_found_WWTPs['epsg'] = newly_found_WWTPs.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                        if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
    
    newly_found_WWTPs_osm_corrected = coordinate_corr_locations_wOSM(rad, pdf, newly_found_WWTPs)
    newly_found_WWTPs_osm_corrected['geometry'] = newly_found_WWTPs_osm_corrected['matched_osm_geometry']
    #newly_found_WWTPs_osm_corrected = newly_found_WWTPs_osm_corrected.to_crs(4326)
    newly_found_WWTPs_osm_corrected['matched_osm_geometry'] = newly_found_WWTPs_osm_corrected['geometry']
    newly_found_WWTPs_osm_corrected['WWTP_exists'] = newly_found_WWTPs_osm_corrected.apply(lambda row: True if pd.notna(row['matched_osm_geometry']) and not row['matched_osm_geometry'].is_empty else False, axis=1)
    
    #final_df = newly_found_WWTPs_osm_corrected.copy()
    final_df = pd.concat([corrected_WWTPs[~((corrected_WWTPs['newly_found_WWTPs'] == False) & (pd.isna(corrected_WWTPs['neigh_lon'])))],
                           newly_found_WWTPs_osm_corrected], ignore_index=True, axis=0)
    final_df['final_geometry'] = final_df.apply(lambda row: row['matched_osm_geometry'] if pd.notna(row['matched_osm_geometry']) else row['combined_geometry'], axis=1)
    final_df['combined_geometry'] = final_df['combined_geometry'].apply(lambda row: to_wkt(row)  if isinstance(row, Point) else None)
    final_df['HW_geometry'] = final_df['HW_geometry'].apply(lambda row: to_wkt(row) if isinstance(row, Point) else None)
    final_df['matched_osm_geometry'] = final_df['matched_osm_geometry'].apply(lambda row: to_wkt(row) if isinstance(row, BaseGeometry) else None)
    final_df['corrected_geometry'] = final_df['corrected_geometry'].apply(lambda row: to_wkt(row) if isinstance(row, Point) else None)
    final_df['geometry'] = final_df['final_geometry']
    final_df = final_df.drop(labels=['final_geometry'], axis=1).reset_index(drop=True)
    final_df = gpd.GeoDataFrame(final_df, geometry='geometry', crs=4326)
    final_df['geometry'] = final_df.apply(lambda row: row['geometry'] if row['WWTP_exists'] != False else None, axis=1)

    print(list(zip(final_df.columns, final_df.dtypes)))
    #final_df.to_file(output_filename, driver='GeoJSON', index=False)
    notna = final_df[final_df['WASTE_ID'].notna()].drop_duplicates(subset=['WASTE_ID'], keep='first')
    na = final_df[final_df['WASTE_ID'].isna()]
    final_df = pd.concat([notna, na], ignore_index=True)

    if 'ISO_2' not in final_df.columns: 
        if not os.path.exists(os.path.abspath(paths["overture"])):
            download_overture_maps(paths['overture_s3_url'], paths["overture"])
        final_df = duckdb_intersect(final_df, os.path.abspath(paths["overture"]))
        alpha_2_to_names, alpha_3_to_names, alpha_2_to_3, alpha_3_to_2 = country_isos()
        final_df['ISO_2'] = final_df.apply(lambda row: row['ISO_2'] if pd.notna(row['ISO_2']) else alpha_3_to_2.get(row['CNTRY_ISO'], None), axis=1)

    final_df.to_file(os.path.abspath(paths['corrected_south']), driver='GeoJSON', index=False)

    old_file = gpd.read_file(old_filename)
    new_points = final_df[~final_df.geometry.isin(old_file.geometry)]

    new_points.to_file(os.path.abspath(os.path.join(paths['data_dir'], 'missing_WWTPs.geojson')), driver='GeoJSON', index=False)
    print(final_df.columns)
    print(len(final_df), len(pd.isna(final_df['geometry'])))

if __name__ == '__main__':
    main()