"""Correct WWTP locations using OSM features and export merged outputs.

This module combines manually corrected WWTP coordinates with nearby OSM
geometries, performs country enrichment, and writes final GeoJSON artifacts.
"""

import os
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import Point, Polygon, LineString, MultiPolygon, MultiLineString, to_wkt, from_wkt
from shapely.geometry.base import BaseGeometry
import duckdb
try:
    from ..create_voronoi import estimate_utm_epsg, download_overture_maps, buffer_geometry
    from ..starter import load_config
    from ..download_pop import country_isos
except ImportError:
    from research_code.create_voronoi import estimate_utm_epsg, duckdb_intersect, download_overture_maps, buffer_geometry
    from research_code.starter import load_config
    from research_code.download_pop import country_isos

def corr_locations_wOSM(rad, pdf, df):
    """Match each input geometry to the nearest OSM geometry within a radius.

    Args:
        rad: Search radius in units of the active CRS.
        pdf: GeoDataFrame with candidate OSM geometries.
        df: GeoDataFrame with input WWTP geometries to be corrected.

    Returns:
        A copy of df with matched_osm_geometry added.
    """
    # Build spatial index once for fast bounding-box candidate lookups.
    sindex = pdf.sindex
    matched_geoms = []

    for i in range(len(df)):
        geometry = df.iloc[i]['geometry']
        # Handle invalid or empty geometries
        if geometry is None or geometry.is_empty:
            matched_geoms.append(np.nan)
            continue

        # Build local search window and query candidate features via spatial index.
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
    # Convert polygon/line matches to representative points for downstream consistency.
    df = df.copy()  # in case you want to avoid modifying the original df
    df['matched_osm_geometry'] = matched_geoms
    df['matched_osm_geometry'] = df['matched_osm_geometry'].apply(lambda row: 
                                                                  Point(row.centroid.x, row.centroid.y) 
                                                                  if isinstance(row, (Polygon, LineString, MultiPolygon, MultiLineString))
                                                                  else row)
    return df

def coordinate_corr_locations_wOSM(rad, pdf, df):
    """Run OSM matching by EPSG group and merge results.

    Args:
        rad: Search radius in units of each group's projected CRS.
        pdf: GeoDataFrame of OSM candidate geometries with an epsg column.
        df: GeoDataFrame of WWTP geometries with an epsg column.

    Returns:
        A concatenated DataFrame with matched OSM geometries in EPSG:4326.
    """
    all_epsgs = set(pdf['epsg']).union(set(df['epsg']))
    results = []
    for epsg in all_epsgs:
        sub_pdf = pdf[pdf['epsg'] == epsg].copy()
        sub_df = df[df['epsg'] == epsg].copy()
        data = corr_locations_wOSM(rad, sub_pdf.to_crs(epsg), sub_df.to_crs(epsg))
        data = data.to_crs(4326)

        # Temporarily swap active geometry to reproject matched geometries cleanly.
        temp_geometry = data['geometry']
        data['geometry'] = data['matched_osm_geometry']
        data = gpd.GeoDataFrame(data, geometry='geometry', crs=epsg)
        data = data.to_crs(4326)

        data['matched_osm_geometry'] = data['geometry']
        data['geometry'] = temp_geometry
        results.append(data)
    return pd.concat(results, ignore_index=True)

def enrich_country_with_duckdb(df, filepath):
    """Spatially intersect features with country polygons using DuckDB.

    Args:
        df: GeoDataFrame of input features.
        filepath: Parquet path containing country polygons.

    Returns:
        GeoDataFrame in EPSG:4326 with ISO_2 assigned where intersections exist.
    """
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
    df['geometry'] = df['geometry'].map(buffer_geometry)
    return df

def create_HW_geom(row):
    """
    'lon' and 'lat' are HydroWASTE coordinates.
    If 'neigh_lon' is 1 or 2, this indicates that these facilities
    have been recently found, so they are not present in the original HydroWASTE dataset
    and therefore do not have HydroWASTE coordinates.
    We skip them by setting HW geometry to None.
    """
    if row['neigh_lon'] == 1 or row['neigh_lon'] == 2:
        return None
    return Point(row['lon'], row['lat'])

def create_corrected_geom(row):
    """
    Create corrected geometry from neighbor coordinates when available.
    If a near WWTP has been found using some model, this returns corrected geometry
    from 'neigh_lon' and 'neigh_lat' fields.
    If 'neigh_lon' is 1 or 2, this indicates that these facilities have been recently found
    and are not present in the original HydroWASTE dataset, so we get their geometries
    directly from the 'lon' and 'lat' fields.
    Otherwise, it defaults to HydroWASTE geometry; if that is also not available, it returns None.
    """
    if pd.notna(row['neigh_lon']):
        if row['neigh_lon'] == 1 or row['neigh_lon'] == 2:
            return Point(row['lon'], row['lat'])
        return Point(row['neigh_lon'], row['neigh_lat'])
    return None

def main():
    """Run the full WWTP location correction and export pipeline.

    Loads corrected inputs, applies OSM-based matching for unresolved HydroWASTE
    facilities, enriches missing country codes, and writes output GeoJSON files.
    """
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    paths = cfg['paths']
    rad = cfg['rad']
    old_filename =  os.path.abspath(os.path.join(paths['data_dir'], 'corrected_WWTP_enhanced.geojson'))

# =============================================================================
    # Prepare corrected and fallback geometries from manual annotation fields.
    corrected_WWTPs = gpd.read_file(paths["paul_corrected_filepath"])
    corrected_WWTPs['corrected_geometry'] = corrected_WWTPs.apply(create_corrected_geom, axis=1)
    corrected_WWTPs['HW_geometry'] = corrected_WWTPs.apply(create_HW_geom, axis=1)
    corrected_WWTPs['combined_geometry'] = corrected_WWTPs.apply(lambda row: row['corrected_geometry'] if pd.notna(row['corrected_geometry'])
                                                                  and not row['corrected_geometry'].is_empty else row['HW_geometry'], axis=1)
    corrected_WWTPs['used_corrected_geometry'] = corrected_WWTPs.apply(
        lambda row: (row['neigh_lon'] != 1 or row['neigh_lon'] != 2),
        axis=1,
    )
    corrected_WWTPs['hw_WWTPs_wo_correction'] = corrected_WWTPs.apply(lambda row: pd.isna(row['WASTE_ID']), axis=1)
    
    
    # The idea is to find the location of HydroWASTE WWTPs for which no correction
    # has been found (i.e. neigh_lon is null). 
    # This will be cross-checked using OSM data in the next steps.
    hw_WWTPs_wo_correction = corrected_WWTPs[(corrected_WWTPs['hw_WWTPs_wo_correction'] == False) & (pd.isna(corrected_WWTPs['neigh_lon']))]

# =============================================================================
    # Load global OSM candidates and assign local projected CRS IDs for distance checks.
    pdf = gpd.read_file(paths['osmgeo_filepath'])
    pdf['epsg'] = pdf.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                        if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
    hw_WWTPs_wo_correction['geometry'] = hw_WWTPs_wo_correction['combined_geometry']
    hw_WWTPs_wo_correction['epsg'] = hw_WWTPs_wo_correction.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                        if isinstance(row['geometry'], Point) else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y) , axis=1)
    
    hw_WWTPs_wo_correction_osm_corrected = coordinate_corr_locations_wOSM(rad, pdf, hw_WWTPs_wo_correction)
    hw_WWTPs_wo_correction_osm_corrected['geometry'] = hw_WWTPs_wo_correction_osm_corrected['matched_osm_geometry']
    hw_WWTPs_wo_correction_osm_corrected['WWTP_exists'] = hw_WWTPs_wo_correction_osm_corrected.apply(lambda row: True if pd.notna(row['matched_osm_geometry']) and not row['matched_osm_geometry'].is_empty else False, axis=1)
    
    # Merge records corrected by OSM matching back with untouched records.
    final_df = pd.concat([corrected_WWTPs[~((corrected_WWTPs['hw_WWTPs_wo_correction'] == False) & (pd.isna(corrected_WWTPs['neigh_lon'])))],
                           hw_WWTPs_wo_correction_osm_corrected], ignore_index=True, axis=0)
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
    # Keep one row per known WASTE_ID, preserving unnamed/new rows.
    notna = final_df[final_df['WASTE_ID'].notna()].drop_duplicates(subset=['WASTE_ID'], keep='first')
    na = final_df[final_df['WASTE_ID'].isna()]
    final_df = pd.concat([notna, na], ignore_index=True)

    if 'ISO_2' not in final_df.columns: 
        # Fill missing country code via spatial join, then fallback via CNTRY_ISO mapping.
        if not os.path.exists(paths["overture"]):
            download_overture_maps(paths['overture_s3_url'], paths["overture"])
        final_df = enrich_country_with_duckdb(final_df, paths["overture"])
        alpha_2_to_names, alpha_3_to_names, alpha_2_to_3, alpha_3_to_2 = country_isos()
        final_df['ISO_2'] = final_df['ISO_2'].where(
            final_df['ISO_2'].notna(),
            final_df['CNTRY_ISO'].map(alpha_3_to_2),
        )

    final_df = gpd.GeoDataFrame(final_df, geometry='geometry', crs=4326)
    final_df.to_file(paths['corrected_south'], driver='GeoJSON', index=False)

    old_file = gpd.read_file(old_filename)
    new_points = final_df[~final_df.geometry.isin(old_file.geometry)]
    new_points = gpd.GeoDataFrame(new_points, geometry='geometry', crs=4326)

    new_points.to_file(os.path.abspath(os.path.join(paths['data_dir'], 'missing_WWTPs.geojson')), driver='GeoJSON', index=False)
    print(final_df.columns)
    print(len(final_df), len(pd.isna(final_df['geometry'])))

if __name__ == '__main__':
    main()