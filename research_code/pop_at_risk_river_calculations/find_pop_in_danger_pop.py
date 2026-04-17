import os
from glob import glob
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
import mercantile

try:
    from ..add_pop import intersect_all_files
    from ..create_voronoi import duckdb_intersect
    from ..starter import load_config
except ImportError:
    from research_code.add_pop import intersect_all_files
    from research_code.create_voronoi import duckdb_intersect
    from research_code.starter import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def finding_tiles(polygon, zoom_level):
    """Find all XYZ tiles that intersect a polygon.
    
    Args:
        polygon: Shapely polygon object.
        zoom_level: Zoom level for tile calculation.
    
    Returns:
        List of strings in format 'x-y-z' for all intersecting tiles.
    """
    bbox = polygon.bounds
    tiles = [f'{int(tile.x)}-{int(tile.y)}-{int(tile.z)}' for tile in mercantile.tiles(*bbox, zoom_level)]
    logger.debug(f"Urban area intersects {len(tiles)} tiles at zoom level {zoom_level}")
    return tiles

def find_bbox(tile):
    """Find the bounding box of a tile.
    Args:
        tile: Mercantile tile object in 'x-y-z' string format.
    Returns:
        Bounding box of the tile.
    """
    return box(*mercantile.bounds(*map(int, tile.split('-'))))

def find_tiles_in_a_country(country_polygon, country, zoom_level):
    """Find all XYZ tiles that intersect a country's polygon.
    
    Args:
        country_polygon: Shapely polygon object representing the country.
        zoom_level: Zoom level for tile calculation.
    
    Returns:
        GeoDataFrame with tiles and their corresponding geometries.
    """
    tiles = finding_tiles(country_polygon, zoom_level)
    bboxes = map(find_bbox, tiles)
    logger.info(f"Country intersects {len(tiles)} tiles at zoom level {zoom_level}")
    gdf = gpd.GeoDataFrame({'tile': tiles, 'geometry': bboxes}, crs=4326)
    gdf = gdf.clip(country_polygon)
    gdf['ISO_2'] = country
    return gdf

def find_tiles_in_countries(countries_gdf, zoom_level, max_workers=4):
    """Find all XYZ tiles that intersect multiple countries.
    
    Args:
        countries_gdf: GeoDataFrame with country geometries and ISO_2 codes.
        zoom_level: Zoom level for tile calculation.
    
    Returns:
        GeoDataFrame with tiles and their corresponding geometries for all countries.
    """
    all_tiles = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(find_tiles_in_a_country, row['geometry'], row['ISO_2'], zoom_level) for _, row in countries_gdf.iterrows()]
        for future in as_completed(futures):
            all_tiles.append(future.result())
    if all_tiles:
        logger.info(f"Total tiles found across all countries: {len(all_tiles)}")
        return pd.concat(all_tiles, ignore_index=True)
    else:
        logger.warning("No tiles found for any country.")
        return pd.DataFrame(columns=['tile', 'geometry', 'ISO_2'])
    
def assign_tile_to_df_worker(df, zoom_level):
    df['tile'] = df['geometry'].apply(lambda geom: finding_tiles(geom, zoom_level))
    df = df.explode('tile', ignore_index=True)
    df['geometry'] = df.apply(lambda row: row['geometry'].intersection(find_bbox(row['tile'])) if pd.notna(row['tile']) else row['geometry'], axis=1)
    return df
    
def assign_tile_to_df(df, zoom_level, max_workers=4):
    if df.empty:
        return df

    if max_workers < 1:
        max_workers = 1

    r = max(1, len(df) // max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(assign_tile_to_df_worker, df.iloc[i: min(i + r, len(df))].copy(), zoom_level) for i in range(0, len(df), r)]
        results = [future.result() for future in as_completed(futures)]
    return pd.concat(results, ignore_index=True)

def group_tile_population_sums(df):
    zonal_sum_cols = [col for col in df.columns if col.endswith('_zonal_sum')]
    if 'tile' not in df.columns or not zonal_sum_cols:
        return df

    grouped = df.groupby('tile', as_index=False)[zonal_sum_cols].sum()
    return grouped

def rename_cols(df, radius):
    return df.rename({col: f'{radius}_{col}' for col in df.columns if col not in ['tile', 'geometry']}, axis=1) 

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    cfg = load_config()
    zoom_level = int(cfg['zoom_level'])
    max_workers = 64
    tif_dir = cfg['paths']['pop_tif_dir']

    input_pattern = cfg['paths']['impact_pop_polygons_outpath'].replace('.gpkg', '_*.gpkg')
    input_files = glob(input_pattern)
    results = None
    for input_file in input_files:
        radius = input_file.split('_')[-1].replace('.gpkg', '')
        logger.info(f"Processing impact polygons for radius {radius} from file {input_file}")

        impact_polygons = gpd.read_file(input_file)
        impact_polygons = assign_tile_to_df(impact_polygons, zoom_level, max_workers)
        impact_polygons = duckdb_intersect(impact_polygons, cfg['paths']['overture'])
        impact_polygons.to_file('impact_polygons_tiled.gpkg', index=False, driver='GPKG')
        impact_polygons = intersect_all_files(impact_polygons, tif_dir, int(max_workers/8), all_years=False)
        tile_groups = group_tile_population_sums(impact_polygons)
        del impact_polygons

        if 'tile' not in tile_groups.columns:
            logger.warning("No 'tile' column found after grouping; writing empty output.")
            tile_groups_gdf = gpd.GeoDataFrame(columns=['tile', 'geometry'], geometry='geometry', crs=4326)
        else:
            geoms = tile_groups['tile'].apply(find_bbox)
            tile_groups['geometry'] = geoms
            tile_groups_gdf = gpd.GeoDataFrame(tile_groups, geometry='geometry', crs=4326)

        if results is None:
            tile_groups_gdf = rename_cols(tile_groups_gdf, radius)
            results = tile_groups_gdf
        else:
            tile_groups_gdf = rename_cols(tile_groups_gdf, radius)
            results = pd.merge(results, tile_groups_gdf[[c for c in tile_groups_gdf.columns if c != 'geometry']], on='tile', how='outer')

    results = gpd.GeoDataFrame(results, geometry=results['geometry'], crs=4326)
    results.to_parquet(cfg['paths']['pop_at_risk_output_filepath'], engine='pyarrow', index=False)

if __name__ == '__main__':
    main()
    







    


