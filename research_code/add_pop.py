"""Population Data Integration Module

Intersects population raster data (GeoTIFFs) with geospatial vector data (Voronoi diagrams)
and aggregates population statistics using zonal statistics.

Workflow:
1. Load Voronoi polygon layer from GeoPackage
2. Locate corresponding population raster tiles by country (ISO code lookup)
3. Compute zonal statistics (sum, std) for population within each polygon
4. Add year-specific population columns to original geodataframe
5. Export enhanced geodataframe to GeoPackage with '_pop_added_' prefix

Supports parallel processing across multiple countries and years of population data.
Requires rasterized population TIFFs organized by ISO-3 country codes.
"""

import os
import logging
import sys
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from rasterstats import zonal_stats
import rasterio
import geopandas as gpd
import pandas as pd
import numpy as np
import pycountry
from starter import load_config
from exactextract import exact_extract

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('add_pop.log'),
        logging.StreamHandler()
    ]
)

def get_iso_codes():
    """Build ISO country code lookup tables.
    
    Returns:
        tuple: Four dictionaries mapping between ISO-2/ISO-3 codes and country names:
            - alpha_3_to_2: ISO-3 -> ISO-2 codes
            - alpha_2_to_3: ISO-2 -> ISO-3 codes  
            - alpha_3_to_names: ISO-3 codes -> country names
            - alpha_2_to_names: ISO-2 codes -> country names
    """
    alpha_3_to_2 = {}
    alpha_2_to_3 = {}
    alpha_3_to_names = {}
    alpha_2_to_names = {}
    for country in pycountry.countries:
        alpha_3_to_2[country.alpha_3.upper()] = country.alpha_2.upper()
        alpha_2_to_3[country.alpha_2.upper()] = country.alpha_3.upper()
        alpha_3_to_names[country.alpha_3.upper()] = country.name
        alpha_2_to_names[country.alpha_2.upper()] = country.name
    return alpha_3_to_2, alpha_2_to_3, alpha_3_to_names, alpha_2_to_names

"""
 def intersect_single_file(gdf, tif_paths):
    org_crs = gdf.crs 
    my_dict = {}
    for file in tif_paths:
        if not os.path.exists(file):
            logging.warning(f"Raster file does not exist: {file}")
            continue
        
        basename = os.path.basename(file)
        parts = [int(k) for k in basename.split('_') if k.startswith('20') and len(k) == 4]
        
        if parts:
            year = parts[0]
            my_dict[year] = file
        else:
            logging.warning(f"Could not extract year from filename: {basename} - skipping")
    for year, tif_path in my_dict.items():
        if tif_path is not None and os.path.exists(tif_path):
            try:
                with rasterio.open(tif_path) as src:
                    raster_crs = src.crs
                    nodata_val = src.nodata

                if raster_crs is not None and gdf.crs != raster_crs:
                    gdf = gdf.to_crs(raster_crs)
            
                try:
                    stats = zonal_stats(
                        vectors=gdf,
                        raster=tif_path,
                        stats=["sum", "std"],
                        geojson_out=False,
                        nodata=nodata_val if nodata_val is not None else None
                    )

                    if not stats or len(stats) == 0:
                        logging.warning(f"No zonal statistics returned for {os.path.basename(tif_path)} - geometry may not intersect raster")
                        continue

                    for key in stats[0].keys():
                        values = [
                            s.get(key, np.nan) if s and s.get("sum") is not None else np.nan
                            for s in stats
                        ]
                        numeric_values = [v if isinstance(v, (int, float)) else np.nan for v in values]
                        gdf[f"{str(year)}_zonal_{key}"] = np.maximum(np.asarray(numeric_values), 0)
                except Exception as err:
                    logging.warning(f"Zonal stats returned None for some features — possibly due to invalid geometry or no intersection: {err}")
            except Exception as err:
                logging.warning(f"Problem with rasterio or CRS conversion: {err}")
        else:
            logging.warning(f"Population raster file does not exist: {tif_path}")
    if gdf.crs != org_crs:
        gdf = gdf.to_crs(org_crs)
    return gdf 
"""

def intersect_single_file(gdf, tif_paths, all_years=True):
    """Compute zonal statistics of population rasters within polygons using exactextract.
    
    Args:
        gdf: GeoDataFrame with polygon geometries
        tif_paths: List of population raster file paths (single or multiple years)
        all_years: Boolean indicating whether to process all years or just the first available year
    Returns:
        GeoDataFrame: Input GeoDataFrame with added columns for year-specific population stats
    """
    if gdf is None or gdf.empty:
        return gdf
    
    org_crs = gdf.crs 
    
    # 1. Map files to years
    my_dict = {}
    for file in tif_paths:
        if not os.path.exists(file):
            logging.warning(f"Raster file does not exist: {file}")
            continue
        
        basename = os.path.basename(file)
        parts = [int(k) for k in basename.split('_') if k.startswith('20') and len(k) == 4]
        
        if parts:
            my_dict[parts[0]] = file
        else:
            logging.warning(f"Could not extract year from filename: {basename}")

    # 2. Process each raster
    last_year = sorted(my_dict.keys())[-1]
    for year, tif_path in my_dict.items():
        if not all_years and year != last_year:
            continue
        try:
            # Check CRS match
            with rasterio.open(tif_path) as src:
                raster_crs = src.crs
            
            if raster_crs is not None and gdf.crs != raster_crs:
                gdf = gdf.to_crs(raster_crs)

            # exact_extract is much faster and memory-efficient
            # 'ops' names correspond to stats (sum, stdev, etc.)
            stats_df = exact_extract(
                rast=tif_path,
                vec=gdf,
                ops=['sum', 'stdev'],
                output='pandas' # Returns a tidy dataframe
            )

            # 3. Merge stats back to gdf with year prefix
            # exact_extract returns columns named 'sum' and 'stdev'
            gdf[f"{year}_zonal_sum"] = stats_df['sum'].clip(lower=0).values
            gdf[f"{year}_zonal_std"] = stats_df['stdev'].clip(lower=0).values

            logging.info(f"Processed population for year {year}")

        except Exception as err:
            logging.error(f"Error processing {tif_path}: {err}")

    # 4. Restore original CRS if changed
    if gdf.crs != org_crs:
        gdf = gdf.to_crs(org_crs)
        
    return gdf


def intersect_all_files(gdf, tif_dir, max_workers=16, all_years=True):
    """Intersect population rasters with polygons across all countries.
    
    Parallelizes zonal statistics computation across countries using ProcessPoolExecutor.
    
    Args:
        gdf: GeoDataFrame with 'ISO_2' column indicating country codes
        tif_dir: Root directory containing subdirectories for each country (organized by ISO-3 code)
        max_workers: Maximum number of parallel workers for processing
        
    Returns:
        GeoDataFrame: Concatenated results from all countries with population statistics
    """
    alpha_3_to_2, alpha_2_to_3, alpha_3_to_names, alpha_2_to_names = get_iso_codes()

    tif_filepaths = {}
    for iso_2 in gdf['ISO_2'].unique():
        if iso_2 in alpha_2_to_3:
            iso_3 = alpha_2_to_3[iso_2].lower()
            temp_dir = os.path.join(tif_dir, iso_3)
            if os.path.exists(temp_dir):
                tif_filepath = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.tif')]
                if tif_filepath:
                    tif_filepaths[iso_2] = tif_filepath
            else:
                tif_filepaths[iso_2] = None
        else:
            tif_filepaths[iso_2] = None

    data = []
    countries = gdf['ISO_2'].unique().tolist()
    countries = [c for c in countries if c in tif_filepaths and tif_filepaths[c] is not None]
    random.shuffle(countries)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(intersect_single_file, gdf[gdf['ISO_2'] == iso_2].copy(), tif_filepaths.get(iso_2, []), all_years=all_years)
                    for iso_2 in countries]
        
        # Progress bar for country processing
        for future in tqdm(as_completed(futures), total=len(futures), desc="Intersecting countries"):
            if future is not None:
                try:
                    sub_gdf = future.result()
                    data.append(sub_gdf)
                except Exception as err:
                    logging.warning(f'an error occurred while retrieving gdfs: {err}')
    if data:
        data = pd.concat(data, ignore_index=True)
        data = gpd.GeoDataFrame(data, geometry='geometry', crs=4326)
        return data
    else:
        logging.warning("No data returned from any country - check raster files and polygon-raster intersection")
        return gpd.GeoDataFrame()
    
def orchestrate_intersections(data_dir, tif_dir, output_dir, index, max_workers=16):
    """Orchestrate population data intersection for a single Voronoi file.
    
    Loads a Voronoi polygon layer by index, intersects with population rasters,
    and exports results with '_pop_added_' prefix.
    
    Args:
        data_dir: Directory containing input Voronoi GeoPackage files
        tif_dir: Root directory containing population raster tiles by country
        output_dir: Output directory for enhanced GeoPackages
        index: Zero-based index selecting which Voronoi file to process
        max_workers: Maximum parallel workers for zonal statistics computation
        
    Raises:
        IndexError: If index is out of range for available Voronoi files
        Exception: File I/O or processing errors with detailed logging
    """
    voronoi_files = sorted([os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.gpkg')])
    
    if index >= len(voronoi_files):
        raise IndexError(f"File index {index} out of range (found {len(voronoi_files)} files)")
    
    voronoi_file = voronoi_files[index]
    logging.info(f"Processing file {index+1}/{len(voronoi_files)}: {os.path.basename(voronoi_file)}")
    
    try:
        gdf = gpd.read_file(voronoi_file)
        logging.info(f"Loaded Voronoi layer with {len(gdf)} features")
        
        gdf = intersect_all_files(gdf, tif_dir, max_workers, all_years=True)
        
        output_path = os.path.join(output_dir, f'pop_added_{os.path.basename(voronoi_file)}')
        gdf.to_file(output_path, driver='GPKG', index=False)
        logging.info(f"Successfully saved population-enhanced file to {output_path}")
    except Exception as err:
        logging.error(f"Failed to process {voronoi_file}: {err}", exc_info=True)
        raise

def main():
    """Main entry point for population data integration.
    
    Reads configuration from first_3.load_config(), validates command-line arguments,
    and orchestrates population raster intersection with Voronoi polygon layer.
    
    Command-line arguments:
        sys.argv[1]: Zero-based index of Voronoi file to process
    """
    # Validate command-line arguments
    if len(sys.argv) < 2:
        logging.error("Usage: python add_pop.py <voronoi_file_index>")
        sys.exit(1)
    
    try:
        index = int(sys.argv[1])
    except ValueError:
        logging.error(f"Invalid index {sys.argv[1]}: must be an integer")
        sys.exit(1)
    
    # Setup paths
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    
    # Extract configuration parameters
    paths = cfg.get('paths', {})
    max_workers = cfg.get('add_pop_max_workers', 8)
    
    if not paths:
        logging.error("No 'paths' configuration found. Check config file.")
        sys.exit(1)
    
    required_paths = ['voronoi_dir', 'pop_tif_dir', 'pop_output_dir']
    for path_key in required_paths:
        if path_key not in paths:
            logging.error(f"Missing required path '{path_key}' in configuration")
            sys.exit(1)
    
    # Create output directory
    os.makedirs(paths["pop_output_dir"], exist_ok=True)
    
    logging.info(f"Configuration loaded: voronoi_dir={paths['voronoi_dir']}, "
                f"pop_tif_dir={paths['pop_tif_dir']}, max_workers={max_workers}")
    
    try:
        orchestrate_intersections(
            os.path.abspath(paths['voronoi_dir']),
            os.path.abspath(paths['pop_tif_dir']),
            os.path.abspath(paths['pop_output_dir']),
            index,
            max_workers
        )
        logging.info("Population data integration completed successfully")
    except Exception as err:
        logging.error(f"Population data integration failed: {err}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()






    





