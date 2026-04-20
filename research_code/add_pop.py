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
import rasterio
import geopandas as gpd
import pandas as pd
import pycountry
try:
    from .starter import load_config, parse_config_overrides
except ImportError:  # Support running as a top-level script
    from starter import load_config, parse_config_overrides
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

def intersect_single_file(gdf, tif_paths, all_years=True):
    """Compute zonal statistics of population rasters within polygons using exactextract.
    
    Args:
        gdf: GeoDataFrame with polygon geometries
        tif_paths: List of population raster file paths (single or multiple years)
        all_years: When false, only process the most recent year found in tif_paths.
    Returns:
        GeoDataFrame: Input GeoDataFrame with added columns for year-specific population stats
    """
    if gdf is None or gdf.empty:
        return gdf
    
    org_crs = gdf.crs 
    
    # Map raster files to the year encoded in their filenames.
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

    # Process each raster year and attach exactextract outputs back onto gdf.
    last_year = sorted(my_dict.keys())[-1]
    for year, tif_path in my_dict.items():
        if not all_years and year != last_year:
            continue
        try:
            # Reproject polygons to raster CRS before extracting zonal statistics.
            with rasterio.open(tif_path) as src:
                raster_crs = src.crs
            
            if raster_crs is not None and gdf.crs != raster_crs:
                gdf = gdf.to_crs(raster_crs)

            # exact_extract is the active zonal-statistics backend for this module.
            stats_df = exact_extract(
                rast=tif_path,
                vec=gdf,
                ops=['sum', 'stdev'],
                output='pandas' # Returns a tidy dataframe
            )

            # exact_extract returns canonical 'sum' and 'stdev' columns.
            gdf[f"{year}_zonal_sum"] = stats_df['sum'].clip(lower=0).values
            gdf[f"{year}_zonal_std"] = stats_df['stdev'].clip(lower=0).values

            logging.info(f"Processed population for year {year}")

        except Exception as err:
            logging.error(f"Error processing {tif_path}: {err}")

    # Restore the original CRS expected by downstream writers.
    if gdf.crs != org_crs:
        gdf = gdf.to_crs(org_crs)
        
    return gdf


def intersect_all_files(gdf, tif_dir, max_workers=16, all_years=True):
    """Intersect population rasters with polygons across all countries.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input polygons with an ``ISO_2`` column.
    tif_dir : str
        Root directory containing country raster subdirectories.
    max_workers : int, default=16
        Maximum number of worker processes.
    all_years : bool, default=True
        Whether to attach all discovered raster years instead of only the latest.

    Returns
    -------
    geopandas.GeoDataFrame
        Concatenated result with population statistics attached.
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
    """Run the population-intersection workflow for one Voronoi file.

    Parameters
    ----------
    data_dir : str
        Directory containing input Voronoi GeoPackage files.
    tif_dir : str
        Root directory containing country population rasters.
    output_dir : str
        Directory where population-enriched GeoPackages are written.
    index : int
        Zero-based index of the Voronoi file to process.
    max_workers : int, default=16
        Maximum number of worker processes for zonal statistics.

    Raises
    ------
    IndexError
        If ``index`` is outside the available file range.
    Exception
        If reading, processing, or writing the selected file fails.
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
    """Validate CLI args, load config, and run population enrichment.

    Notes
    -----
    The first positional argument selects the Voronoi file index. Optional
    trailing positionals override ``level``, ``version``, and ``buffer``.
    """
    if len(sys.argv) < 2:
        logging.error("Usage: python -m research_code.add_pop <voronoi_file_index> [level] [version] [buffer] [weight_method] [is_multiplicative]")
        sys.exit(1)
    
    try:
        index = int(sys.argv[1])
    except ValueError:
        logging.error(f"Invalid index {sys.argv[1]}: must be an integer")
        sys.exit(1)
    
    try:
        overrides = parse_config_overrides(start_index=2)
    except ValueError as exc:
        logging.error(str(exc))
        sys.exit(1)
    
    # Switch to the package directory so the relative config path resolves correctly.
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config(**overrides)
    
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
    
    os.makedirs(paths["pop_output_dir"], exist_ok=True)
    
    logging.info(f"Configuration loaded: voronoi_dir={paths['voronoi_dir']}, "
                f"pop_tif_dir={paths['pop_tif_dir']}, max_workers={max_workers}")
    
    try:
        orchestrate_intersections(
            paths['voronoi_dir'],
            paths['pop_tif_dir'],
            paths['pop_output_dir'],
            index,
            max_workers
        )
        logging.info("Population data integration completed successfully")
    except Exception as err:
        logging.error(f"Population data integration failed: {err}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()






    





