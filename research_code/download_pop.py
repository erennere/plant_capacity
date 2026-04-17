"""
Population Data Processing Module

Downloads and processes global population data from WorldPop.
Converts point-based population data (CSV) to raster format (GeoTIFF)
and creates country-level mosaics.

Workflow:
1. Retrieve download URLs for all countries from WorldPop
2. Download population data (rasters or point clouds)
3. For CSVs: rasterize point data to GeoTIFF with UTM projection
4. Mosaic multiple tiles into single country-level GeoTIFF files

Output directories:
- ../data/population/zipped/      : Downloaded archives
- ../data/population/unzipped/    : Extracted files
- ../data/population/rasterized/  : Individual CSV rasterized outputs
- ../data/population/merged/      : Final country mosaics
"""
import requests
import zipfile
import os, re, shutil
from concurrent.futures import as_completed, ProcessPoolExecutor
from tqdm import tqdm
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rasterio.features
from rasterio.merge import merge
from rasterio.windows import from_bounds
from pyproj import CRS
import pycountry
import logging
try:
    from .create_voronoi import estimate_utm_crs
    from .starter import load_config
except ImportError:  # Support running as a top-level script
    from create_voronoi import estimate_utm_crs
    from starter import load_config

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

def extract_first_wildcard(test_string, pattern):
    """Extract first capture group from regex pattern match.
    
    Args:
        test_string: String to search
        pattern: Regex pattern with one capture group
        
    Returns:
        First capture group content or None if no match
    """
    match = re.search(pattern, test_string)
    if match:
        return match.group(1)
    return None

def try_extract_country(name, patterns):
    """Try multiple regex patterns against a filename and return first match."""
    for pattern in patterns:
        result = extract_first_wildcard(name, pattern)
        if result is not None:
            return result
    return None

def add_country_url(country_urls, country, url):
    """Add a URL to the country_urls dict, creating the list if needed."""
    if country in country_urls:
        country_urls[country].append(url)
    else:
        country_urls[country] = [url]

def get_urls_from_hdx():
    from hdx.utilities.easy_logging import setup_logging
    from hdx.api.configuration import Configuration
    from hdx.data.dataset import Dataset
    """
    Fetch population URLs from HDX (Humanitarian Data Exchange) API.
    Note: Currently not used in main workflow, but kept for future reference.
    """
    setup_logging()
    Configuration.create(hdx_site="prod", user_agent="HeiGIT", hdx_read_only=True)
    country_set = set()
    country_urls = {}
    datasets = Dataset.search_in_hdx("Meta Population")

    remaining_countries = {
        'ANR' : ['ANR_men_geotiff.zip', 'ANR_women_geotiff.zip'],
        'AUS' : ['population_aus_northeast_2018-10-01_geotiff.zip','population_aus_northwest_2018-10-01_geotiff.zip',
                'population_aus_southeast_2018-10-01_geotiff.zip','population_aus_southwest_2018-10-01_geotiff.zip'],
        'BRA' : [
            'population_bra_northeast_2018-10-01.geotiff.zip',
            'population_bra_northwest_2018-10-01.geotiff.zip',
            'population_bra_southeast_2018-10-01.geotiff.zip',
            'population_bra_southwest_2018-10-01.geotiff.zip'],
        'DOM' : ['population_dom_2018-10-01.geotiff.zip'],
        'FJI' : ['population_fji_2018-10-01_geotiff.zip'],
        'GIB' : ['gib_men_2020_geotiff.zip', 'gib_women_2020_geotiff.zip'],
        'HKG' : ['population_hkg_2018-10-01_geotiff.zip'],
        'HND' : ['population_hnd_2018-10-01.geotiff.zip'],
        'HTI' : ['population_hti_2018-10-01.geotiff.zip'],
        'IMN' : ['IMN_men_2019-08-03_csv.zip', 'IMN_women_2019-08-03_csv.zip'],
        'IND' : ['population_ind_pak_general.zip'],
        'IRL' : ['population_irl_2019-07-01_geotiff.zip'],
        'JAM' : ['population_jam_2018-10-01.geotiff.zip'],
        'KAZ' : ['population_kaz_2018-10-01_geotiff.zip'],
        'KIR' : ['population_kir_2018-10-01_geotiff.zip'],
        'KNA' : ['population_kna_2018-10-01.geotiff.zip'],
        'KOR' : ['population_kor_2018-10-01_geotiff.zip'],
        'LCA' : ['population_lca_2018-10-01.geotiff.zip'],
        'MEX' : ['population_mex_2018-10-01.geotiff.zip'],
        'MNG' : ['population_mng_2018-10-01_geotiff.zip'],
        'MSR' : ['population_msr_2018-10-01.geotiff.zip'],
        'NCL' : ['population_ncl_2018-10-01_geotiff.zip'],
        'NIC' : ['population_nic_2018-10-01.geotiff.zip'],
        'NPL' : ['population_npl_2018-10-01_geotiff.zip'],
        'NZL' : ['population_nzl_2018-10-01_geotiff.zip'],
        'PAK' : ['population_ind_pak_general.zip'],
        'PAN' : ['population_pan_2018-10-01.geotiff.zip'],
        'PRI' : ['population_pri_2018-10-01.geotiff.zip'],
        'PYF' : ['population_pyf_2018-10-01_geotiff.zip'],
        'REU' : ['population_reu_2018-10-01_geotiff.zip'],
        'SLB' : ['population_slb_2018-10-01_geotiff.zip'],
        'SLV' : ['population_slv_2018-10-01.geotiff.zip'],
        'SVN' : ['population_svn_2019-07-01_geotiff.zip'],
        'SYC' : ['population_syc_2018-10-01_geotiff.zip'],
        'TTO' : ['population_tto_2018-10-01.geotiff.zip'],
        'TUR' : ['population_turkey_2020_tif.zip'],
        'USA' : ['population_usa.part_1_of_6.csv.zip','population_usa.part_2_of_6.csv.zip',
                'population_usa.part_3_of_6.csv.zip','population_usa.part_4_of_6.csv.zip',
                'population_usa.part_5_of_6.csv.zip','population_usa.part_6_of_6.csv.zip'],
        'UZB' : ['population_uzb_2018-10-01_geotiff.zip'],
        'VCT' : ['population_vct_2018-10-01.geotiff.zip'],
        'VGB' : ['population_vgb_2018-10-01.geotiff.zip'],
        'VIR' : ['population_vir_2018-10-01.geotiff.zip'],
        'VUT' : ['population_vut_2018-10-01_geotiff.zip'],
        'WLF' : ['population_wlf_2018-10-01_geotiff.zip'],
        'WSM' : ['population_wsm_2018-10-01_geotiff.zip']
    }

    patterns = [
        r'^population_([a-z]{3})\.geotiff\.zip$',
        r'^(.*?)_general_(.*?)_geotiff\.zip$',
        r'^([a-z]{3})_population_\d{4}_geotiff\.zip$',
        r'^([a-z]{3})_general_.*_csv\.zip$',
        r'^([a-z]{3})_population_\d{4}_csv\.zip$'
    ]
    all_vals_flattened = [v for key, val in remaining_countries.items() for v in val]
    all_vals_flattened_keys  = [key for key, val in remaining_countries.items() for v in val]
    all_vals_dict = {k:v.lower() for k, v in zip(all_vals_flattened, all_vals_flattened_keys)}

    for i, elm in enumerate(datasets):
        for j, res in enumerate(elm.get_resources()):
            url = res.get('download_url')
            name = res.get('name')
            
            # Try to extract country code from filename patterns
            country = try_extract_country(name, patterns)
            
            if country is not None:
                if country in country_set:
                    continue
                country_set.add(country)
                add_country_url(country_urls, country, url)
            elif name in all_vals_flattened:
                iso3 = all_vals_dict[name]
                if iso3 in country_urls:
                    continue
                country_set.add(iso3)
                add_country_url(country_urls, iso3, url)
    return country_urls

def get_urls():
    """
    Generate WorldPop population data URLs for all countries.
    Returns a dictionary mapping country codes to lists of download URLs.
    """
    alpha_2_to_names, alpha_3_to_names, alpha_2_to_3, alpha_3_to_2 = country_isos()
    all_countries = list(alpha_3_to_2.keys())
    country_urls = {k.lower(): [f'https://data.worldpop.org/GIS/Population/Global_2000_2020_1km/2014/{k.upper()}/{k.lower()}_ppp_2014_1km_Aggregated.tif'] for k in all_countries}
    for k in country_urls.keys():
        for year in range(2015, 2025):
            country_urls[k].append(f'https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/{str(int(year))}/{k.upper()}/v1/100m/constrained/{k.lower()}_pop_{str(int(year))}_CN_100m_R2024B_v1.tif')    
    return country_urls

def download_file(url, output_path):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            total_size = int(response.headers.get('Content-Length', 0))
            if total_size == 0:
                chunk_size = 8192
            elif total_size < 10 * 1024 * 1024:
                chunk_size = 8192
            elif total_size < 100 * 1024 * 1024:
                chunk_size = 65536
            else:
                chunk_size = 262144
            print(f"Downloading with chunk size: {chunk_size // 1024} KB")
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk: 
                        f.write(chunk)
        print(f"Saved to {output_path}")
        return True
    except requests.RequestException as e:
        print(f"Download error for {url}: {e}")
        return False

def download_save_and_unzip_pop(url, country, data_dir='../data/population'):
    """Download and extract population data file.
    
    Args:
        url: Download URL for the file
        country: ISO3 country code
        data_dir: Base directory for data storage
        
    Returns:
        Path to extracted folder or None if download/extraction failed
    """
    filename = url.split('/')[-1]
    zip_folder = os.path.join(data_dir, 'zipped')
    extract_folder = os.path.join(data_dir, 'unzipped', country)
    zip_filename = os.path.join(zip_folder, filename)

    os.makedirs(zip_folder, exist_ok=True)
    os.makedirs(extract_folder, exist_ok=True)

    if url.endswith('zip'):
        try:
            response = requests.get(url)
            with open(zip_filename, "wb") as f:
                f.write(response.content)
        except Exception as err:
            logging.warning(f'Download failed for {url}: {err}')
            try:
                logging.info('Attempting fallback URL with maxar_v1')
                response = requests.get(url.replace('BSGM', 'maxar_v1'))
                with open(zip_filename, "wb") as f:
                    f.write(response.content)
            except Exception as err:
                logging.error(f'Fallback download also failed: {err}')
                return None
        try:
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                zip_ref.extractall(extract_folder)
        except Exception as err:
            logging.error(f'Extraction failed for {zip_filename}: {err}') 
            return None
        return extract_folder
    else:
        output_file = os.path.join(extract_folder, os.path.basename(url))
        if not download_file(url, output_file):
            logging.info('Attempting fallback URL with maxar_v1')
            if not download_file(url.replace('BSGM', 'maxar_v1'), output_file):
                return None
        return extract_folder
            
def download_save_and_unzip_pops(country_urls, country, data_dir='../data/population'):
    """Download and extract all population files for a country.
    
    Args:
        country_urls: Dictionary mapping countries to lists of URLs
        country: ISO3 country code
        data_dir: Base directory for data storage
        
    Returns:
        Path to extracted folder or None if all downloads failed
    """
    urls = country_urls[country]
    extract_folder = None
    for url in urls:
        result = download_save_and_unzip_pop(url, country, data_dir)
        if result is not None:
            extract_folder = result
    return extract_folder

def find_type(folder, file_type):
    """Recursively find all files with given extension in folder.
    
    Args:
        folder: Directory path
        file_type: File extension to search for (e.g., '.tif')
        
    Returns:
        List of file paths matching the extension
    """
    result = []
    for entry in os.scandir(folder):
        if entry.is_file() and entry.name.endswith(file_type):
            result.append(entry.path)
        elif entry.is_dir():
            result.extend(find_type(entry.path, file_type))
    return result

def find_files(folder):
    """Find GeoTIFF or CSV files in folder hierarchy.
    
    Args:
        folder: Directory path
        
    Returns:
        Tuple of (file_list, is_tif_format)
        - file_list: List of found .tif or .csv files
        - is_tif_format: True if TIFFs found, False if CSVs
    """
    result = find_type(folder, '.tif')
    if not result:
        return find_type(folder, '.csv'), False
    return result, True
    
def rasterize_csv(df, output_path, res=30):
    """Rasterize point-based population CSV to GeoTIFF.
    
    Converts point locations with population values to a raster grid.
    Projects from lat/lon (EPSG:4326) to UTM and back to ensure accuracy.
    
    Args:
        df: DataFrame with latitude, longitude, and population columns
        output_path: Path to save output GeoTIFF
        res: Raster resolution in meters (default 30)
        
    Returns:
        output_path if successful, None if required columns missing
    """
    df.columns = [c.lower() for c in df.columns]

    try:
        lat_col = next(c for c in df.columns if c.startswith('lat'))
        lon_col = next(c for c in df.columns if c.startswith('lon'))
        pop_col = next(c for c in df.columns if 'pop' in c or 'general' in c)
    except StopIteration:
        logging.error(f"Missing required column in CSV. Available columns: {list(df.columns)}")
        return None

    df[lat_col] = df[lat_col].astype(float)
    df[lon_col] = df[lon_col].astype(float)
    df[pop_col] = df[pop_col].astype(float)

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs="EPSG:4326")
    utm_crs = estimate_utm_crs(gdf)
    gdf_utm = gdf.to_crs(utm_crs)

    minx, miny, maxx, maxy = gdf_utm.total_bounds
    width = int((maxx - minx) / res)
    height = int((maxy - miny) / res)
    transform_utm = from_origin(minx, maxy, res, res)

    shapes = ((geom, value) for geom, value in zip(gdf_utm.geometry, gdf_utm[pop_col]))
    raster_utm = rasterio.features.rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform_utm,
        fill=0,
        all_touched=False,
        dtype='float32'
    )
    dst_crs = "EPSG:4326"
    transform_4326, width_4326, height_4326 = calculate_default_transform(
        utm_crs, dst_crs, width, height, *gdf_utm.total_bounds)

    kwargs = {
        'driver': 'GTiff',
        'height': height_4326,
        'width': width_4326,
        'count': 1,
        'dtype': 'float32',
        'crs': dst_crs,
        'transform': transform_4326
    }

    with rasterio.open(output_path, "w", **kwargs) as dst:
        reproject(
            source=raster_utm,
            destination=rasterio.band(dst, 1),
            src_transform=transform_utm,
            src_crs=utm_crs,
            dst_transform=transform_4326,
            dst_crs=dst_crs,
            resampling=Resampling.nearest
        )
    return output_path

def resample_raster(src, target_transform, target_shape, target_crs, resampling_method=Resampling.nearest):
    """Resample raster to match target transform and shape.
    
    Args:
        src: Rasterio source object
        target_transform: Target affine transform
        target_shape: Target (height, width) shape
        target_crs: Target coordinate reference system
        resampling_method: Resampling algorithm (default: nearest neighbor)
        
    Returns:
        Resampled data array
    """
    data = src.read(1)  # Read the first band (assuming 1 band)
    resampled_data = np.zeros(target_shape, dtype=data.dtype)
    
    reproject(
        data,  # source data
        resampled_data,  # target data
        src_transform=src.transform,  # source transform
        src_crs=src.crs,  # source CRS
        dst_transform=target_transform,  # target transform
        dst_crs=target_crs,  # target CRS
        resampling=resampling_method  # Resampling method
    )
    return resampled_data

def mosaic_large_rasters(raster_files, output_path):
    """Mosaic multiple raster tiles into single output file.
    
    Handles rasters with different resolutions by resampling to common grid.
    
    Args:
        raster_files: List of raster file paths
        output_path: Path to save mosaicked output
    """
    if len(raster_files) == 1:
        shutil.copy(raster_files[0], output_path)
        return
    bounds = []
    crs = None
    dtype = None

    # Collect bounds, CRS, and dtype from input rasters
    for fp in raster_files:
        with rasterio.open(fp) as src:
            bounds.append(src.bounds)
            crs = src.crs
            dtype = src.dtypes[0]

    # Calculate mosaic bounds (bounding box of all rasters)
    minx = min(b.left for b in bounds)
    maxx = max(b.right for b in bounds)
    miny = min(b.bottom for b in bounds)
    maxy = max(b.top for b in bounds)

    # Get resolution from the first raster
    with rasterio.open(raster_files[0]) as src:
        res = src.res
        target_transform = rasterio.transform.from_origin(minx, maxy, res[0], res[1])
        width = int((maxx - minx) / res[0])
        height = int((maxy - miny) / res[1])
        count = src.count

    # Create the profile for the output mosaic
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': count,
        'dtype': dtype,
        'crs': crs,
        'transform': target_transform
    }

    with rasterio.open(output_path, 'w+', **profile) as mosaic:
        mosaic_data = np.zeros((count, height, width), dtype=dtype)
        for fp in raster_files:
            with rasterio.open(fp) as src:
                if src.res != res:
                    # Resample the whole raster
                    resampled_data = resample_raster(
                        src, 
                        target_transform, 
                        (height, width), 
                        crs
                    )
                    mosaic_data[0] += resampled_data
                else:
                    # No resampling, so we calculate window
                    window = from_bounds(*src.bounds, transform=target_transform)
                    window = window.round_offsets().round_lengths()

                    # Read and write only into the window
                    data = src.read(
                        out_shape=(count, window.height, window.width)
                    )
                    temp =  mosaic_data[:, window.row_off:window.row_off + window.height, window.col_off:window.col_off + window.width]
                    if temp.shape != data.shape:
                        data = data[:, int(data.shape[0]-temp.shape[0]):, int(data.shape[1]-temp.shape[1]):]
                    mosaic_data[:,window.row_off:window.row_off + window.height, window.col_off:window.col_off + window.width] += data
        mosaic.write(mosaic_data)

def process_single_country(country_urls, country, res=30, data_dir='../data/population'):
    """Download, process, and mosaic population data for a single country.
    
    Args:
        country_urls: Dictionary mapping countries to lists of URLs
        country: ISO3 country code
        res: Raster resolution in meters (default 30)
        data_dir: Base directory for data storage
    """
    extract_folder = download_save_and_unzip_pops(country_urls, country, data_dir)
    if extract_folder is None:
        logging.warning(f'Failed to download data for {country}')
        return None
    result, if_tif = find_files(extract_folder)

    merged_path = os.path.join(data_dir, 'merged')
    output_path = os.path.join(data_dir, 'rasterized', country)
    if not os.path.exists(merged_path):
            os.makedirs(merged_path, exist_ok=True)
    if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)

    merged_path = os.path.join(merged_path, f'pop_{country}_merged.tif')
    #if os.path.exists(merged_path):
    #    return 
    
    if if_tif:
        mosaic_large_rasters(result, merged_path)
    else:
        filepaths = []
        for index, csv_filepath in enumerate(result):
            part_output_path = os.path.join(output_path, f'pop_{country}_part_{int(index+1)}.tif')
            try:
                df = pd.read_csv(csv_filepath)
                result_path = rasterize_csv(df, part_output_path, res)
                if result_path is not None:
                    filepaths.append(part_output_path)
            except Exception as err:
                logging.error(f'Failed to rasterize {csv_filepath}: {err}')
                continue
        filepaths = [f for f in filepaths if os.path.exists(f)]
        if filepaths:
            mosaic_large_rasters(filepaths, merged_path)
        else:
            logging.error(f'No successfully rasterized files for {country}')

def process_all_countries(country_urls, res=30, max_workers=16, data_dir='../data/population'):
    """Process population data for all countries in parallel.
    
    Args:
        country_urls: Dictionary mapping countries to lists of URLs
        res: Raster resolution in meters (default 30)
        max_workers: Number of parallel workers (default 16)
        data_dir: Base directory for data storage
    """
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_country, country_urls, country, res, data_dir) 
                   for country in country_urls.keys()]
        
        # Progress bar for country processing
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing countries"):
            try:
                future.result()
            except Exception as err:
                logging.error(f'Error processing country: {err}')

def main(res=30, max_workers=8):
    """Main entry point for population data processing.
    
    Args:
        res: Raster resolution in meters (default 30)
        max_workers: Number of parallel workers (default 8)
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    cfg = load_config()
    data_dir = os.path.abspath(cfg["paths"]["pop_dir"])
    
    logging.info('Retrieving population data URLs')
    country_urls = get_urls()
    country_urls = {k: v for k, v in country_urls.items() if k in list(country_urls.keys())[0:3]}
    
    logging.info(f'Processing {len(country_urls)} countries with {max_workers} workers')
    process_all_countries(country_urls, res, max_workers, data_dir)
    logging.info('Population data processing complete')

if __name__ == '__main__':
    main()