"""
Plant Capacity Spatial Data Science Module

Comprehensive geospatial utilities for WWTP (Wastewater Treatment Plant) 
capacity analysis, including coordinate transformations, Voronoi diagrams, 
spatial clustering, and watershed integration.

Key Features:
  - Geometry validation and topology fixing (zero-buffer algorithm)
  - Spatial clustering with Union-Find optimization
  - UTM coordinate transformation and projection estimation
  - Weighted Voronoi diagram generation with multiple distance metrics
  - DuckDB-based spatial indexing for watershed and country integration
  - Buffer dissolution and polygon overlap management
  - Multi-process orchestration for large-scale spatial analysis

Dependencies:
  - geopandas, shapely: Spatial geometry and operations
  - duckdb: SQL-based spatial queries
  - scipy: Spatial indexing and distance metrics
  - pyproj: Coordinate reference system transformations
  
Organization:
  The module is organized into 9 functional sections:
    1. Geometry Validation & Manipulation
    2. Coordinate Transformation & Projection
    3. Spatial Clustering
    4. Grid & Distance Utilities
    5. Data Processing & Normalization
    6. DuckDB & External Data Integration
    7. Buffer & Geometry Dissolution
    8. Voronoi Computation & Orchestration
    9. Configuration & Main Execution
"""

class UnionFind:
    """Efficient union-find data structure with path compression."""
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x):
        """Find root with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x, y):
        """Union two sets by rank."""
        px, py = self.find(x), self.find(y)
        if px == py:
            return
        # Union by rank - attach smaller tree under larger
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1
            
import os, re, logging, sys, yaml
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from collections import defaultdict
from joblib import Parallel, delayed

import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
from tqdm import tqdm 
from pyproj import CRS, Transformer
from rasterio.features import shapes

from scipy.spatial.distance import pdist, squareform
from scipy.spatial import cKDTree
from skimage.measure import find_contours
import cv2

from shapely import Point, Polygon, LineString, MultiPolygon, MultiLineString, box, from_wkt, to_wkb, from_wkb, to_wkt
from shapely.ops import unary_union
from shapely.geometry import shape
import shapely.affinity
import shapely
import duckdb

# Configure module-level logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


################################################################################
# SECTION 1: GEOMETRY VALIDATION & MANIPULATION
################################################################################

def normalize_plane(a, b):
    """
    Normalize 2D coordinates to [0, 1] range based on min/max bounds.
    
    Normalizes both input arrays to a common [0, 1] bounding box for
    distance calculations in normalized space.
    
    Args:
        a (np.ndarray): Array of shape (n, 2) with x,y coordinates
        b (tuple or list): Single (x, y) coordinate pair
        
    Returns:
        tuple: (a_normalized, b_normalized) - Both normalized to [0, 1]
        
    Notes:
        Handles degenerate cases where max == min by using denominator of 1.
        This prevents division by zero in distance calculations.
    """
    b = np.array(b)  # Convert tuple to array
    all_points = np.vstack([a, b.reshape(1, 2)])
    min_vals = np.min(all_points, axis=0)
    max_vals = np.max(all_points, axis=0)
    denom = np.where(max_vals - min_vals == 0, 1, max_vals - min_vals)
    a_norm = (a - min_vals) / denom
    b_norm = (b - min_vals) / denom
    logger.debug(f"Normalized {len(a)} points to [0,1] range")
    return a_norm, b_norm

def is_valid_geom(geom):
    """
    Validate geometry for valid topology and finite coordinates.
    
    Checks that geometry is valid, non-empty, and has finite coordinates.
    Silently catches exceptions during validation.
    
    Args:
        geom: Shapely geometry object (or None)
        
    Returns:
        bool: True if geometry is valid with all finite coordinates, False otherwise
        
    """
    try:
        if geom is None:
            logger.debug("Geometry is None")
            return False
        if not geom.is_valid:
            logger.warning(f"Invalid geometry topology: {geom.geom_type}")
            return False
        coords = list(geom.coords) if hasattr(geom, "coords") else []
        for x, y in coords:
            if not np.isfinite(x) or not np.isfinite(y):
                logger.warning(f"Non-finite coordinates in {geom.geom_type}: x={x}, y={y}")
                return False
        return True
    except Exception as e:
        logger.warning(f"Exception during geometry validation: {e}")
        return False
    
def drop_duplicates(df, col):
    """
    Remove duplicate entries in a DataFrame column while preserving NaN rows.
    
    Drops duplicate rows based on specified column while keeping all
    NaN/None entries intact.
    
    Args:
        df (pd.DataFrame or None): Input dataframe
        col (str): Column name to check for duplicates
        
    Returns:
        pd.DataFrame: DataFrame with duplicates removed (or original if empty/None)
        
    Notes:
        NaN values are always kept and not considered duplicates.
    """
    if df is not None and not df.empty:
        nans = df[df[col].isna()]
        uniques = df[df[col].notna()].drop_duplicates(subset=[col], keep='first')
        df = pd.concat([uniques, nans], ignore_index=True)
    return df

def buffer_geometry(geom):
    """
    Apply topology-fixing zero-buffer to polygon geometries only.
    
    Zero-buffer (geom.buffer(0)) is a standard technique to fix invalid
    polygon topology (self-intersections, wrong ring orientation).
    Point and LineString geometries are returned unchanged.
    
    Args:
        geom: Shapely geometry (Point, Polygon, MultiPolygon, etc.)
        
    Returns:
        Shapely geometry: Same geometry type, with topology fixed (if polygon)
        
    Logs:
        DEBUG: When topology fix is applied to polygon
        WARNING: When geometry type is unknown
        
    Notes:
        POTENTIAL ERROR: geom.buffer(0) on invalid geometry can return
        GEOMETRYCOLLECTION or empty geometry. Should validate output.
    """
    if isinstance(geom, Point):
        return geom
    elif isinstance(geom, (LineString, MultiLineString)):
        return geom
    elif isinstance(geom, (Polygon, MultiPolygon)):
        try:
            buffered = geom.buffer(0)
            return buffered
        except Exception as e:
            logger.warning(f"Error buffering geometry: {e}")
            return geom
    else:
        logger.warning(f"Unknown geometry type in buffer_geometry: {type(geom)}")
        return geom
    
def create_centroid_points(row):
    """
    Extract centroid from geometry in a DataFrame row with validation.
    
    Handles different geometry types:
      - Points: return as-is
      - Polygons/LineStrings: return .centroid (if valid)
      - Other types: return None
    
    Args:
        row (pd.Series): DataFrame row with 'geometry' column
        
    Returns:
        shapely.Point or None: Valid centroid point, original point, or None
        
    Logs:
        WARNING: When computed centroid is invalid
        DEBUG: When unsupported geometry type encountered
        
    Notes:
        VALIDATION: Checks that centroid is valid and non-empty.
        MultiPolygon centroids that are invalid are rejected.
    """
    if 'geometry' not in row:
        logger.warning("Row missing 'geometry' column")
        return None
    geom = row.geometry
    if isinstance(geom, Point):
        return geom
    elif isinstance(geom, (Polygon, LineString, MultiLineString, MultiPolygon)):
        centroid = geom.centroid
        if centroid.is_valid and not centroid.is_empty:
            return centroid
        else:
            logger.warning(f"Invalid centroid for {geom.geom_type} geometry")
            return None
    else:
        logger.debug(f"Unsupported geometry type for centroid: {type(geom).__name__}")
        return None

################################################################################
# SECTION 2: COORDINATE TRANSFORMATION & PROJECTION
################################################################################
    
def utm_stuff(lon, lat):
    """
    Transform WGS84 (EPSG:4326) longitude/latitude to UTM coordinates.
    
    Automatically determines UTM zone from longitude, computes hemisphere from
    latitude, then performs coordinate transformation.
    
    Args:
        lon (float): Longitude in degrees (-180 to 180)
        lat (float): Latitude in degrees (-90 to 90)
        
    Returns:
        tuple: (x_utm, y_utm) - Easting and Northing in meters
        
    Logs:
        DEBUG: When transformer is created and transformation succeeds
        
    Notes:
        INEFFICIENCY: Creating transformer object on every call.
        For batch operations, create once and reuse at module level.
    """
    utm_zone = int((lon + 180) / 6) + 1
    hemisphere = 'north' if lat >= 0 else 'south'
    crs_utm = f"+proj=utm +zone={utm_zone} +{hemisphere} +datum=WGS84 +units=m +no_defs"
    transformer = Transformer.from_crs("EPSG:4326", crs_utm, always_xy=True)
    logger.debug(f"Transforming {lon:.2f}, {lat:.2f} to UTM zone {utm_zone} ({hemisphere})")
    x_utm, y_utm = transformer.transform(lon, lat)
    logger.debug(f"Transformed coordinates: ({x_utm:.2f}, {y_utm:.2f})")
    return (x_utm, y_utm)

################################################################################
# SECTION 3: SPATIAL CLUSTERING
################################################################################

def cluster_point_indices(geoms, threshold):
    """
    Group point geometries into spatial clusters using K-D tree and Union-Find.
    
    Finds all points within 'threshold' distance of each other using
    K-D tree spatial indexing and Union-Find for efficient clustering.
    
    Args:
        geoms (array-like): Iterable of shapely Point geometries
        threshold (float): Distance threshold for clustering (in geometry units)
        
    Returns:
        list[set]: List of point index clusters, each cluster is a set of indices
        
    Logs:
        DEBUG: Summary of clustering results (total points and clusters found)
        
    Notes:
        OPTIMIZED: Uses Union-Find with path compression for O(n log n) complexity
        vs O(n²) worst case with BFS. More efficient for large point sets.
        
        POTENTIAL ERROR: Assumes all geometries are Points with .x, .y attributes.
        Will fail silently if geoms contain other geometry types.
    """
    coords = np.array([(pt.x, pt.y) for pt in geoms])
    tree = cKDTree(coords)
    neighbors = tree.query_ball_point(coords, threshold)
    
    # Use Union-Find to group connected points
    uf = UnionFind(len(coords))
    for i, neighbor_list in enumerate(neighbors):
        for j in neighbor_list:
            if i != j:
                uf.union(i, j)
    
    # Build clusters from union-find groups
    clusters_dict = defaultdict(set)
    for i in range(len(coords)):
        root = uf.find(i)
        clusters_dict[root].add(i)
    
    clusters = list(clusters_dict.values())
    logger.debug(f"Clustered {len(coords)} points into {len(clusters)} clusters (threshold={threshold}m)")
    logger.debug(f"Cluster sizes: min={min(len(c) for c in clusters)}, max={max(len(c) for c in clusters)}, mean={np.mean([len(c) for c in clusters]):.1f}")
    return clusters

def cluster_points(df, threshold):
    """
    Cluster nearby points and assign cluster IDs to DataFrame.
    
    Groups geometries that are within threshold distance using Union-Find
    algorithm after computing pairwise centroid distances. Assigns cluster
    ID to each point based on membership.
    
    Args:
        df (pd.GeoDataFrame): Points with 'geometry' column
        threshold (float): Maximum distance for clustering in same CRS units
        
    Returns:
        pd.GeoDataFrame: Input df with new 'cluster_id' column (integer cluster assignments)
        
    Notes:
        Uses centroid-to-centroid distance for clustering
        Union-Find ensures transitive closure (A~B and B~C → A~C)
    """

    df = df.copy()
    df['num_missing'] = df.isnull().sum(axis=1)

    cluster_sets = cluster_point_indices(df['geometry'], threshold)
    data = []

    for cluster_set in cluster_sets:
        sub_df = df.iloc[list(cluster_set)]
        if len(sub_df) == 1:
            data.append(sub_df)
        else:
            pop_served = sub_df['POP_SERVED'].sum()
            weights = sub_df['weights'].sum()
            # Get the row(s) with the fewest NaNs
            min_missing = sub_df['num_missing'].min()
            best_rows = sub_df[sub_df['num_missing'] == min_missing]
            best_row = best_rows.iloc[0].copy()  # Choose the first one if there's a tie
            best_row['POP_SERVED'] = pop_served
            best_row['weights'] = weights
            data.append(pd.DataFrame([best_row]))

    df = pd.concat(data, ignore_index=True)
    df = df.drop(columns=['num_missing'])
    return df
        
################################################################################
# SECTION 4: GRID & DISTANCE UTILITIES
################################################################################

def create_ranges(x, y, step, min_step=100):
    """
    Create adaptive coordinate range between two values with flexible step size.
    
    Creates a range of coordinates from min(x,y) to max(x,y) using the
    specified step size. If step is too large, adaptively reduces it until
    fitting minimum step size requirement.
    
    Args:
        x, y (float): Boundary coordinates
        step (float): Desired step size (can be adjusted downward)
        min_step (float): Minimum step size threshold (default 1e-6)
        
    Returns:
        np.ndarray: Linear spaced coordinates from min to max
        
    Notes:
        INEFFICIENCY: No max iteration limit on adaptive loop.
        Can cause performance issues if min_step is very small.
    """
    min_val = min(x, y)
    max_val = max(x, y)
    n_range = max_val - min_val

    if n_range == 0:
        return np.array([x, y])

    while True:
        if n_range >= step:
            n_steps = int(np.ceil(n_range / step))
            return np.linspace(min_val, max_val, n_steps + 1)
        else:
            step /= 2
            if step < min_step:
                return np.array([min_val, max_val])
            
def auto_weight_scale(points):
    """
    Compute automatic weight scaling factor based on median nearest neighbor distance.
    
    Calculates the median distance between each point and its nearest neighbor.
    This is used to normalize weights in distance-based weighting functions.
    
    Args:
        points (list or np.ndarray): Array of shape (n, 2) with point coordinates
        
    Returns:
        float: Median of minimum inter-point distances
        
    Notes:
        Used for weight scaling in additive distance functions.
    """
    distances = pdist(points, metric='euclidean')
    distance_matrix = squareform(distances)
    np.fill_diagonal(distance_matrix, np.nan)
    min_dists = np.nanmin(distance_matrix, axis=1)
    median_distance = np.nanmean(min_dists)
    return median_distance

def default_distance_additive(a, b, weight, factor):
    """
    Additive weighted distance function for Voronoi weighting.
    
    Computes distance in normalized space: sqrt(sum((a-b)²) - weight²)
    Acts as a contraction/expansion based on weight values.
    
    Args:
        a (np.ndarray): Grid points array shape (n, 2)
        b (tuple): Single point coordinate (x, y)
        weight (float): Weight parameter (affects distance directly)
        factor (float): Unused parameter (kept for API compatibility with distance_fn interface)
        
    Returns:
        np.ndarray: Weighted distances for all points in a
        
    Notes:
        Result is clipped to minimum 0.01 for numerical stability.
    """
    a, b = normalize_plane(a, b)
    result = np.sum((a - b) ** 2, axis=-1)
    result -= weight**2
    return np.sqrt(np.where(result >= 0, result, 0.01))

def default_distance_multiplicative(a, b, weight, factor):
    """
    Multiplicative weighted distance function for Voronoi weighting.
    
    Computes normalized Euclidean distance scaled inversely by weight.
    weight > 1 contracts the Voronoi region, weight < 1 expands it.
    
    Args:
        a (np.ndarray): Grid points array shape (n, 2)
        b (tuple): Single point coordinate (x, y)
        weight (float): Weight parameter (affects scaling - 1/weight)
        factor (float): Unused in multiplicative metric
        
    Returns:
        np.ndarray: Weighted distances for all points in a
        
    Notes:
        Division by weight means weight=0 causes division error (not handled).
    """
    a, b = normalize_plane(a, b)
    return np.sqrt(np.sum((a - b) ** 2, axis=-1))/weight

def estimate_utm_epsg(lon, lat):
    """
    Estimate UTM EPSG code from WGS84 longitude and latitude.
    
    Determines the appropriate UTM zone and hemisphere from coordinates,
    then returns the corresponding EPSG code for that zone.
    
    Args:
        lon (float): Longitude in degrees (-180 to 180)
        lat (float): Latitude in degrees (-90 to 90)
        
    Returns:
        int: EPSG code for the UTM zone (e.g., 32633 for UTM zone 33N)
        
    Notes:
        UTM zones are 6 degrees wide
        Hemisphere determined by latitude sign (S = southern, N = northern)
        EPSG codes: 32601-32660 for northern, 32701-32760 for southern
    """

    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        logger.error(f"Invalid coordinates: lon={lon}, lat={lat}")
        raise ValueError("Invalid longitude or latitude")

    zone = int((lon + 180) // 6) + 1
    hemisphere = 'north' if lat >= 0 else 'south'
    epsg = 32600 + zone if hemisphere == 'north' else 32700 + zone
    logger.debug(f"Estimated UTM EPSG {epsg} for zone {zone} ({hemisphere}) from lon={lon:.2f}, lat={lat:.2f}")
    try:
        CRS.from_epsg(epsg)
    except Exception as err:
        logger.warning(f"Invalid EPSG {epsg} when estimating UTM: {err}. Falling back to Web Mercator (3857)")
        return 3857
    return epsg

################################################################################
# SECTION 5: DATA PROCESSING & NORMALIZATION
################################################################################

def estimate_utm_crs(gdf):
    """
    Estimate appropriate UTM CRS from GeoDataFrame geometries.
    
    Extracts a valid centroid from geometries and estimates UTM zone.
    Validates the resulting CRS and falls back to Web Mercator if needed.
    
    Args:
        gdf (pd.GeoDataFrame): GeoDataFrame with geometry column
        
    Returns:
        pyproj.CRS: UTM CRS for the region, or Web Mercator EPSG:3857
        
    Logs:
        WARNING: When valid geometries are insufficient or coordinates invalid
        
    Notes:
        Validates both lon and lat are finite using 'and' logic.
    """
    valid_geoms = gdf.geometry[
        gdf.geometry.is_valid & 
        ~gdf.geometry.is_empty & 
        gdf.geometry.notna()
    ]
    
    if valid_geoms.empty:
        logger.warning("No valid geometries available to estimate UTM CRS. Falling back to Web Mercator (3857).")
        return CRS.from_epsg(3857)
    
    centroid = valid_geoms.unary_union.centroid
    lon, lat = centroid.x, centroid.y
    logger.debug(f"Extracted centroid from {len(valid_geoms)} valid geometries: lon={lon:.4f}, lat={lat:.4f}")

    if lon is None or lat is None or not (np.isfinite(lon) and np.isfinite(lat)):
        logger.debug("Initial centroid has non-finite coordinates, searching for valid point geometry...")
        check = True
        for geom in valid_geoms:
            if isinstance(geom, Point):
                lon, lat = geom.x, geom.y
                check = False
                logger.debug(f"Found valid Point geometry: lon={lon:.4f}, lat={lat:.4f}")
                break
            elif isinstance(geom, (Polygon, LineString, MultiPolygon, MultiLineString)):
                lon, lat = geom.centroid.x, geom.centroid.y
                check = False
                logger.debug(f"Found valid {geom.geom_type} with centroid: lon={lon:.4f}, lat={lat:.4f}")
                break
        if check:
            logger.warning("Centroid has non-finite coordinates (inf or NaN). Falling back to Web Mercator (3857).")
            return CRS.from_epsg(3857)

    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    logger.debug(f"Estimated UTM EPSG {epsg} for zone {zone} from centroid")
    try:
        epgs = CRS.from_epsg(epsg)
        return epgs
    except Exception as err:
        logger.warning(f'Failed to create UTM CRS EPSG:{epsg}: {err}. Falling back to Web Mercator (3857)')
        return CRS.from_epsg(3857)

def calculate_area(df, only_round=False):
    """
    Calculate Voronoi region areas from assigned points.
    
    Computes area (in m²) for each Voronoi region based on point assignments.
    Optionally rounds values before calculating to handle overlapping regions.
    
    Args:
        df (pd.GeoDataFrame): Points with 'geometry' (Point), 'weight', and 'WASTE_ID' columns
        only_round (bool): Round weight values before area calculation (default False)
        
    Returns:
        pd.DataFrame: Aggregated by 'WASTE_ID' with 'area_m2' and 'point_count' columns
        
    Notes:
        Assumes 1 point per cell in Voronoi grid
        Area = point_count × cell_size (depends on Voronoi grid resolution)
        Rounding reduces numeric precision but may handle overlaps better
    """

    def round_function(diameters):
        diameters_2 = [float(i) for i in re.findall(r"[-+]?\d*\.\d+|\d+", str(diameters))]
        round_area = np.sum([(d/2)**2 * np.pi for d in diameters_2])
        return round_area
    
    if df is None or df.empty:
        logger.warning("Input dataframe is empty, returning as-is")
        return df
    logger.debug(f"Calculating area for {len(df)} WWTP facilities")
    if 'wwtp_area_rect' in df:
        df['wwtp_area_rect_2'] = df['wwtp_area_rect'].apply(
            lambda x: np.sum([
                float(i) for i in str(x).strip().strip('[]').split() 
                if i and i.lower() != 'none'
            ]) if pd.notnull(x) else 0)
        df['round_area'] = df['diameters'].apply(round_function)
        if only_round:
            df['total_area'] = df['round_area']
            logger.debug(f"Using round areas only for {len(df)} facilities")
        else:
            df['total_area'] = df['round_area'] + df['wwtp_area_rect_2']
            logger.debug(f"Combined round + building areas for {len(df)} facilities")
    else:
        logger.warning("No 'wwtp_area_rect' column found, using default area=1")
        df['total_area'] = 1
    return df

def normalize_column_to_rounded_str(series):
    """
    Convert numeric column values to rounded string IDs for grouping.
    
    Rounds numeric values and converts to strings for use as group keys.
    Handles NaN values by returning NaN. Used for grouping country/buffer IDs.
    
    Args:
        series (pd.Series): Numeric values to normalize
        
    Returns:
        pd.Series: String values rounded to nearest integer, NaN preserved
        
    Notes:
        Potential precision loss when rounding floating-point IDs
    """

    logger.debug(f"Normalizing {len(series)} values to rounded strings")
    numeric = pd.to_numeric(series, errors='coerce')
    rounded = numeric.round(0).astype('Int64')  # Use 'Int64' to allow NaNs
    nans_preserved = rounded.isna().sum()
    logger.debug(f"Conversion complete: {nans_preserved} NaN values preserved")
    return rounded.astype(str)

################################################################################
# SECTION 6: DUCKDB & EXTERNAL DATA INTEGRATION
################################################################################

def download_overture_maps(url, filepath):
    """
    Download and extract Overture Maps data from S3 URL.
    
    Downloads gzip-compressed Overture Maps GeoParquet file and extracts it.
    Creates parent directories if needed. Logs download progress.
    
    Args:
        url (str): S3 URL to gzip-compressed parquet file
        filepath (str): Local path to save extracted parquet file
        
    Returns:
        None (saves file to disk)
        
    Logs:
        INFO: Download start, progress, completion
        WARNING: Connection/download errors
        
    Raises:
        Exception: If download or extraction fails
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    logger.info(f"Starting download of Overture Maps from {url}")
    query = "INSTALL SPATIAL; LOAD SPATIAL;"
    download_query  = f"""
    COPY(
        SELECT * -- REPLACE(ST_AsWKB(geometry)) as geometry
        FROM read_parquet('{url}', filename=true, hive_partitioning=1)
        WHERE subtype = 'country'
    )
    TO '{filepath}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    try:
        logger.debug("Installing and loading DuckDB SPATIAL extension")
        duckdb.sql(query)
        logger.debug(f"Executing download query to save parquet at {filepath}")
        duckdb.sql(download_query)
        logger.info(f"Successfully downloaded Overture Maps country boundaries to {filepath}")
    except Exception as err:
        logger.warning(f"Error downloading country polygons from Overture Maps: {err}")

def process_centroid(args):
    """
    Worker function for parallel watershed centroid matching.
    
    Given a single point centroid, finds which watershed polygon contains it
    using spatial index query. Returns the value from the target column.
    
    Args:
        args (tuple): (centroid, spatial_index, watershed_gdf, column_name)
            - centroid: shapely.Point
            - spatial_index: rtree.index.Index
            - watershed_gdf: pd.GeoDataFrame with polygons
            - column_name: str, column to extract value from
            
    Returns:
        value: Value from watershed_gdf[column_name] or None if no match found
        
    Notes:
        Used in parallel workers for intersect_watershed_sindex()
        Returns None if centroid not in any polygon
    """

    centroid, sidx, watershed, col = args

    if centroid is None or centroid.is_empty or not centroid.is_valid:
        logger.debug(f"Skipping invalid centroid")
        return None

    possible_matches_index = list(sidx.intersection(centroid.bounds))
    if not possible_matches_index:
        logger.debug(f"No spatial index matches found for centroid at {centroid.x:.4f}, {centroid.y:.4f}")
        return None

    possible_matches = watershed.iloc[possible_matches_index]
    possible_matches = possible_matches[possible_matches.is_valid & ~possible_matches.is_empty]

    try:
        precise_matches = possible_matches[possible_matches.intersects(centroid)]
    except Exception:
        precise_matches = gpd.GeoDataFrame(columns=possible_matches.columns)

    if not precise_matches.empty:
        match_value = precise_matches.iloc[0][col]
        logger.debug(f"Found watershed intersection for centroid: {col}={match_value}")
        return match_value
    else:
        logger.debug(f"No precise watershed intersection found for centroid")
        return None       

def intersect_watershed_sindex(df, watershed, col, concurrency=False):
    """
    Intersect dataframe centroids with watershed using spatial indexing.
    
    Finds which watershed contains each point centroid using R-tree
    spatial index for efficiency. Optionally parallelizes via threads.
    
    Args:
        df (pd.GeoDataFrame): Points to intersect with 'geometry' column
        watershed (pd.GeoDataFrame): Watershed polygons with column 'col'
        col (str): Column name in watershed to return
        concurrency (bool): Use ThreadPoolExecutor for parallel processing
        
    Returns:
        pd.GeoDataFrame: Input with new column 'col' containing watershed values
        
    Notes:
        INEFFICIENCY: Processes invalid geometries separately but still
        iterates through them in concat.
        DUPLICATE CONCAT: Uses pd.concat twice.
    """
    if df is None or df.empty:
        return df
    
    # Separate rows with invalid or missing geometry
    nans = df[df['geometry'].isna() | (~df['geometry'].is_valid) | (df['geometry'].is_empty)].copy()
    df = df[df['geometry'].notna() & df['geometry'].is_valid & ~df['geometry'].is_empty].copy()
    utm = df.crs
    # Create centroids safely
    watershed['geometry'] = watershed['geometry'].apply(buffer_geometry) 
    df['geometry'] = df['geometry'].apply(buffer_geometry) 
    df['centroid'] = df['geometry'].centroid

    # Build spatial index on watershed
    sidx = watershed.sindex
    matched_col_values = []
    args_list = [(centroid, sidx, watershed, col) for centroid in df['centroid']]
    if concurrency:
        logger.info(f"Intersecting {len(args_list)} centroids with watershed using ThreadPoolExecutor")
        with ThreadPoolExecutor() as executor:
            matched_col_values = list(executor.map(process_centroid, args_list))
    else:
        logger.info(f"Intersecting {len(args_list)} centroids with watershed (sequential)")
        matched_col_values = [process_centroid(args) for args in args_list]
    matched_count = sum(1 for v in matched_col_values if v is not None)
    logger.debug(f"Successfully matched {matched_count}/{len(args_list)} centroids to watershed")
    df[col] = matched_col_values
    
    df['geometry'] = df['geometry'].apply(buffer_geometry)    
    df = df.drop(columns=['centroid'])
    df = pd.concat([df, nans], ignore_index=True)
    df = gpd.GeoDataFrame(df, geometry='geometry', crs=utm)
    return df

def duckdb_intersect_watershed_single(df, watershed, col):
    """
    Single UTM zone watershed intersection using DuckDB spatial SQL.
    
    Performs spatial intersection of points with watershed polygons
    within a single UTM projection zone using DuckDB for efficiency.
    
    Args:
        df (pd.GeoDataFrame): Points to intersect with 'geometry' column
        watershed (pd.GeoDataFrame): Watershed polygons with 'geometry' and 'col' columns
        col (str): Column name to extract from watershed
        
    Returns:
        pd.GeoDataFrame: Input df with new column 'col' containing watershed values
        
    Notes:
        ASSUMPTION: All input geometries already in same UTM zone
        Uses DuckDB's SPATIAL extension for fast spatial joins
    """
    query = "INSTALL SPATIAL; LOAD SPATIAL;"
    query2 = f"""
    WITH
    data AS (
        SELECT * REPLACE(ST_GeomFromText(centroid)) AS centroid
        FROM df
    ),
    watersheds AS(
        SELECT * REPLACE(ST_GeomFromText(geometry)) AS geometry
        FROM watershed
    )
    SELECT
        a.* REPLACE(ST_AsText(a.centroid)) AS centroid, 
        b.{col}
        FROM data a
        LEFT JOIN watersheds b ON a.ISO_2 = b.ISO_2 
        AND ST_IsValid(a.centroid)
        AND ST_IsValid(b.geometry)
        AND ST_Intersects(a.centroid, b.geometry)
    """
    if df is None or df.empty or watershed is None or watershed.empty:
        return df
    crs = df.crs
    if crs is None:
        df.set_crs(4326)
    if watershed.crs is None:
        watershed = watershed.to_crs(4326)

    utm = df['utm'].mode()[0]
    df = df.to_crs(utm)
    watershed = watershed.to_crs(utm)

    nans = df[df['geometry'].isna() | (~df['geometry'].is_valid) | (df['geometry'].is_empty)].copy()
    df = df[df['geometry'].notna() & df['geometry'].is_valid & ~df['geometry'].is_empty].copy()
    
    df['centroid'] = df.apply(create_centroid_points, axis=1)
    df['centroid'] = df['centroid'].map(lambda x: to_wkt(x) if isinstance(x, (Point, LineString, Polygon, MultiLineString, MultiPolygon)) else None)
    df['geometry'] = df['geometry'].map(lambda x: to_wkt(x) if isinstance(x, (Point, LineString, Polygon, MultiLineString, MultiPolygon)) else None)
    watershed['geometry'] = watershed['geometry'].map(lambda x: to_wkt(x) if isinstance(x, (Point, LineString, Polygon, MultiLineString, MultiPolygon)) else None)
    
    con = None
    temp = f'temp_{int(np.random.randint(0, int(1e12)))}.db'
    logger.info(f"Starting DuckDB watershed intersection for {len(df)} points")
    try:
        con = duckdb.connect(database=temp)
        con.execute(query)
        df = con.execute(query2).df()
        logger.debug(f"Query returned {len(df)} results")
        
        df = df.drop(labels=['centroid'], axis=1)
        df['geometry'] = df['geometry'].map(lambda x: from_wkt(x) if not pd.isna(x) else None)
        df = pd.concat([df, nans], ignore_index=True) 
        df['geometry'] = df['geometry'].apply(buffer_geometry)
        df = gpd.GeoDataFrame(df, geometry='geometry', crs=utm).to_crs(4326)
        return df
    except Exception as err:
        logger.warning(f'Error during DuckDB watershed intersection: {err}')
        return df
    finally:
        if con is not None:
            con.close()
        if os.path.exists(temp):
            os.remove(temp)

def duckdb_intersect_watershed(df, watershed, col, use_duckdb=False, max_workers=16):
    """
    Parallel watershed intersection with automatic UTM zone partitioning.
    
    Partitions data by UTM projection zone, processes each zone in parallel
    using either spatial indexing (default) or DuckDB spatial SQL, then
    concatenates results. Handles invalid geometries separately.
    
    Args:
        df (pd.GeoDataFrame): Points to intersect with 'geometry' column
        watershed (pd.GeoDataFrame): Polygon features with 'geometry' and 'col' columns
        col (str): Column name to extract from watershed (e.g., 'HYBAS_ID')
        use_duckdb (bool): Use DuckDB for spatial queries (default False = use sindex)
        max_workers (int): Number of parallel workers for zone processing
        
    Returns:
        pd.GeoDataFrame: Input with new column 'col' from watershed
        
    Notes:
        Automatically determines UTM zones via estimate_utm_epsg()
        Separates invalid/missing geometries and restores them at end
        Uses ProcessPoolExecutor for parallel zone processing
    """
    nans = df[df['geometry'].isna() | (~df['geometry'].is_valid) | (df['geometry'].is_empty)].copy().reset_index(drop=True) 
    df = df[df['geometry'].notna() & df['geometry'].is_valid & ~df['geometry'].is_empty].copy().reset_index(drop=True)  
    
    df['utm'] = df.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                        if isinstance(row['geometry'], Point)
                                                        else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y),
                                                        axis=1)
    watershed['utm'] = watershed.apply(lambda row: estimate_utm_epsg(row['geometry'].x, row['geometry'].y) 
                                                        if isinstance(row['geometry'], Point)
                                                        else estimate_utm_epsg(row['geometry'].centroid.x, row['geometry'].centroid.y),
                                                        axis=1)
    
    data = []
    unique_utms = set(df['utm'].unique()).union(watershed['utm'].unique())
    func = intersect_watershed_sindex if not use_duckdb else duckdb_intersect_watershed_single
    if not use_duckdb:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(
                func, df[df['utm'] == utm].copy(),
                watershed[watershed['utm'] == utm].copy(), col) for utm in unique_utms]
        for future in as_completed(futures):
            if future is not None:
                try:
                    result = future.result()
                    if result is not None:
                        data.append(result)
                except Exception as err:
                    logging.warning(f'error at retrieving intersections: {err}')
    data.append(nans)
    if data:
        return gpd.GeoDataFrame(pd.concat(data, ignore_index=True), geometry='geometry', crs=4326)
    else:
        return gpd.GeoDataFrame(columns=df.columns)

def duckdb_intersect(df, filepath):
    """
    Intersect point geometries with country boundaries using DuckDB.
    
    Performs spatial join to find country ISO_2 codes for each point
    using bounding box filtering followed by precise intersection test.
    
    Args:
        df (pd.GeoDataFrame): Points with WKT geometry column
        filepath (str): Path to country boundaries parquet file
        
    Returns:
        pd.GeoDataFrame: Input with new 'ISO_2' column from boundaries
        
    Logs:
        INFO: Operation start and result count
        WARNING: Errors during spatial operations
        
    Notes:
        INEFFICIENCY: Uses WKT (20x larger than WKB) for geometry transfer.
    """
    logger.info(f"Starting DuckDB country boundary intersection for {len(df)} points")
    query = "LOAD SPATIAL;"
    query2 = f"""
    WITH 
    data AS (
        SELECT *, 
        ST_XMax(geometry) AS LON_MAX,
        ST_XMin(geometry) AS LON_MIN,
        ST_YMax(geometry) AS LAT_MAX,
        ST_YMin(geometry) AS LAT_MIN
        FROM (
            SELECT * REPLACE(ST_GeomFromText(geometry)) AS geometry
            FROM df
        )
    ),
    countries AS (
        SELECT *, 
        ST_XMax(geometry) AS LON_MAX,
        ST_XMin(geometry) AS LON_MIN,
        ST_YMax(geometry) AS LAT_MAX,
        ST_YMin(geometry) AS LAT_MIN
        FROM ( SELECT * REPLACE(ST_GeomFromWKB(geometry)) AS geometry 
            FROM read_parquet('{filepath}')
        )
    )
    SELECT 
        a.* REPLACE(ST_AsText(a.geometry)) AS geometry, 
        b.country AS ISO_2
    FROM data a 
    LEFT JOIN countries b ON 
        a.LON_MIN >= b.LON_MIN 
        AND a.LON_MAX <= b.LON_MAX 
        AND a.LAT_MIN >= b.LAT_MIN 
        AND a.LAT_MAX <= b.LAT_MAX
        AND ST_Intersects(a.geometry, b.geometry)
    """
    if df is None or df.empty:
        logger.warning("Input dataframe is empty, returning as-is")
        return df
    crs = df.crs
    if crs is not None and df.crs.to_epsg() != 4326:
        logger.debug(f"Transforming from {df.crs.to_epsg()} to EPSG:4326 for intersection")
        df = df.to_crs(epsg=4326)

    logger.debug(f"Converting {len(df)} geometries to WKT format")
    df['geometry'] = df['geometry'].map(lambda x: to_wkt(x) if isinstance(x, (Point, LineString, Polygon, MultiLineString, MultiPolygon)) else None)
    duckdb.sql(query)
    logger.debug(f"Executing DuckDB spatial intersection query")
    df = duckdb.sql(query2).df()
    iso_matched = df['ISO_2'].notna().sum()
    logger.info(f"DuckDB intersection complete: {iso_matched}/{len(df)} points matched to countries")
    df['geometry'] = df['geometry'].map(lambda x: from_wkt(x) if not pd.isna(x) else None)
    df = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)
    df['geometry'] = df['geometry'].apply(buffer_geometry)
    return df

################################################################################
# SECTION 7: BUFFER & GEOMETRY DISSOLUTION
################################################################################

def dissolve_overlapping_geometries(subdf, radius, convex=False, recursion_lim=50000):
    """
    Dissolve overlapping polygon geometries into unified regions.
    
    Groups overlapping geometries using spatial bounds matching (longitude/latitude)
    and connected components analysis, then merges overlapping regions.
    
    Args:
        subdf (pd.GeoDataFrame): Input geometries with 'geometry' column
        radius (float): Buffer radius for convex hull or bounding box expansion
        convex (bool): Use convex hull; else use centroid-based buffering
        recursion_lim (int): Recursion depth limit for DFS (default 50000)
        
    Returns:
        pd.GeoDataFrame: Dissolved geometries with merged overlapping regions
        
    Logs:
        Progress bars via tqdm for longitude/latitude grouping phases
        
    Notes:
        INEFFICIENCY: O(n²) nested loop (lines 953-982) comparing all geometries.
        Creates bounding boxes and sorts multiple times.
        
        Requires 'centroid' computation from create_centroid_points.
        Assumes row index has 'some_id' column (undocumented requirement).
    """
    import sys
    sys.setrecursionlimit(recursion_lim)
    if subdf is None or subdf.empty:
        logger.warning("Input subdf is empty")
        return None
    logger.info(f"Starting dissolution of {len(subdf)} overlapping geometries with radius={radius}")
    utm = estimate_utm_crs(subdf)
    if utm is None:
        logger.warning("Could not estimate UTM CRS")
        return None
    
    subdf = subdf.to_crs(utm)
    subdf = subdf[subdf['geometry'].notna()].reset_index(drop=True)
    subdf['centroid'] = subdf.apply(create_centroid_points, axis=1)
    if convex:
        subdf['geometry'] = subdf['geometry'].apply(lambda c: box(*c.bounds))
    else:
        subdf['geometry'] = subdf['centroid'].apply(lambda c: box(*c.buffer(radius).bounds) if c else None)
    subdf = subdf[subdf['geometry'].notna()].reset_index(drop=True)
    subdf['lat_max'] = subdf['geometry'].apply(lambda geom: geom.bounds[3])  
    subdf['lat_min'] = subdf['geometry'].apply(lambda geom: geom.bounds[1]) 
    subdf['lon_max'] = subdf['geometry'].apply(lambda geom: geom.bounds[2])  
    subdf['lon_min'] = subdf['geometry'].apply(lambda geom: geom.bounds[0])
    subdf['centroid_lon'] = subdf['centroid'].x
    subdf['centroid_lat'] = subdf['centroid'].y

    # Create dictionaries to hold the intersections and the graph structure
    lon_id_dict = {}
    lat_id_dict = {}
    longitude_groups = {}
    latitude_groups = {}

    # Grouping by Longitude 
    subdf = subdf.sort_values(by=['centroid_lon', 'centroid_lat'], ascending=[False, False]).reset_index(drop=True)
    for index, row in tqdm(subdf.iterrows(), total=len(subdf), desc="Processing longitude groups"):
        unique_id = row.some_id
        if unique_id in lon_id_dict:
            continue
        temp_list = [unique_id]
        lon_id_dict[unique_id] = unique_id
        last_row = row

        for second_index, second_row in subdf.iloc[index + 1:].iterrows():
            if last_row.lon_min <= second_row.lon_max:
                if max(last_row.lat_min, second_row.lat_min) <= min(last_row.lat_max, second_row.lat_max):
                    last_row = second_row
                    temp_list.append(second_row.some_id)
                    lon_id_dict[second_row.some_id] = unique_id
            else:
                break
        longitude_groups[unique_id] = set(temp_list)

    # Grouping by Latitude 
    subdf = subdf.sort_values(by=['centroid_lat', 'centroid_lon'], ascending=[False, False]).reset_index(drop=True)
    for index, row in tqdm(subdf.iterrows(), total=len(subdf), desc="Processing latitude groups"):
        unique_id = row.some_id
        if unique_id in lat_id_dict:
            continue
        temp_list = [unique_id]
        lat_id_dict[unique_id] = unique_id
        last_row = row

        for second_index, second_row in subdf.iloc[index + 1:].iterrows():
            if last_row.lat_min <= second_row.lat_max:
                if max(last_row.lon_min, second_row.lon_min) <= min(last_row.lon_max, second_row.lon_max):
                    last_row = second_row
                    temp_list.append(second_row.some_id)
                    lat_id_dict[second_row.some_id] = unique_id
            else:
                break
        latitude_groups[unique_id] = set(temp_list)

    # Create an intersection graph between longitude_groups and latitude_groups
    graph = defaultdict(set)

    # Find intersections between longitude and latitude groups
    for lon_group_id, lon_group in longitude_groups.items():
        for lat_group_id, lat_group in latitude_groups.items():
            intersecting_ids = lon_group.intersection(lat_group)
            if intersecting_ids:
                # Create bidirectional edges between longitude and latitude group IDs
                graph[lon_group_id].add(lat_group_id)
                graph[lat_group_id].add(lon_group_id)

    # Find connected components using DFS
    visited = set()
    components = []

    def dfs(node, component):
        visited.add(node)
        component.add(node)
        for neighbor in graph[node]:
            if neighbor not in visited:
                dfs(neighbor, component)

    # Find connected components
    for node in graph:
        if node not in visited:
            component = set()
            dfs(node, component)
            components.append(component)

    # Now you have `components`, which are the connected components (groups that intersect)
    # Merge the groups in each component into a single set of distinct groups

    final_groups = []
    for component in components:
        merged_group = set()  # To merge all intersecting groups
        for group_id in component:
            if group_id in longitude_groups:
                merged_group.update(longitude_groups[group_id])
            if group_id in latitude_groups:
                merged_group.update(latitude_groups[group_id])
        final_groups.append(merged_group)

    subdf = subdf.drop(labels=['centroid', 'lat_max',
                                'lat_min', 'lon_max',
                                'lon_min', 'centroid_lon',
                                'centroid_lat'], axis=1).reset_index(drop=True)
    subdf = subdf.to_crs(4326) 
    subdf['geometry'] = subdf['geometry'].buffer(0)
    return final_groups, subdf

def dissolve_overlapping_geometries_fast(subdf, radius, convex=False):
    """
    Faster version of the original dissolution logic.
    Uses an R-tree spatial index and Graph Theory instead of nested loops.
    """
    if subdf is None or subdf.empty:
        return [], None

    # 1. Coordinate Projection (UTM is required for distance-based buffering)
    utm = subdf.estimate_utm_crs()
    subdf = subdf.to_crs(utm).reset_index(drop=True)
    
    # 2. Geometry Preparation (Replicating your original logic)
    if convex:
        # Use the bounding box of the original geometry
        subdf['geometry'] = subdf['geometry'].apply(lambda g: box(*g.bounds))
    else:
        # Buffer the centroid and create a bounding box around it
        subdf['geometry'] = subdf.geometry.centroid.buffer(radius).apply(lambda g: box(*g.bounds))

    # 3. Spatial Join to find overlaps (The "Sweep" replacement)
    # This finds every pair of geometries that intersect
    sindex = subdf.sindex
    left_indices, right_indices = sindex.query(subdf.geometry, predicate='intersects')

    # 4. Building the Graph (The DFS replacement)
    # We treat each row index as a node and an intersection as an edge
    g = nx.Graph()
    g.add_edges_from(zip(left_indices, right_indices))

    # 5. Extracting Connected Components
    # This identifies "islands" of overlapping shapes
    components = list(nx.connected_components(g))
    
    final_groups = []
    for component in components:
        # Map the row indices back to your 'some_id' column
        group_ids = set(subdf.iloc[list(component)]['some_id'])
        final_groups.append(group_ids)

    # 6. Cleanup to match original return format
    subdf = subdf.to_crs(4326)
    # Final buffer(0) to ensure valid geometries as per original code
    subdf['geometry'] = subdf['geometry'].buffer(0)

    return final_groups, subdf

def orchestrate_overlaps(df, max_workers, buffers_filepath, radius, convex=False):
    """
    Orchestrate parallel dissolving of overlapping geometries by country.
    
    Processes countries in parallel using ProcessPoolExecutor, applies
    dissolution to each country's geometries, and merges results.
    Caches results to file to avoid recomputation.
    
    Args:
        df (pd.GeoDataFrame): Input geometries with 'ISO_2' country column
        max_workers (int): Number of parallel workers
        buffers_filepath (str): Path to cache dissolved geometries (GPKG format)
        radius (float): Buffer radius for overlap expansion
        convex (bool): Use convex hull for dissolution
        
    Returns:
        pd.GeoDataFrame: Dissolved and aggregated geometries by country
        
    Logs:
        INFO: Operation start and cached file status
        DEBUG: Parallel processing details
        WARNING: Caching failures
        
    Notes:
        Caches results to avoid recomputation on subsequent runs.
        Uses ProcessPoolExecutor which requires picklable functions.
    """
    logger.info(f"Starting parallel dissolution orchestration for {len(df)} geometries")
    if os.path.exists(buffers_filepath):
        logger.info(f"Loading cached dissolved buffers from {buffers_filepath}")
        return gpd.read_file(buffers_filepath)
    
    countries = df['ISO_2'].unique()
    np.random.shuffle(countries)
    logger.debug(f"Processing {len(countries)} countries in parallel with {max_workers} workers")
    df['some_id'] = np.arange(0, len(df))
    #subdf = df.copy()
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(dissolve_overlapping_geometries_fast, #dissolve_overlapping_geometries,
                                    df[df['ISO_2'] == country].copy(), radius, convex
                                   ) for country in countries]
    
    final_groups = []
    dfs = []
    error_count = 0
    for future in as_completed(futures):
        if future is None:
            continue
        try:
            result = future.result()
            if result is None:
                continue
            groups, subdf = result
            if groups:
                final_groups.extend(groups)
            if subdf is not None and not subdf.empty:
                dfs.append(subdf)
        except Exception as err:
            error_count += 1
            logger.warning(f'Error in parallel dissolution: {err}')

    logger.debug(f"Parallel dissolution completed with {error_count} errors")
    if dfs:
        dfs = pd.concat(dfs, ignore_index=True)
    else:
        logger.error("No results from parallel dissolution")
        dfs = pd.DataFrame()
    
    final_dict = {}
    for i, group in enumerate(final_groups):
        for item in group:
            final_dict[int(item)] = i

    dfs['some_id'] = dfs['some_id'].astype(int)
    dfs['group_id'] = dfs['some_id'].map(lambda x: final_dict[x])
    dissolved_buffers = dfs.dissolve(by='group_id').reset_index(drop=True)
    logger.debug(f"Dissolved {len(dfs)} geometries into {len(dissolved_buffers)} groups")
    dissolved_buffers['geometry'] = dissolved_buffers['geometry'].buffer(0)
    if 'centroid' in dissolved_buffers:
        dissolved_buffers = dissolved_buffers.drop(labels='centroid', axis=1)
    try:
        dissolved_buffers.to_file(buffers_filepath, driver='GPKG', index=False)
        logger.info(f"Saved dissolved buffers to {buffers_filepath}")
    except Exception as err:
        logger.warning(f"Failed to cache dissolved buffers: {err}")
    return dissolved_buffers

def optimized_split_geometries_parallel(gdf1, gdf2, n_jobs=-1):
    """
    Split geometries in gdf1 based on intersection with gdf2 in parallel.
    
    For each country in gdf2, intersects gdf1 geometries with country-level
    polygons and splits resulting geometries. Uses joblib for parallelization
    with pre-computed spatial indexes per country.
    
    Args:
        gdf1 (pd.GeoDataFrame): Geometries to split
        gdf2 (pd.GeoDataFrame): Country boundary geometries with 'ISO_2' column
        n_jobs (int): Number of parallel jobs (-1 = use all cores)
        
    Returns:
        pd.GeoDataFrame: Split geometries with country assignments
        
    Notes:
        Pre-computes spatial indexes and country-filtered dataframes
        to minimize redundant computation in parallel jobs.
    """
    # Precompute per-country filtered gdf2 and spatial indexes
    countries = gdf2['ISO_2'].unique()
    logger.debug(f"Precomputing spatial indexes for {len(countries)} countries")
    gdfs = {}
    for country in countries:
        country_df = gdf2[gdf2['ISO_2'] == country].reset_index(drop=True)
        sindex = country_df.sindex
        gdfs[country] = (country_df, sindex)

    logger.info(f"Starting parallel geometry splitting for {len(gdf1)} geometries across {len(countries)} countries")

    def split_geometry(geom, country):
        if country not in gdfs:
            return [{'geometry': geom, 'HYBAS_ID': None, 'ISO_2': None}]
        
        country_df, sindex = gdfs[country]
        possible_matches_index = list(sindex.intersection(geom.bounds))
        possible_matches = country_df.iloc[possible_matches_index]
        if possible_matches.empty:
            return [{'geometry': geom, 'HYBAS_ID': None, 'ISO_2': country}]

        merged_gdf2 = unary_union(possible_matches.geometry)
        geoms = []

        if geom.intersects(merged_gdf2):
            difference = geom.difference(merged_gdf2)
            if not difference.is_empty:
                geoms.append({'geometry': difference, 'HYBAS_ID': None, 'ISO_2': country})

            for _, row_j in possible_matches.iterrows():
                geom_j = row_j['geometry']
                intersection = geom.intersection(geom_j)
                if not intersection.is_empty:
                    geoms.append({'geometry': intersection, 'HYBAS_ID': row_j['HYBAS_ID'], 'ISO_2': country})
        else:
            geoms.append({'geometry': geom, 'HYBAS_ID': None, 'ISO_2': country})
        return geoms

    tasks = [(row['geometry'], row['ISO_2']) for _, row in gdf1.iterrows()]
    results = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(split_geometry)(geom, country) for geom, country in tqdm(tasks, desc="Splitting geometries")
    )
    flat_geometries = [geom for sublist in results for geom in sublist]
    logger.info(f"Splitting complete: {len(gdf1)} input geometries -> {len(flat_geometries)} output geometries")
    return gpd.GeoDataFrame(flat_geometries, geometry='geometry', crs=gdf1.crs)

################################################################################
# SECTION 8: VORONOI COMPUTATION & ORCHESTRATION
################################################################################

def resolve_polygon_overlaps(region_polygons):
    """
    Remove overlapping areas from Voronoi region polygons based on area size.
    
    For each pair of overlapping polygons, removes the intersection area from
    the smaller polygon and keeps it in the larger one. Operates on a copy of
    input geometries to avoid modifying the original GeoDataFrame.
    
    Args:
        region_polygons (pd.GeoDataFrame): GeoDataFrame with 'geometry' column
        
    Returns:
        np.ndarray: Processed geometries with overlaps resolved, same length as input
        
    Logs:
        DEBUG: Start of overlap resolution, completion count
        
    Notes:
        COMPLEXITY: O(n²) nested loop comparing all geometry pairs.
        Iterates through each geometry and removes overlaps with all others.
        
        Operation on geometries:
        - For larger geometry: intersection area is removed via difference()
        - For smaller geometry: intersection area is removed via difference()
        - Preserves the separation to avoid duplicate areas
        
        Assumes valid (non-None) geometries are already filtered.
    """
    logger.debug(f"Starting polygon overlap resolution for {len(region_polygons)} geometries")
    
    # Create working copy of geometries
    non_intersecting_polygons = region_polygons.geometry.to_numpy()
    
    for i, _ in enumerate(region_polygons.geometry):
        geom = non_intersecting_polygons[i]
        if geom is None:
            continue
        
        # Compare with all other geometries
        for j, _ in enumerate(region_polygons.geometry):
            geom_j = non_intersecting_polygons[j]
            # Skip: same geometry, already processed, or invalid geometry
            if i == j or i > j or geom_j is None:
                continue
            
            # Remove overlapping area from smaller geometry
            if geom.area >= geom_j.area:
                geom = buffer_geometry(geom.difference(geom_j))
            else:
                non_intersecting_polygons[j] = buffer_geometry(geom_j.difference(geom))
        
        non_intersecting_polygons[i] = geom
    
    logger.debug(f"Polygon overlap resolution complete: {len(non_intersecting_polygons)} geometries processed")
    return non_intersecting_polygons

def extract_site_coordinates(df, centroid_points, points_col):
    """
    Extract Voronoi site coordinates from dataframe geometries.
    
    Handles two coordinate sources:
      1. Pre-computed points from specified column (converted from WKT, transformed to UTM)
      2. Computed centroids from site geometries (with fallback for complex types)
    
    Args:
        df (pd.GeoDataFrame): Input sites with geometry column
        centroid_points (bool): If False and points_col provided, use pre-computed points
        points_col (str or None): Column name containing pre-computed WKT geometries
        
    Returns:
        list: List of (x, y) coordinate tuples for Voronoi sites
        
    Logs:
        DEBUG: Coordinate extraction method used and result count
        
    Notes:
        PRE-COMPUTED PATH (centroid_points=False, points_col specified):
          - Converts WKT strings to geometries
          - Transforms coordinates to UTM via utm_stuff()
          - Returns list of transformed (x, y) tuples
        
        COMPUTED PATH (centroid_points=True or points_col=None):
          - Extracts centroids from site geometries via create_centroid_points()
          - Handles Point geometries directly (no centroid needed)
          - Handles complex types (LineString, Polygon) by extracting their centroids
          - Falls back to (None, None) placeholder for invalid/missing geometries
          - Returns list of (x, y) tuples
    """
    if not centroid_points and points_col is not None:
        # Use pre-computed point geometries from specified column
        logger.debug(f"Extracting pre-computed site coordinates from column '{points_col}'")
        points = df[points_col].map(lambda geom: from_wkt(geom) if not pd.isna(geom) else None)
        points = [utm_stuff(point.x, point.y) for point in points if point is not None]
        logger.debug(f"Extracted {len(points)} pre-computed points")
    else:
        logger.debug(f"Computing centroids from {len(df)} site geometries")
        points = []
        for geom in df.apply(create_centroid_points, axis=1):
            if isinstance(geom, Point):
                points.append((geom.x, geom.y))
            elif isinstance(geom, (LineString, MultiLineString, Polygon, MultiPolygon)):
                points.append((geom.centroid.x, geom.centroid.y))
            else:
                points.append((None, None))
        logger.debug(f"Computed {len(points)} site centroids")
    
    return points

def initialize_voronoi_weights(df, distance_fn, scale_weights, points):
    """
    Initialize weight parameters for Voronoi computation based on distance function type.
    
    Handles weight setup differently depending on whether additive or multiplicative
    distance metrics are used. Optionally applies weight scaling based on nearest neighbors.
    
    Args:
        df (pd.GeoDataFrame): Input sites with 'weights' column
        distance_fn (callable): Distance function (default_distance_additive or default_distance_multiplicative)
        scale_weights (bool): Whether to apply nearest-neighbor weight scaling
        points (list or np.ndarray): Site coordinates for scaling calculation
        
    Returns:
        tuple: (weights, factor) where:
          - weights (np.ndarray): Computed weight values for each site
          - factor (float): Scaling factor (0 if no scaling applied, otherwise auto-computed)
        
    Logs:
        DEBUG: Weight initialization method and parameters used
        
    Notes:
        ADDITIVE DISTANCE BEHAVIOR:
          - scale_weights=True: Factor computed via auto_weight_scale(points), weights scaled by factor
          - scale_weights=False: Factor=0, weights set to zeros (standard Euclidean, no weight effect)
          - Purpose: Direct weight influence on region size (larger weight = larger region)
        
        MULTIPLICATIVE DISTANCE BEHAVIOR:
          - scale_weights=True: Weights kept as-is from df['weights'], factor=0 (provided weights used as-is)
          - scale_weights=False: Factor=0, weights set to ones (standard Voronoi, equal influence)
          - Purpose: Weights scale the distance metric (weight>1 = site dominates farther)
        
        DEFAULT/UNKNOWN DISTANCE FUNCTION:
          - Assumes multiplicative behavior (same as multiplicative distance)
        
        Weight normalization occurs during Voronoi computation in assign_sites_streaming().
    """
    weights = df['weights'].values.astype(float)
    factor = 0
    
    logger.debug(f"Initializing weights for {len(df)} sites (distance_fn={distance_fn.__name__}, scale_weights={scale_weights})")
    
    if distance_fn == default_distance_additive:
        if scale_weights:
            logger.debug("Additive distance: Computing weight scaling factor")
            factor = auto_weight_scale(points)
            weights = weights * factor
            logger.debug(f"Additive distance: Applied scaling factor {factor:.4f}")
        else:
            logger.debug("Additive distance: Using zero weights (standard Euclidean)")
            weights = np.zeros(len(df))
    elif distance_fn == default_distance_multiplicative:
        if not scale_weights:
            logger.debug("Multiplicative distance: Using equal weights (standard Voronoi)")
            weights = np.ones(len(df))
        else:
            logger.debug("Multiplicative distance: Using provided weights (already normalized in assign_sites_streaming)")
    logger.debug(f"Weight initialization complete: min={weights.min():.4f}, max={weights.max():.4f}, mean={weights.mean():.4f}")
    return weights, factor

def extract_contours_scipy(region_mask_2d, n_points, grid_minx, grid_miny):
    """
    Extract polygon contours from a region mask using scipy.measure.find_contours.
    
    Uses scipy's marching squares algorithm to find contours in a 2D binary mask,
    then converts contours to polygon coordinates in the original coordinate system.
    
    Args:
        region_mask_2d (np.ndarray): 2D boolean array indicating region membership
        n_points (int): Grid resolution (spacing multiplier)
        grid_minx (float): Minimum x coordinate of grid origin
        grid_miny (float): Minimum y coordinate of grid origin
        
    Returns:
        list: List of valid Polygon objects with non-empty boundaries
        
    Logs:
        DEBUG: Number of contours extracted and polygons created
    """
    polygons = []
    contours = find_contours(region_mask_2d, level=0.5, fully_connected='low', positive_orientation='low')
    logger.debug(f"scipy.find_contours extracted {len(contours)} contours")
    
    for contour in contours:
        contour = np.array(contour)
        # Convert from grid indices to actual coordinates
        contour_x = contour[:, 1] * n_points + grid_minx
        contour_y = contour[:, 0] * n_points + grid_miny
        poly_coords = np.stack([contour_x, contour_y], axis=-1).reshape(-1, 2)
        
        # Ensure polygon is closed
        if not np.array_equal(poly_coords[0], poly_coords[-1]):
            poly_coords = np.vstack([poly_coords, poly_coords[0]])
        
        # Create and validate polygon
        poly = Polygon(poly_coords).buffer(0)
        if poly.is_valid and not poly.is_empty:
            polygons.append(poly)
    
    logger.debug(f"scipy contour extraction created {len(polygons)} valid polygons")
    return polygons

def extract_contours_cv2(region_mask_2d, n_points, grid_minx, grid_miny):
    """
    Extract polygon contours from a region mask using OpenCV's contour finding.
    
    Uses cv2.findContours with external contour retrieval to extract boundaries
    from a binary mask, then converts contours to polygon coordinates.
    
    Args:
        region_mask_2d (np.ndarray): 2D boolean array indicating region membership
        n_points (int): Grid resolution (spacing multiplier)
        grid_minx (float): Minimum x coordinate of grid origin
        grid_miny (float): Minimum y coordinate of grid origin
        
    Returns:
        list: List of valid Polygon objects with non-empty boundaries
        
    Logs:
        DEBUG: Number of contours extracted and polygons created
    """
    polygons = []
    mask_uint8 = (region_mask_2d.astype(np.uint8) * 255)
    contours, _ = cv2.findContours(mask_uint8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    logger.debug(f"cv2.findContours extracted {len(contours)} contours")
    
    for contour in contours:
        contour = contour.squeeze()
        # Skip invalid contours
        if contour.ndim != 2 or contour.shape[0] < 3:
            continue
        
        # Convert from grid indices to actual coordinates
        contour_x = contour[:, 0] * n_points + grid_minx
        contour_y = contour[:, 1] * n_points + grid_miny
        poly_coords = np.stack([contour_x, contour_y], axis=-1).reshape(-1, 2)
        
        # Ensure polygon is closed
        if not np.array_equal(poly_coords[0], poly_coords[-1]):
            poly_coords = np.vstack([poly_coords, poly_coords[0]])
        
        # Create and validate polygon
        poly = Polygon(poly_coords).buffer(0)
        if poly.is_valid and not poly.is_empty:
            polygons.append(poly)
    
    logger.debug(f"cv2 contour extraction created {len(polygons)} valid polygons")
    return polygons

def extract_contours_rasterio(region_mask_2d, n_points, grid_minx, grid_miny):
    """
    Extract polygon contours from a region mask using rasterio's feature extraction.
    
    Uses rasterio.features.shapes to convert a raster mask to vector polygons,
    then applies affine transformations to map grid coordinates to actual space.
    
    Args:
        region_mask_2d (np.ndarray): 2D boolean array indicating region membership
        n_points (int): Grid resolution (spacing multiplier)
        grid_minx (float): Minimum x coordinate of grid origin
        grid_miny (float): Minimum y coordinate of grid origin
        
    Returns:
        list: List of valid Polygon objects with non-empty boundaries
        
    Logs:
        DEBUG: Number of polygons created from extracted features
    """
    polygons = []
    results = shapes(region_mask_2d.astype(np.uint8), mask=region_mask_2d > 0)
    for geom, val in results:
        poly = shape(geom)
        # Apply scaling and translation transforms
        poly = shapely.affinity.scale(poly, xfact=n_points, yfact=n_points, origin=(0, 0))
        poly = shapely.affinity.translate(poly, xoff=grid_minx, yoff=grid_miny)
        poly = poly.buffer(0)
        
        if poly.is_valid and not poly.is_empty:
            polygons.append(poly)
    
    logger.debug(f"rasterio contour extraction created {len(polygons)} valid polygons")
    return polygons

def finalize_gdf(df_list, cols):
    """
    Finalize and concatenate list of GeoDataFrames into single output.
    
    Concatenates multiple GeoDataFrames from parallel Voronoi workers,
    applies topology normalization, and returns single combined GeoDataFrame.
    
    Args:
        df_list (list): List of GeoDataFrames to concatenate
        cols (pd.Index): Column names for empty fallback DataFrame
        
    Returns:
        pd.GeoDataFrame: Concatenated GeoDataFrame with geometry column, WGS84 CRS
        
    Notes:
        - If df_list is empty, returns empty GeoDataFrame with specified columns
        - Applies buffer_geometry() to normalize topology of all geometries
        - Sets CRS to EPSG:4326 (WGS84)
    """
    if len(df_list) > 0:
        df = pd.concat(df_list, ignore_index=True)
    else:
        # fallback: empty GeoDataFrame with geometry column
        df = pd.DataFrame(columns=cols)
    df = gpd.GeoDataFrame(df, geometry='geometry', crs='epsg:4326')
    if len(df) > 0:
        df['geometry'] = df['geometry'].apply(buffer_geometry)
    return df

def assign_sites_streaming(valid_points, points, weights, distance_fn, factor):
    """
    Assign each grid point to nearest Voronoi site using weighted distance.
    
    Computes distance from each grid point to all sites using the provided
    distance function, tracking minimum distance site for streaming computation.
    Supports multiplicative and additive weighting schemes.
    
    Args:
        valid_points (np.ndarray): Grid points shape (n, 2) to assign
        points (np.ndarray): Site locations shape (m, 2)
        weights (np.ndarray): Weight values for each site shape (m,)
        distance_fn (callable): Distance function signature (points, site, weight, factor)
        factor (float or None): Scale factor passed to distance_fn
        
    Returns:
        tuple: (assignments, min_distances)
            - assignments: np.ndarray shape (n,) with site indices (0 to m-1)
            - min_distances: np.ndarray shape (n,) with minimum distances
            
    Notes:
        Streaming: computes minimum distance on-the-fly without storing full distance matrix
        Reduces memory overhead for large grids or many sites
    """

    n_points = valid_points.shape[0]
    best_distances = np.full(n_points, np.inf)
    assignments = np.full(n_points, -1, dtype=int)
    logger.debug(f"Assigning {n_points} grid points to {len(points)} sites")

    weights = weights/np.sum(weights)
    for idx, (site, weight) in enumerate(zip(points, weights)):
        dist = distance_fn(valid_points, site, weight, factor)
        mask = dist < best_distances
        best_distances[mask] = dist[mask]
        assignments[mask] = idx
    
    assigned_count = np.sum(assignments >= 0)
    logger.debug(f"Assignment complete: {assigned_count}/{n_points} points assigned")
    return assignments

def weighted_voronoi(df, col, country_clip, scale_weights=False, clipping=None, n_points=100, distance_fn=default_distance_multiplicative,
                      scipy_true=False, cv2_true=False, centroid_points=False, points_col=None, buffering=False, buffer=10000, threshold=500):
    """
    Generate weighted Voronoi diagram from point sites with multiple contour methods.
    
    Creates Voronoi regions using weighted distance metrics. Generates grid of points,
    assigns each to nearest site, then extracts contours using scipy/cv2/rasterio helper
    functions (extract_contours_scipy, extract_contours_cv2, extract_contours_rasterio).
    
    Args:
        df (pd.GeoDataFrame): Point sites with 'geometry', 'weights', 'WASTE_ID' columns
        col (str): Column to identify sites (for grouping if needed)
        country_clip (pd.GeoDataFrame): Country boundary for clipping
        scale_weights (bool): Scale weights by distance to nearest neighbor
        clipping (pd.GeoDataFrame): Additional clipping boundary
        n_points (int): Grid resolution (n_points x n_points grid)
        distance_fn (callable): Distance function for weighting
        scipy_true (bool): Use scipy.measure.find_contours for contour extraction
        cv2_true (bool): Use cv2 for contour extraction
        centroid_points (bool): Use pre-computed centroids instead of geometry
        points_col (str or None): Column with point geometries to use
        buffering (bool): Apply buffer intersection to regions
        buffer (float): Buffer radius if buffering=True
        threshold (float or int): Clustering distance threshold
        
    Returns:
        tuple: (df_waste, region_polygons, point_df) GeoDataFrames
        
    Logs:
        Progress tracking and region assignment details via DEBUG/INFO levels
        
    Notes:
        REFACTORED: Contour extraction logic moved to separate helper functions
        for improved testability and maintainability. See extract_contours_scipy(),
        extract_contours_cv2(), and extract_contours_rasterio().
        
        POTENTIAL ERROR: Special case for single site (len(df)==1):
        Returns region based on buffer or clipping geometry, not true Voronoi.
        
        MULTIPLE CONCAT: Uses pd.concat multiple times, potentially
        duplicating NaN rows.
    """

    if df is None or df.empty:
        logger.warning("Input dataframe for weighted_voronoi is empty")
        return
    
    logger.info(f"Starting weighted Voronoi generation for {len(df)} sites (n_points={n_points})")
    
    # === PHASE 1: CRS VALIDATION & PROJECTION ===
    # Ensure all geometries have a proper coordinate system
    # The Projection to UTM must have happened in the previous steps, 
    # but we check again here to be safe
    if df.crs is None:
        df = df.set_crs('epsg:4326')
        utm = estimate_utm_crs(df)
        if utm is None:
            logger.warning("Could not estimate UTM CRS for Voronoi")
            return
        df = df.to_crs(utm)
        logger.debug(f"Projected to UTM CRS: {utm}")
    crs = df.crs
    
    # === PHASE 2: SITE PREPROCESSING ===
    # Cluster nearby points to avoid creating duplicate Voronoi regions
    # Points within 'threshold' distance are merged together with aggregate properties
    df = gpd.GeoDataFrame(cluster_points(df, threshold), geometry='geometry', crs=crs)
    logger.debug(f"After clustering: {len(df)} sites")

    # The clipping object might be a collection of objects so we unify them into a single geometry
    # We also make sure that the clipping geometry is in the same CRS as the sites
    # IF a clipping object is provided, we will use it
    # Otherwise, we will use the bounding box of the sites with a buffer as the clipping geometry
    # We will use the same buffer for the creation of the grid geometry for Voronoi computation
    # Because using the actual clipping geometry for the grid creation will increase the
    #  computational cost significantly if the clipping geometry is significantly bigger than the
    # area of interest [(O(nm)) where n and m are the dimensions of the clipping geometry]

    buffered = df.buffer(buffer)
    minx, miny, maxx, maxy = buffered.total_bounds
    
    # === PHASE 3: GRID EXTENT FROM BUFFERED BOUNDS ===
    # Determine the bounding box for the Voronoi grid computation
    # If clipping geometry provided: use it as actual_clipping_object
    # If no clipping: use buffered bounding box as actual_clipping_object
    # Grid always uses buffered bounding box (minx, miny, maxx, maxy) for computation
    actual_clipping_object = None
    if clipping is not None and not clipping.empty:
        if clipping.crs is None:
            clipping = clipping.set_crs('epsg:4326')
        if clipping.crs != crs:
            clipping = clipping.to_crs(crs)
        actual_clipping_object = buffer_geometry(unary_union(clipping.geometry))
    else:
        actual_clipping_object = buffer_geometry(box(minx, miny, maxx, maxy))
    
    # === PHASE 4: COUNTRY CRS ALIGNMENT ===
    # Ensure country clipping geometry is aligned with site CRS
    # This is critical for accurate clipping during final boundary operations
    if country_clip is not None:
        if country_clip.crs is None:
            country_clip = country_clip.set_crs('epsg:4326')
        if  country_clip.crs != crs:
            country_clip = country_clip.to_crs(crs)
    
    if len(df) == 1:
        # === SPECIAL CASE: SINGLE SITE ===
        # When only one site exists, create Voronoi region from clipping boundary
        # This is NOT a true Voronoi diagram, but the site's service area
        region_polygons = pd.DataFrame({'WASTE_ID':[df.iloc[0]['WASTE_ID']], 
                'geometry':[buffered.geometry.values[0]]})
        # Merge site attributes with region geometry
        region_polygons = pd.merge(region_polygons, df.drop(['geometry'], axis=1), on=['WASTE_ID']) 
        region_polygons = gpd.GeoDataFrame(region_polygons, geometry='geometry', crs=crs)
        
        geom = df.iloc[0]['geometry']
        if isinstance(geom, (Point, Polygon, MultiPolygon, LineString, MultiLineString)):
            region_polygons.loc[0, 'geometry'] = buffer_geometry(region_polygons.loc[0, 'geometry'])

        # Optionally intersect region with buffer around site point
        if buffering:
            point_buffer = geom.centroid.buffer(buffer)
            region_polygons.loc[0, 'geometry'] = region_polygons.loc[0, 'geometry'].intersection(point_buffer).buffer(0)

        # Filter sites that appear in final regions
        point_df = df[df[col].isin(region_polygons[col])].reset_index(drop=True)
        df_waste = drop_duplicates(df, 'WKT_WWTP')

        # Clip region to actual clipping geometry to ensure it does not exceed bounds
        region_polygons = gpd.clip(region_polygons, actual_clipping_object)
        region_polygons['geometry'] = region_polygons['geometry'].map(buffer_geometry)
        
        # Apply country boundary clipping if provided
        if country_clip is not None and not country_clip.empty:
            region_polygons = gpd.clip(region_polygons, country_clip)
        region_polygons['geometry'] = region_polygons['geometry'].map(buffer_geometry)

        # Convert all outputs to WGS84 for standard output format
        df_waste = df_waste.to_crs(4326)
        region_polygons = region_polygons.to_crs(4326)
        point_df = point_df.to_crs(4326)
        return df_waste, region_polygons, point_df

    # === PHASE 5: SITE COORDINATES EXTRACTION ===
    # Extract point locations for Voronoi site assignment
    # Points are either pre-computed from points_col or computed as centroids
    points = extract_site_coordinates(df, centroid_points, points_col)

    # === PHASE 6: GRID GENERATION ===
    # Use adaptive step sizing to ensure reasonable coverage
    x_coords = create_ranges(minx, maxx, n_points)
    y_coords = create_ranges(miny, maxy, n_points)
    # Create 2D mesh grid
    xv, yv = np.meshgrid(x_coords, y_coords)
    # Flatten to list of (x, y) coordinate pairs
    grid_points = np.stack([xv, yv], axis=-1).reshape(-1, 2)
    grid_minx = x_coords[0]
    grid_miny = y_coords[0]

    # === PHASE 7: WEIGHT INITIALIZATION ===
    # Set up weight parameters based on distance function type
    weights, factor = initialize_voronoi_weights(df, distance_fn, scale_weights, points)
        
    # === PHASE 8: GRID MASKING & SITE ASSIGNMENT ===
    # Filter grid points to include only those within the the clipping boundary to optimize assignment
    # Extract only points that are inside the clipping boundary
    # Assign each grid point to its nearest weighted site
    # This is the core Voronoi computation step
    mask = np.array([actual_clipping_object.contains(Point(p)) for p in grid_points])
    valid_points = grid_points[mask]
    assignments = assign_sites_streaming(valid_points, points, weights, distance_fn, factor)
    
    # === PHASE 9: REGION BOUNDARY EXTRACTION ===
    # Build Voronoi region polygon for each site
    region_polygons = []
    df.reset_index(drop=True, inplace=True)
    for point, (i, row) in zip(points, df.iterrows()):
        # Extract grid points assigned to this site
        region_points = valid_points[assignments == i]
        if len(region_points) == 0:
            # No points assigned to this site: create empty region placeholder
            region_polygons.append({'WASTE_ID':row['WASTE_ID'], 'geometry':None})
            continue

        # === CONTOUR EXTRACTION ===
        # Create 2D binary mask indicating which grid points belong to this site
        # Reshape 1D mask back to 2D grid for contour detection
        region_mask = np.zeros_like(mask, dtype=bool)
        region_mask[np.where(mask)[0][assignments == i]] = True
        region_mask_2d = region_mask.reshape(len(y_coords), len(x_coords))
        
        # Extract contours (region boundaries) from binary mask using selected method:
        # Different contour extraction algorithms have different speed/accuracy tradeoffs
        if scipy_true:
            # scipy marching squares: smooth contours, good for analysis
            polygons = extract_contours_scipy(region_mask_2d, n_points, grid_minx, grid_miny)
        elif cv2_true:
            # OpenCV contours: fast, good edge detection
            polygons = extract_contours_cv2(region_mask_2d, n_points, grid_minx, grid_miny)
        else:
            # rasterio shapes (default): standard raster-to-vector conversion
            polygons = extract_contours_rasterio(region_mask_2d, n_points, grid_minx, grid_miny)

        if polygons:
            polygons = buffer_geometry(unary_union(polygons))
            # Optionally intersect region with buffer around site for local influence zone
            if buffering:
                point_buffer = Point(point).buffer(buffer)
                polygons = polygons.intersection(point_buffer).buffer(0)
            region_polygons.append({'WASTE_ID':row['WASTE_ID'], 'geometry': polygons})
        else:
            # No contours found for this site
            region_polygons.append({'WASTE_ID':row['WASTE_ID'], 'geometry': None})

    # === PHASE 10: GEODATAFRAME CONVERSION & DEDUPLICATION ===
    # Convert region list to DataFrame for further processing
    region_polygons = pd.DataFrame(region_polygons)
    region_polygons = pd.merge(region_polygons, df.drop(['geometry'], axis=1), on=['WASTE_ID'])
    region_polygons = gpd.GeoDataFrame(region_polygons, geometry='geometry', crs=crs)
    region_polygons['geometry'] = region_polygons['geometry'].map(buffer_geometry)
    region_polygons = drop_duplicates(region_polygons, 'WASTE_ID')
    
    # === PHASE 11: OVERLAP RESOLUTION ===
    # Remove overlapping areas between adjacent Voronoi regions
    # Each region intersection is assigned to larger polygon via area comparison
    non_intersecting_polygons = resolve_polygon_overlaps(region_polygons)
    region_polygons['geometry'] = non_intersecting_polygons 
    region_polygons['geometry'] = region_polygons['geometry'].map(buffer_geometry)
    region_polygons['area'] = region_polygons.geometry.area

    # === PHASE 12: FINAL BOUNDARY CLIPPING ===
    # Filter sites that appear in final regions (have valid geometry)
    # Deduplicate original input sites by WKT representation
    point_df = df[df[col].isin(region_polygons[col])].reset_index(drop=True)
    df_waste = drop_duplicates(df, 'WKT_WWTP')
    
    # Clip regions to computed bounding box
    region_polygons = gpd.clip(region_polygons, actual_clipping_object)
    region_polygons['geometry'] = region_polygons['geometry'].map(buffer_geometry)

    # Clip regions to country boundary if provided (second clipping operation)
    if country_clip is not None and not country_clip.empty:
        region_polygons = gpd.clip(region_polygons, country_clip)
        region_polygons['geometry'] = region_polygons['geometry'].map(buffer_geometry)
    
    # === PHASE 13: CRS STANDARDIZATION & RETURN ===
    # Convert all outputs to WGS84 for standard geographic format
    df_waste = df_waste.to_crs(4326)
    region_polygons = region_polygons.to_crs(4326)
    point_df = point_df.to_crs(4326)
    
    # Return three GeoDataFrames:
    # - df_waste: Original sites (dedup by WKT_WWTP)
    # - region_polygons: Generated Voronoi regions with attributes
    # - point_df: Sites with valid regions in final output
    return df_waste, region_polygons, point_df

def voronoi_worker(args):
    """
    Worker function for parallel Voronoi generation.
    
    Unpacks tuple of arguments and calls weighted_voronoi function.
    Designed for use with multiprocessing.Pool.map().
    
    Args:
        args (tuple): Packed arguments for weighted_voronoi function
        
    Returns:
        tuple: (df_waste, region_polygons, point_df) from weighted_voronoi
        
    Notes:
        Catches and prints exceptions during unpacking.
    """
    try:
        sub_df, col, country_clip, scale_weights, clipping, n_points, distance_fn, scipy_true, cv2_true, centroid_points, points_col, buffering, buffer, threshold = args
        logger.debug(f"voronoi_worker: Unpacked arguments for {len(sub_df)} sites")
    except Exception as err:
        logger.error(f"voronoi_worker: Error unpacking arguments: {err}")
        raise
    return weighted_voronoi(sub_df, col, country_clip, scale_weights, clipping, n_points, distance_fn, scipy_true, cv2_true, centroid_points, points_col, buffering, buffer, threshold)

def create_weights(sub_df, sigma=3, percent_threshold=10, method='linear'):
    """
    Calculates weights based on 'total_area' using various scaling methods.
    Options for method: 'linear', 'logarithmic', 'square_root', 'sigmoid'
    """
    df = sub_df.copy()
    
    # 1. Handle missing/zero values in the raw data
    fallback_mean = df['total_area'].mean()
    base_values = df['total_area'].replace(0.0, np.nan).fillna(fallback_mean)
    
    # If everything is still NaN (empty or all zeros), fallback to equal distribution
    if base_values.isnull().all() or base_values.sum() == 0:
        df['weights'] = 1.0 / len(df)
        return df

    # 2. Apply the chosen transformation method
    if method == 'logarithmic':
        df['weights'] = np.log1p(base_values)
        
    elif method == 'square_root':
        df['weights'] = np.sqrt(base_values)
        
    elif method == 'sigmoid':
        # Normalize to Z-scores so the sigmoid center (0) aligns with the data mean
        z = (base_values - base_values.mean()) / (base_values.std() + 1e-9)
        df['weights'] = 1 / (1 + np.exp(-z))
    else: # Default to 'linear'
        df['weights'] = base_values
    # 3. Initial Normalization to sum=1
    df['weights'] = df['weights'] / df['weights'].sum()
    # 4. Outlier Clipping
    sub_std = df['weights'].std()
    sub_median = df['weights'].median()
    if not pd.isna(sub_std) and sub_std > 0:
        # Upper bound clipping
        upper_limit = sub_median + (sub_std * sigma)
        # Lower bound clipping
        lower_limit = sub_median / percent_threshold
        
        df['weights'] = df['weights'].clip(lower=lower_limit, upper=upper_limit)
    # 5. Final Re-normalization 
    # Necessary because clipping changes the total sum
    total_w = df['weights'].sum()
    if total_w > 0:
        df['weights'] = df['weights'] / total_w
    return df

def orchestrate_voronoi_weights(df, col, country_df, workers=12, scale_weights=False, clipping=None, n_points=100, distance_fn=default_distance_multiplicative,
                                scipy_true=False, cv2_true=False, centroid_points=False, points_col=None,
                                buffering=False, buffer=10000, threshold=500, only_round=False, sigma=3, percent_threshold=10, method='linear'):
    """
    Orchestrate parallel Voronoi generation across data groups.
    
    Groups dataframe by column, processes each group in parallel with
    weighted Voronoi generation, then concatenates results.
    
    Args:
        df (pd.GeoDataFrame): Input locations with geometry, weights, ISO_2
        col (str): Column to group by for parallel processing
        country_df (pd.GeoDataFrame): Country boundaries for clipping
        workers (int): Number of parallel workers
        scale_weights (bool): Apply weight scaling
        clipping (pd.GeoDataFrame): Boundary clipping GeoDataFrame
        (... and many other Voronoi parameters)
        
    Returns:
        tuple: (df_waste_final, region_df_final, point_df_final)
               All concatenated and merged from parallel workers
               
    Notes:
        COMPLEXITY: Extremely complex orchestration function with many
        nested conditions and edge cases.
        
        WEIGHT NORMALIZATION: Complex logic for standardizing weights:
          1. Replaces 0/NaN weights with mean
          2. Normalizes to sum=1
          3. Clips outliers (< sigma*std or < median/percent_threshold)
        
        COLUMN CONVERSION: Normalizes col values to rounded strings for
        grouping. May lose numeric precision.
        
        ASSUMPTION: Requires ISO_2, WKT_WWTP, WASTE_ID columns
        
        UNUSED VARIABLE: old_sub_df is created but never used.
    """
    # Group both df and clipping by the same column
    logger.info(f"Starting orchestrate_voronoi_weights for {len(df)} sites with {workers} workers")
    df = df[~df[col].isna()].reset_index(drop=True)
    logger.debug(f"After NaN filtering: {len(df)} sites in {len(df[col].unique())} groups")
    df[col] = normalize_column_to_rounded_str(df[col])

    if clipping is not None:
        clipping = clipping[~clipping[col].isna()].reset_index(drop=True)
        clipping[col] = normalize_column_to_rounded_str(clipping[col])

    df_groups = {str(k): v for k, v in df.groupby(col)}
    clip_groups = {str(k): v for k, v in clipping.groupby(col)} if clipping is not None else {}

    args = []
    skipped_count = 0
    for key, sub_df in df_groups.items():
        # For each group, perform weight normalization and outlier clipping before Voronoi generation
        # Each group will be projected to the appropriate UTM CRS for accurate area calculation and Voronoi generation
        # the clipping geometry for each group will also be projected to the same UTM CRS for accurate clipping during Voronoi generation
        if sub_df is None or sub_df.empty or 'ISO_2' not in sub_df:
            continue
        
        # Calculate area for weight normalization and outlier clipping
        sub_df = calculate_area(sub_df, only_round)
        sub_df = create_weights(sub_df, sigma, percent_threshold, method)
        
        # Estimate UTM CRS for this group based on geometry centroid for accurate distance calculations in Voronoi
        # and apply it to both the sites and the clipping geometry for this group   
        utm_crs = estimate_utm_crs(sub_df)
        if utm_crs is None:
            logger.debug(f"Group {key}: Could not estimate UTM CRS, skipping")
            skipped_count += 1
            continue

        sub_df = sub_df.to_crs(utm_crs)
        logger.debug(f"Group {key}: {len(sub_df)} sites after area calculation and CRS conversion")
        sub_df = drop_duplicates(drop_duplicates(sub_df, 'WASTE_ID'), 'geometry')
        if sub_df is None or sub_df.empty:
            continue
        
        # Get corresponding clipping geometry for this group if available
        sub_clip = clip_groups.get(key, None)
        if sub_clip is not None and not sub_clip.empty:
            if sub_clip.crs is None:
                sub_clip = sub_clip.set_crs(4326)
            sub_clip = sub_clip.to_crs(utm_crs)
            sub_clip = drop_duplicates(drop_duplicates(sub_clip, col), 'geometry')

        # Get corresponding country boundary for this group if available
        country_iso_2 = []
        country_clip = None
        if not sub_df.empty and 'ISO_2' in sub_df:
            iso2_series = sub_df['ISO_2'].dropna()
            if not iso2_series.empty:
                unique_vals = iso2_series.unique().tolist()
                if unique_vals:
                    country_iso_2 = unique_vals

        if len(country_iso_2) > 0:
            country_clip = country_df[country_df['country'].isin(country_iso_2)]
            if country_clip is not None and not country_clip.empty:
                if country_clip.crs is None:
                    country_clip = country_clip.set_crs(4326)
                country_clip = country_clip.to_crs(utm_crs)

        # If no country clipping is needed, pass the entire sub_df to the worker.
        # Otherwise, create separate tasks for each country within the group.
        if country_clip is None:
            args.append((sub_df, col, country_clip, scale_weights, sub_clip, n_points, distance_fn, scipy_true, cv2_true, centroid_points, points_col, buffering, buffer, threshold))
        else:
            for country in country_iso_2:
                args.append((sub_df[sub_df['ISO_2'] == country].copy().reset_index(drop=True),
                            col, country_clip[country_clip['country'] == country].copy().reset_index(drop=True),
                            scale_weights, sub_clip, n_points, distance_fn, scipy_true, cv2_true,
                            centroid_points, points_col, buffering, buffer, threshold)
                            )
                
    with Pool(processes=workers) as pool:
        results = pool.map(voronoi_worker, args)

    df_waste_all = []
    region_df_all = []
    point_df_all = []
    logger.debug(f"Processing {len(results)} Voronoi results from parallel workers")
    for result in results:
        if result is None:
            continue
        df_waste, region_df, point_df = result
        df_waste_all.append(df_waste)
        region_df_all.append(region_df)
        point_df_all.append(point_df)

    df_waste_final = finalize_gdf(df_waste_all, df.columns)
    region_df_final = finalize_gdf(region_df_all, df.columns)
    point_df_final = finalize_gdf(point_df_all, df.columns)
    logger.info(
        f"Orchestrate Voronoi complete: "
        f"{len(df_waste_final)} waste points, "
        f"{len(region_df_final)} regions, "
        f"{len(point_df_final)} points"
    )
    return df_waste_final, region_df_final, point_df_final

################################################################################
# SECTION 9: CONFIGURATION & MAIN EXECUTION
################################################################################

"""
MAIN EXECUTION WORKFLOW
=======================

This section orchestrates the complete Voronoi spatial allocation pipeline.
When run as __main__, it executes all 6 approaches in sequence:

1. Configuration: Loads YAML config and initializes output paths
2. Data Preparation: Loads WWTP, watershed, and country boundary data
3. Approach Execution: Runs each of 6 Voronoi variants with configured parameters
4. Output: Saves results to GeoTIFF/GeoJSON per approach_id

WORKFLOW:
  cfg = load_config()                    # Load YAML configuration
  → create_output_paths(cfg)              # Create output directory structure
  → prepare_data(cfg)                     # Load input spatial data
  → run_voronoi_approach() × 6            # Execute all 6 approach variants
  → Save results to cfg['paths']['voronoi_dir']

APPROACH VARIANTS (Conditional Execution):
  
  Approach 0: Buffer WWTP + Voronoi
    - Creates buffers around WWTP facilities
    - Generates Voronoi from buffer centroids
    - No weighting (equal allocation)
  
  Approaches 1a-1d: Weighted Buffer Voronoi (4 variants)
    - Applies distance-based weighting to Approach 0
    - 1a: multiplicative, no rounding
    - 1b: multiplicative, with rounding
    - 1c: additive, no rounding
    - 1d: additive, with rounding
    - Only runs if cfg['weights_cond'] == True
  
  Approach 2: Watershed + Voronoi
    - Uses watershed boundaries to constrain regions
    - Assigns to hydrological basins instead of WWTP buffers
    - No weighting (equal allocation)
  
  Approaches 3a-3d: Weighted Watershed Voronoi (4 variants)
    - Applies distance weighting to Approach 2
    - Same 4 weighting strategies as Approach 1
    - Only runs if cfg['weights_cond'] == True
  
  Approaches 4-5: City Voronoi (conditional)
    - Uses major cities instead of WWTP facilities
    - Approach 4: unweighted
    - Approach 5: distance-weighted (multiplicative)
    - Only runs if cfg['city_voronoi'] == True

See pipelines.run_voronoi_approach() for parameter documentation.
"""

########################### Global Variables ####################################
if __name__ == '__main__':
    import argparse
    import sys
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Run Voronoi spatial allocation approach(es) individually',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
EXAMPLES:
  # Run approach 0 only
  python -m research_code.create_voronoi --approach 0
  
  # Run multiple approaches
  python -m research_code.create_voronoi --approach 0 1a 1b 2
  
  # Run all weighted variants
  python -m research_code.create_voronoi --approach 1a 1b 1c 1d 3a 3b 3c 3d
  
  # Run with verbose logging
  python -m research_code.create_voronoi --approach 0 --verbose
  
  # Run all approaches (default)
  python -m research_code.create_voronoi
        '''
    )
    parser.add_argument('--approach', nargs='+', type=str, default=None,
                       help='Specific approach(es) to run (0, 1a-1d, 2, 3a-3d, 4, 5). Default: all')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Validate and normalize approach names
    VALID_APPROACHES = ['0', '1a', '1b', '1c', '1d', '2', '3a', '3b', '3c', '3d', '4', '5']
    if args.approach:
        requested = [str(a).lower() for a in args.approach]
        invalid = [a for a in requested if a not in VALID_APPROACHES]
        if invalid:
            parser.error(f"Invalid approach(es): {', '.join(invalid)}. Valid: {', '.join(VALID_APPROACHES)}")
        approaches_to_run = requested
    else:
        approaches_to_run = VALID_APPROACHES
    
    # Setup paths and logging
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    try:
        from .starter import load_config
        from .pipelines import create_output_paths, prepare_data, run_voronoi_approach
    except ImportError:  # Support running as a top-level script
        from starter import load_config
        from pipelines import create_output_paths, prepare_data, run_voronoi_approach
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    cfg = load_config()
    paths_dict = create_output_paths(cfg)
    data = prepare_data(cfg)
    
    gdf_bbox = data['gdf_bbox']
    watershed_gdf = data['watershed_gdf']
    country_df = data['country_df']
    
    # Ensure output directory exists
    os.makedirs(cfg['paths']['voronoi_dir'], exist_ok=True)
    
    logger.info(f"Running approaches: {', '.join(approaches_to_run)}")
    print("=" * 80)
    print(f"VORONOI ALLOCATION - APPROACH EXECUTION")
    print(f"Requested: {', '.join(approaches_to_run)}")
    print("=" * 80)
    
    # Pre-compute buffers needed for approaches 0, 1a-1d, 4, 5
    dissolved_buffers_WWTP = None
    dissolved_buffers_cities = None
    gdf_0 = None
    gdf_4 = None
    
    # Pre-compute watershed variants for approaches 2, 3a-3d
    gdf_2 = None
    watershed_gdf_2 = None
    
    # Execute requested approaches
    for approach_id in approaches_to_run:
        try:
            # === APPROACH 0: Buffer WWTP + Voronoi ===
            if approach_id == '0':
                logger.info("Starting Approach 0: Buffer WWTP + Voronoi")
                dissolved_buffers_WWTP = orchestrate_overlaps(gdf_bbox, cfg['max_workers'], 
                                                             paths_dict['buffers']['WWTP'], cfg['buffer'])
                dissolved_buffers_WWTP = drop_duplicates(drop_duplicates(dissolved_buffers_WWTP, 'WASTE_ID'), 'geometry')
                dissolved_buffers_WWTP['buffer_id'] = np.arange(len(dissolved_buffers_WWTP))
                
                gdf_0 = gdf_bbox.copy()
                gdf_0 = intersect_watershed_sindex(gdf_0, dissolved_buffers_WWTP, 'buffer_id', 
                                                   concurrency=cfg['sindex_concurrency'])
                gdf_0 = drop_duplicates(drop_duplicates(gdf_0, 'WASTE_ID'), 'geometry')
                
                run_voronoi_approach('0', gdf_0, dissolved_buffers_WWTP, country_df, cfg, cfg['distance_fn'], 
                                    paths_dict['voronoi']['0'], buffer_id_col='buffer_id', scale_weights=False, buffering=False, method=cfg['weight_method'])
                print("✓ Approach 0 completed")
            
            # === APPROACHES 1a-1d: Weighted Buffer Voronoi ===
            elif approach_id in ['1a', '1b', '1c', '1d']:
                # Setup approach 0 data if not already done
                if dissolved_buffers_WWTP is None:
                    logger.info("Pre-computing Approach 0 buffers for weighted variant")
                    dissolved_buffers_WWTP = orchestrate_overlaps(gdf_bbox, cfg['max_workers'], 
                                                                 paths_dict['buffers']['WWTP'], cfg['buffer'])
                    dissolved_buffers_WWTP = drop_duplicates(drop_duplicates(dissolved_buffers_WWTP, 'WASTE_ID'), 'geometry')
                    dissolved_buffers_WWTP['buffer_id'] = np.arange(len(dissolved_buffers_WWTP))
                
                if gdf_0 is None:
                    gdf_0 = gdf_bbox.copy()
                    gdf_0 = intersect_watershed_sindex(gdf_0, dissolved_buffers_WWTP, 'buffer_id', 
                                                       concurrency=cfg['sindex_concurrency'])
                    gdf_0 = drop_duplicates(drop_duplicates(gdf_0, 'WASTE_ID'), 'geometry')
                
                # Map variant ID to parameters
                variant_config = {
                    '1a': (default_distance_multiplicative, False),
                    '1b': (default_distance_multiplicative, True),
                    '1c': (default_distance_additive, False),
                    '1d': (default_distance_additive, True),
                }
                dist_fn, only_round = variant_config[approach_id]
                
                logger.info(f"Starting Approach {approach_id}: Weighted Voronoi variant")
                run_voronoi_approach(approach_id, gdf_0, dissolved_buffers_WWTP, country_df, cfg, dist_fn,
                                    paths_dict['voronoi'][approach_id], buffer_id_col='buffer_id', 
                                    scale_weights=True, only_round=only_round, buffering=False, method=cfg['weight_method'])
                print(f"✓ Approach {approach_id} completed")
            
            # === APPROACH 2: Watershed + Voronoi ===
            elif approach_id == '2':
                logger.info("Starting Approach 2: Watershed + Voronoi")
                gdf_2 = gdf_bbox.copy()
                gdf_2['buffer_id'] = gdf_2['HYBAS_ID']
                watershed_gdf_2 = watershed_gdf.copy()
                watershed_gdf_2['buffer_id'] = watershed_gdf_2['HYBAS_ID']
                run_voronoi_approach('2', gdf_2, watershed_gdf_2, country_df, cfg, cfg['distance_fn'],
                                    paths_dict['voronoi']['2'], buffer_id_col='buffer_id', 
                                    scale_weights=False, buffering=True, method=cfg['weight_method'])
                print("✓ Approach 2 completed")
            
            # === APPROACHES 3a-3d: Weighted Watershed Voronoi ===
            elif approach_id in ['3a', '3b', '3c', '3d']:
                # Setup approach 2 data if not already done
                if gdf_2 is None:
                    logger.info("Pre-computing Approach 2 data for weighted variant")
                    gdf_2 = gdf_bbox.copy()
                    gdf_2['buffer_id'] = gdf_2['HYBAS_ID']
                    watershed_gdf_2 = watershed_gdf.copy()
                    watershed_gdf_2['buffer_id'] = watershed_gdf_2['HYBAS_ID']
                
                # Map variant ID to parameters
                variant_config = {
                    '3a': (default_distance_multiplicative, False),
                    '3b': (default_distance_multiplicative, True),
                    '3c': (default_distance_additive, False),
                    '3d': (default_distance_additive, True),
                }
                dist_fn, only_round = variant_config[approach_id]
                
                logger.info(f"Starting Approach {approach_id}: Weighted watershed variant")
                run_voronoi_approach(approach_id, gdf_2, watershed_gdf_2, country_df, cfg, dist_fn,
                                    paths_dict['voronoi'][approach_id], buffer_id_col='buffer_id', 
                                    scale_weights=True, only_round=only_round, buffering=True, method=cfg['weight_method'])
                print(f"✓ Approach {approach_id} completed")
            
            # === APPROACHES 4-5: City Voronoi ===
            elif approach_id in ['4', '5']:
                # Setup city data if not already done
                if dissolved_buffers_cities is None:
                    logger.info("Loading and processing city data for Approaches 4-5")
                    df_cities = pd.read_csv(cfg['paths']['cities'])
                    df_cities = gpd.GeoDataFrame(df_cities, 
                                               geometry=gpd.GeoSeries([from_wkt(geom) if isinstance(geom, str) else geom 
                                                                        for geom in df_cities['geometry']]), 
                                               crs='epsg:4326')
                    df_cities['geometry'] = df_cities['geometry'].apply(buffer_geometry)
                    
                    if 'ISO_2' not in df_cities.columns:
                        if not os.path.exists(cfg['paths']['overture']):
                            download_overture_maps(cfg['paths']['overture_s3_url'], cfg['paths']['overture'])
                        df_cities = duckdb_intersect(df_cities, cfg['paths']['overture'])
                    
                    dissolved_buffers_cities = orchestrate_overlaps(df_cities, cfg['max_workers'], 
                                                                   paths_dict['buffers']['city'], cfg['buffer'])
                    dissolved_buffers_cities = drop_duplicates(dissolved_buffers_cities, 'geometry')
                    dissolved_buffers_cities['geometry'] = dissolved_buffers_cities['geometry'].apply(buffer_geometry)
                    dissolved_buffers_cities['buffer_id'] = np.arange(len(dissolved_buffers_cities))
                    
                    gdf_4 = gdf_bbox.copy()
                    gdf_4 = duckdb_intersect_watershed(gdf_4, dissolved_buffers_cities, 'buffer_id', 
                                                      use_duckdb=cfg.get('duckdb_cond', False), 
                                                      max_workers=cfg['max_workers'])
                    gdf_4 = drop_duplicates(drop_duplicates(gdf_4, 'WASTE_ID'), 'geometry')
                    gdf_4['geometry'] = gdf_4['geometry'].apply(buffer_geometry)
                
                if approach_id == '4':
                    logger.info("Starting Approach 4: City Voronoi")
                    run_voronoi_approach('4', gdf_4, dissolved_buffers_cities, country_df, cfg, cfg['distance_fn'],
                                        paths_dict['voronoi']['4'], buffer_id_col='buffer_id', 
                                        scale_weights=False, buffering=False, method=cfg['weight_method'])
                    print("✓ Approach 4 completed")
                elif approach_id == '5':
                    logger.info("Starting Approach 5: Weighted City Voronoi")
                    run_voronoi_approach('5', gdf_4, dissolved_buffers_cities, country_df, cfg, cfg['distance_fn'],
                                        paths_dict['voronoi']['5'], buffer_id_col='buffer_id', 
                                        scale_weights=True, buffering=False, method=cfg['weight_method'])
                    print("✓ Approach 5 completed")
        
        except Exception as e:
            logger.error(f"Error executing approach {approach_id}: {e}", exc_info=True)
            print(f"✗ Approach {approach_id} FAILED: {e}")
            sys.exit(1)
    
    print("=" * 80)
    print(f"SUCCESS: All requested approaches completed ({', '.join(approaches_to_run)})")
    print("=" * 80)
    logger.info(f"Voronoi generation completed for approaches: {', '.join(approaches_to_run)}")
