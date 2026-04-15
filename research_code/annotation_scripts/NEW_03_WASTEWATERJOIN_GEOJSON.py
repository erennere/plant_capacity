import os, sys
import random
import glob
import shutil
import geopandas as gpd
import pandas as pd
import shapely.geometry as geom
from shapely import from_wkb
from shapely.strtree import STRtree
from typing import List, Set, Dict, Tuple
import duckdb
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

# Configure logging to flush output immediately (important for HPC batch jobs)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)
# Ensure handler flushes after each message
for handler in logging.root.handlers:
    handler.flush()
    if hasattr(handler, 'setLevel'):
        handler.setLevel(logging.INFO)

# ============================================================
# I/O
# ============================================================
def from_wkb_modified(x): 
    try:
        x = from_wkb(x)
        return x
    except Exception as err:
        return None
    
def load_geodata(path):
    # Read the Parquet file
    df = pd.read_parquet(path)

    # If df already has a 'geometry' column as bytes or object type,
    # GeoPandas can interpret it automatically
    if 'geometry' not in df.columns and 'geom' in df.columns:
        # Rename the column
        df = df.rename(columns={'geom': 'geometry'})

    # Convert to GeoDataFrame; GeoPandas may auto-convert Arrow binary
    df['geometry'] = df['geometry'].map(from_wkb_modified)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)

    return gdf


def write_geodata(gdf: gpd.GeoDataFrame, path: str, driver: str = "GeoJSON") -> None:
    gdf.to_file(path, driver=driver)

# ============================================================
# Geometry helpers
# ============================================================

def compute_centroids(gdf):
    gdf = gdf.copy()
    gdf['centroid'] = None
    mask = gdf.geometry.is_valid & ~gdf.geometry.is_empty
    gdf.loc[mask, 'centroid'] = gdf.loc[mask, 'geometry'].centroid
    return gdf

def build_spatial_index(points):
    # We replace None with an empty Point to preserve the list index
    # STRtree will accept empty geometries but they won't match any spatial query
    placeholder = geom.Point() 
    indexed_points = [pt if (pt is not None and not pt.is_empty) else placeholder for pt in points]
    
    return STRtree(indexed_points)

# ============================================================
# Clustering
# ============================================================

def cluster_points(points,tree, distance_threshold):
    visited: Set[int] = set()
    clusters: List[Set[int]] = []

    for i, center in enumerate(points):
        # Skip if already visited OR if the point itself is None/Empty
        if i in visited or center is None or center.is_empty:
            continue

        cluster = {i}
        queue = [i]
        visited.add(i)
        while queue:
            current = queue.pop()
            curr_geom = points[current]

            # tree.query now returns the EXACT indices from the original points list
            candidate_indices = tree.query(curr_geom.buffer(distance_threshold), predicate="intersects")

            for j in candidate_indices:
                if j in visited:
                    continue
                
                neighbor = points[j]
                # Double check neighbor exists (though tree shouldn't match EMPTY points)
                if neighbor is None or neighbor.is_empty:
                    continue
                    
                if curr_geom.distance(neighbor) <= distance_threshold:
                    visited.add(j)
                    cluster.add(j)
                    queue.append(j)
        clusters.append(cluster)
    return clusters

# ============================================================
# Output geometry
# ============================================================

def clusters_to_bboxes(
    gdf: gpd.GeoDataFrame,
    clusters: List[Set[int]],
    label: str,
) -> gpd.GeoDataFrame:
    records = []
    for cid, cluster in enumerate(clusters):
        geoms = gdf.iloc[list(cluster)].geometry
        merged = geoms.unary_union
        bbox = geom.box(*merged.bounds)
        records.append({
            "cluster_id": cid,
            "man_name": label,
            "geometry": bbox
        })
    return gpd.GeoDataFrame(records, crs=gdf.crs)

def sanitize_gdf_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Force lowercase column names and remove duplicates
    gdf.columns = [c.lower() for c in gdf.columns]
    gdf = gdf.loc[:, ~gdf.columns.duplicated()]
    return gdf

def convert_geojson_to_parquet(
    geojson_file: str,
    temp_parquet_dir: str,
    overwrite: bool = False
) -> str:
    """
    Convert a single GeoJSON file to Parquet format using DuckDB.
    Returns the path to the created Parquet file.
    
    This function is designed to run in parallel via ThreadPoolExecutor.
    """
    basename = os.path.splitext(os.path.basename(geojson_file))[0]
    temp_parquet = os.path.join(temp_parquet_dir, f"{basename}.parquet")
    
    if not os.path.exists(temp_parquet) or overwrite:
        try:
            temp_conn = duckdb.connect(":memory:")
            temp_conn.execute("INSTALL SPATIAL; LOAD SPATIAL;")
            temp_conn.execute(f"""
            COPY (
                SELECT *
                FROM ST_Read('{geojson_file}')
            ) TO '{temp_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            temp_conn.close()
            logging.info(f"✅ Converted {basename} to Parquet")
        except Exception as e:
            logging.error(f"❌ Failed to convert {geojson_file}: {e}")
            return None
    else:
        logging.info(f"⏭️  Parquet already exists for {basename}, skipping")
    
    return temp_parquet


def parallel_convert_geojsons(
    geojson_files: List[str],
    temp_parquet_dir: str,
    max_workers: int = 4,
    overwrite = False
) -> List[str]:
    """
    Parallelize the conversion of GeoJSON files to Parquet format.
    
    Args:
        geojson_files: List of GeoJSON file paths
        temp_parquet_dir: Directory to store Parquet files
        max_workers: Number of parallel workers (default 4, adjust based on system)
    
    Returns:
        List of Parquet file paths created
    """
    parquet_files = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(convert_geojson_to_parquet, f, temp_parquet_dir, overwrite): f
            for f in geojson_files
        }
        
        for future in as_completed(futures):
            try:
                parquet_file = future.result()
                if parquet_file is not None:
                    parquet_files.append(parquet_file)
            except Exception as e:
                logging.error(f"Exception in parallel conversion: {e}")
    return sorted(parquet_files)  # Sort for consistent ordering


def get_parquet_schema_info(
    conn: duckdb.DuckDBPyConnection,
    tmp_table: str,
) -> Tuple[List[str], Dict[str, str]]:
    """
    Get column names and their types from a temporary table.
    Returns:
        Tuple of (column_names, column_type_dict)
    Note: Caller is responsible for dropping the table after use.
    """
    cols_info = conn.execute(f"PRAGMA table_info('{tmp_table}');").df()
    column_names = list(cols_info['name'])
    column_types = dict(zip(cols_info['name'], cols_info['type']))
    return column_names, column_types


def build_cast_expr(col: str, dtype: str) -> str:
    """Helper to build type casting expressions."""
    if dtype.upper() in ("BLOB", "STRING", "TEXT"):
        return f'CAST("{col}" AS VARCHAR) AS "{col}"'
    return f'"{col}"'


def discover_parquet_schema(parquet_file: str) -> Tuple[str, str, List[str], Dict[str, str]]:
    """
    Discover schema of a single parquet file in parallel.
    Uses a separate connection to avoid lock contention.
    
    Args:
        parquet_file: Path to parquet file
        
    Returns:
        Tuple of (parquet_file, grid, column_names, column_types_dict)
    """
    temp_conn = duckdb.connect(":memory:")
    try:
        basename = os.path.splitext(os.path.basename(parquet_file))[0]
        grid = basename.split('_')[1]
        tmp_table = f"tmp_{basename.replace('-', '_')}"
        
        # Load parquet WITHOUT adding GRID column (GRID is metadata, not a data column)
        temp_conn.execute(
            f"""CREATE TEMP TABLE {tmp_table} 
               AS SELECT * 
               FROM read_parquet('{parquet_file}');"""
        )
        col_names, col_types = get_parquet_schema_info(temp_conn, tmp_table)
        return parquet_file, grid, col_names, col_types
    finally:
        temp_conn.close()

def merge_parquets_sql(
    conn: duckdb.DuckDBPyConnection,
    parquet_files: List[str],
    max_workers: int = 4,
    insert_batch_size: int = 8,
) -> Dict[str, str]:
    """
    Merge multiple Parquet files with parallel schema discovery and data loading.
    Eliminates lock contention by creating unified schema upfront instead of incremental ALTERs.
    
    Args:
        conn: Main DuckDB connection
        parquet_files: List of parquet file paths
        max_workers: Number of parallel workers for schema discovery (default 4)
    
    Returns:
        Dictionary mapping original parquet paths to grid names
    """
    grid_mapping = {}
    schema_results = {}
    
    # Phase 1: Discover all schemas in parallel using separate connections
    logging.info(f"🔄 Discovering schemas from {len(parquet_files)} parquets (max_workers={max_workers})...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(discover_parquet_schema, pf) for pf in parquet_files]
        
        for future in as_completed(futures):
            try:
                pf, grid, col_names, col_types = future.result()
                schema_results[pf] = (grid, col_names, col_types)
                grid_mapping[pf] = grid
                basename = os.path.splitext(os.path.basename(pf))[0]
                logging.info(f"  ✅ {basename}: {len(col_names)} columns")
            except Exception as e:
                logging.error(f"❌ Failed to discover schema: {e}")
    
    # Phase 2: Build unified schema (union of all columns) - normalize to lowercase
    all_columns_map = {}  # col_name (lowercase) -> col_type (normalized)
    
    for pf, (grid, col_names, col_types) in schema_results.items():
        for col, dtype in col_types.items():
            col_lower = col.lower()
            if col_lower not in all_columns_map:
                # Normalize text types
                if dtype.upper() in ("BLOB", "STRING", "TEXT"):
                    dtype = "VARCHAR"
                all_columns_map[col_lower] = dtype
    
    # Add GRID as a metadata column (present in all parquets)
    all_columns_map['grid'] = 'VARCHAR'
    
    all_columns_ordered = sorted(all_columns_map.keys())
    logging.info(f"📊 Unified schema: {len(all_columns_ordered)} unique columns")
    
    # Phase 3: Create dataset table with all columns at once (avoids ALTER locks)
    # Deduplicate case-insensitive column names using a set
    col_defs = []
    seen_cols = set()
    for col in all_columns_ordered:
        col_lower = col.lower()
        if col_lower not in seen_cols:
            dtype = all_columns_map[col]
            col_defs.append(f'"{col}" {dtype}')
            seen_cols.add(col_lower)
    
    conn.execute('DROP TABLE IF EXISTS dataset;')
    conn.execute(f'CREATE TABLE dataset ({", ".join(col_defs)});')
    logging.info(f"✅ Created dataset table with unified schema")
    
    # Phase 4: Load and insert from all parquets using controlled batched UNION ALL inserts.
    logging.info(f"⚙️  Inserting {len(parquet_files)} parquets into dataset...")
    batch_size = max(1, insert_batch_size)

    for batch_start in range(0, len(parquet_files), batch_size):
        batch_files = parquet_files[batch_start:batch_start + batch_size]

        select_statements = []
        for pf in batch_files:
            grid, col_names, _col_types = schema_results[pf]
            col_names_lower = {c.lower(): c for c in col_names}

            aligned_expr = []
            for col in all_columns_ordered:
                if col == 'grid':
                    aligned_expr.append(f"'{grid}' AS \"{col}\"")
                elif col in col_names_lower:
                    original_col = col_names_lower[col]
                    aligned_expr.append(f'"{original_col}" AS "{col}"')
                else:
                    aligned_expr.append(f'NULL AS "{col}"')

            select_statements.append(
                f'SELECT {", ".join(aligned_expr)} FROM read_parquet(\'{pf}\')'
            )

        union_query = ' UNION ALL '.join(select_statements)
        conn.execute(f'INSERT INTO dataset {union_query}')

        batch_num = (batch_start // batch_size) + 1
        total_batches = (len(parquet_files) + batch_size - 1) // batch_size
        logging.info(f"  Inserted batch {batch_num}/{total_batches} ({len(batch_files)} files)")
    
    return grid_mapping

def merge_bboxes_sql(
    polygons_dir: str,
    prototype: str,
    output_filepath: str,
    temp_parquet_dir: str = "temp_parquets",
    max_workers: int = 4,
    insert_batch_size: int = 8,
    duckdb_threads: int = 4,
    overwrite: bool = False
) -> None:
    """
    Main orchestration function for merging GeoJSON files into a single Parquet.
    
    Args:
        polygons_dir: Directory containing GeoJSON files
        prototype: Glob pattern for GeoJSON files (e.g., '*_polygons.geojson')
        output_filepath: Output path for merged Parquet
        temp_parquet_dir: Temporary directory for intermediate Parquet files
        max_workers: Number of parallel workers for GeoJSON conversion and parquet merge (default 4)
        overwrite: Whether to overwrite existing files (default False)
    """
    files = glob.glob(os.path.join(polygons_dir, prototype))
    if not files:
        print(f"❌ No files found in {polygons_dir}", flush=True)
        return

    conn = temp_file = None
    os.makedirs(temp_parquet_dir, exist_ok=True)

    try:
        # Temporary DuckDB file
        temp_file = f'temp_{random.randint(1, int(1e12))}.db'
        conn = duckdb.connect(temp_file)
        conn.execute(f"SET threads TO {int(duckdb_threads)};")
        conn.execute("SET preserve_insertion_order=false;")
        conn.execute("SET memory_limit='220GB';")  # Adjust based on system capabilities
        conn.execute("SET max_temp_directory_size = '220GB';")  # Use current directory for temp files
        conn.execute('INSTALL SPATIAL; LOAD SPATIAL;')
        
        # 1️⃣ Parallelize GeoJSON to Parquet conversion
        logging.info(f"🔄 Converting {len(files)} GeoJSON files to Parquet (max_workers={max_workers})...")
        temp_parquet_files = parallel_convert_geojsons(files, temp_parquet_dir, max_workers=max_workers, overwrite=overwrite)
        logging.info(f"✅ Converted all files. Starting merge...")
        
        # 2️⃣ Merge Parquets with parallel schema discovery and unified table creation
        merge_parquets_sql(
            conn,
            temp_parquet_files,
            max_workers=max_workers,
            insert_batch_size=insert_batch_size,
        )
        conn.execute(f"""
            COPY (
                SELECT * FROM dataset
            ) TO '{output_filepath}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)
        logging.info(f"✅ Exported: {output_filepath}")

    finally:
        if conn is not None:
            conn.close()
        if temp_file is not None and os.path.exists(temp_file):
            os.remove(temp_file)
        # Note: temp_parquet_dir is kept for potential reuse; delete if cleanup is needed


# ============================================================
# Main
# ============================================================

def main(
    input_path: str,
    output_path: str,
    distance_threshold: float,
    label: str = "wastewater_plant",
) -> None:
    gdf = load_geodata(input_path)
    print(f"✅ Loaded {len(gdf)} polygons", flush=True)

    gdf = compute_centroids(gdf)
    centroids = gdf.centroid.tolist()
    tree = build_spatial_index(centroids)

    clusters = cluster_points(
        centroids,
        tree,
        distance_threshold
    )
    print(f"✅ Found {len(clusters)} spatial clusters", flush=True)

    out_gdf = clusters_to_bboxes(gdf, clusters, label)
    write_geodata(out_gdf, output_path)
    print(f"✅ GeoJSON saved to: {output_path}", flush=True)

# ============================================================
# Entry point
# ============================================================

CRS_OUT = "EPSG:4326"

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from first_3 import load_config
    cfg = load_config('../config.yaml')
    os.chdir(parent_dir)
    overwrite = cfg["annotations"]["overwrite"]

    points_path = cfg["paths"]["corrected_all_filepath"]
    #points_path = './annotation_scripts/ref.geojson'
    
    grid_filedir = cfg["paths"]["annotations_grid_dir"]
    grid_filepath = os.path.join(grid_filedir, f'grids_{os.path.basename(points_path)}')
    polygons_dir = cfg["paths"]["annotations_by_osm_dir"]
    temp_parquet_dir = cfg["paths"]["annotations_temp_parquet_dir"]

    output_dir = os.path.join(os.path.abspath(os.path.join(grid_filedir, '..')), 'data')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    output_filepath = os.path.join(output_dir, 'merged_polygons.parquet')
    output_path = os.path.join(output_dir, 'wastewater_plant.geojson')
    prototype = f'*_polygons.geojson'
    
    
    # Run polygons and lines merges in parallel using separate processes (bypasses GIL)
    merge_tasks = []
    max_parallel_workers = 128
    
    if not os.path.exists(output_filepath) or overwrite:
        merge_tasks.append({
            "polygons_dir": polygons_dir,
            "prototype": prototype,
            "output_filepath": output_filepath,
            "temp_parquet_dir": temp_parquet_dir
        })
    
    if not os.path.exists(output_filepath.replace('polygons', 'lines')) or overwrite:
        merge_tasks.append({
            "polygons_dir": polygons_dir,
            "prototype": prototype.replace('polygons', 'lines'),
            "output_filepath": output_filepath.replace('polygons', 'lines'),
            "temp_parquet_dir": temp_parquet_dir
        })
    
    # Execute merge tasks in parallel with separate processes
    if merge_tasks:
        # Split workers across concurrently running merge jobs to avoid memory spikes.
        per_task_workers = max_parallel_workers // len(merge_tasks)
        with ProcessPoolExecutor(max_workers=len(merge_tasks)) as executor:
            futures = [
                executor.submit(
                    merge_bboxes_sql,
                    **task,
                    max_workers=per_task_workers,
                    insert_batch_size=64,
                    duckdb_threads=per_task_workers,
                    overwrite=overwrite
                )
                for task in merge_tasks
            ]
            for future in futures:
                future.result()
        
    main(
        input_path=output_filepath,
        output_path=output_path,
        distance_threshold=0.02,
    )
