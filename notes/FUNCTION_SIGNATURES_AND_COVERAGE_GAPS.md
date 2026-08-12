# Function Signatures and Coverage Gaps Analysis

> **Stale in places.** This is a point-in-time analysis snapshot. Since it was
> written, `create_voronoi.dissolve_overlapping_geometries` (the slow recursive
> variant) has been deleted along with its `recursion_lim` config key — only
> `dissolve_overlapping_geometries_fast` remains — and several helpers have moved
> into `src/geo_utils.py` / `src/utils.py`. Verify against the code before acting
> on any entry here.


## 1. create_voronoi.py (598 MISSING STATEMENTS - HIGHEST PRIORITY)

### Core Class

#### `UnionFind.__init__(n)`
- **Parameters:** `n: int` (number of elements)
- **Returns:** None (instance initialization)
- **Uncovered Paths:**
  - Large `n` values (performance testing with n > 1M)
  - Edge case: n=0, n=1

#### `UnionFind.find(x)`
- **Parameters:** `x: int` (element index)
- **Returns:** `int` (root element with path compression)
- **Uncovered Paths:**
  - Invalid index (negative, out of bounds)
  - Deep compression paths (N-depth chains)

#### `UnionFind.union(x, y)`
- **Parameters:** `x: int`, `y: int` (elements to union)
- **Returns:** None
- **Uncovered Paths:**
  - Already-unioned sets
  - Rank-based optimization paths (equal rank case)

---

### Geometry Validation & Manipulation (SECTION 1)

#### `geometry_contains_points(geometry, points)`
- **Parameters:** `geometry: shapely.BaseGeometry`, `points: np.ndarray | None`
- **Returns:** `np.ndarray[bool]` (mask of contained points)
- **Uncovered Paths:**
  - `points=None` or empty array
  - Fallback to loop when vectorized shapely unavailable
  - Non-standard geometry types

#### `ensure_output_dir_for_file(filepath)`
- **Parameters:** `filepath: str` (output file path)
- **Returns:** None (creates directory)
- **Uncovered Paths:**
  - Permission errors
  - Path with "." or "" parent directories
  - Very long paths

#### `normalize_plane(a, b)`
- **Parameters:** `a: np.ndarray` (shape (n, 2)), `b: tuple | np.ndarray` ((x, y))
- **Returns:** `tuple[np.ndarray, np.ndarray]` (normalized a, normalized b)
- **Uncovered Paths:**
  - **CRITICAL:** When `max_vals - min_vals == 0` on axis (denominator clamped to 1)
  - All points identical (all_points min == max)
  - Very small differences (numerical precision edge case)

#### `is_valid_geom(geom)`
- **Parameters:** `geom: shapely.BaseGeometry | None`
- **Returns:** `bool`
- **Uncovered Paths:**
  - Exception in `.is_valid` check
  - Exception in `.coords` access
  - Non-finite coordinates at edge of float64 range
  - Geometries with mixed finite/non-finite coordinates

#### `drop_duplicates(df, col)`
- **Parameters:** `df: pd.DataFrame | None`, `col: str`
- **Returns:** `pd.DataFrame | None`
- **Uncovered Paths:**
  - `df=None`
  - Column not in dataframe
  - All NaN values in column

#### `buffer_geometry(geom)`
- **Parameters:** `geom: shapely.BaseGeometry`
- **Returns:** `shapely.BaseGeometry`
- **Uncovered Paths:**
  - Exception during buffer(0) operation
  - **CRITICAL:** Unknown geometry type (not Point, LineString, Polygon, MultiX)
  - Corrupted/self-intersecting geometries

#### `create_centroid_points(geom)`
- **Parameters:** `geom: shapely.BaseGeometry | None`
- **Returns:** `shapely.Point | None`
- **Uncovered Paths:**
  - Invalid centroid result (`.is_empty` or `.is_valid` fails)
  - Unsupported geometry type (GeometryCollection, etc.)
  - NaN geometry passed as Series element

---

### Spatial Clustering (SECTION 3)

#### `cluster_point_indices(geoms, threshold)`
- **Parameters:** `geoms: Iterable[shapely.Point]`, `threshold: float` (distance in units)
- **Returns:** `list[set]` (clusters of point indices)
- **Uncovered Paths:**
  - Empty geoms list
  - `threshold <= 0`
  - All points identical (no neighbors besides self)
  - Very large point sets (K-D tree performance degradation)
  - **CRITICAL:** Non-point geometries in geoms (expected to be pre-filtered)

#### `cluster_points(df, threshold)`
- **Parameters:** `df: pd.GeoDataFrame`, `threshold: float`
- **Returns:** `pd.GeoDataFrame`
- **Uncovered Paths:**
  - Empty dataframe
  - Missing 'weights' or 'geometry' column
  - All single-point clusters (no merging needed)
  - Multiple rows with identical 'num_missing' values (tie-breaking uses iloc[0])
  - Clusters missing 'POP_SERVED' column

---

### Grid & Distance Utilities (SECTION 4)

#### `create_ranges(x, y, step, min_step=100)`
- **Parameters:** `x: float`, `y: float`, `step: float`, `min_step: float = 100`
- **Returns:** `np.ndarray` (coordinate range)
- **Uncovered Paths:**
  - **CRITICAL:** `min_step` very small (no iteration limit - infinite loop risk)
  - `x == y` (returns just `[x, y]`)
  - `step > (max_val - min_val)` (immediate halving loop)
  - Negative step values

#### `nearest_neighbor_distances_and_median(df)`
- **Parameters:** `df: pd.DataFrame | pd.GeoDataFrame | None`
- **Returns:** `tuple[np.ndarray, float]` (distances, median)
- **Uncovered Paths:**
  - `df=None` or empty
  - No 'geometry' column
  - All geometries empty or None
  - Single geometry (returns [NaN], NaN)
  - Non-point geometries with invalid centroids

#### `auto_weight_scale(points)`
- **Parameters:** `points: list | np.ndarray` (shape (n, 2))
- **Returns:** `float` (median inter-point distance)
- **Uncovered Paths:**
  - Empty points list
  - All points identical (all min_dists = 0)
  - Non-finite coordinates (not filtered by comprehension)
  - Single valid point

#### `default_distance_additive(a, b, weight, factor)`
- **Parameters:** `a: np.ndarray` (shape (n, 2)), `b: tuple`, `weight: float`, `factor: float`
- **Returns:** `np.ndarray` (weighted distances)
- **Uncovered Paths:**
  - `weight > distance` (result clamped to 0.01)
  - **CRITICAL:** `weight=0` (no contraction)
  - **CRITICAL:** Negative weight (squared term reverses sign)

#### `default_distance_multiplicative(a, b, weight, factor)`
- **Parameters:** `a: np.ndarray`, `b: tuple`, `weight: float`, `factor: float`
- **Returns:** `np.ndarray`
- **Uncovered Paths:**
  - **CRITICAL:** `weight=0` (division by zero - NO ERROR HANDLING)
  - Negative weight (inverts distance scaling)

#### `estimate_utm_epsg(lon, lat)`
- **Parameters:** `lon: float` (-180 to 180), `lat: float` (-90 to 90)
- **Returns:** `int` (EPSG code) or `3857` on error
- **Uncovered Paths:**
  - Out-of-range coordinates (raises ValueError)
  - Edge cases: lon=±180, lat=±90
  - Invalid EPSG codes (returns 3857)
  - Coordinates at UTM zone boundaries (off-by-one risk)

#### `estimate_utm_crs(gdf)`
- **Parameters:** `gdf: pd.GeoDataFrame`
- **Returns:** `pyproj.CRS` (UTM or fallback EPSG:3857)
- **Uncovered Paths:**
  - No valid geometries (returns EPSG:3857)
  - Non-finite centroid coordinates (searches for Point, then fallback)
  - All geometries have invalid centroids
  - GeometryCollection or other complex types

---

### Data Processing & Normalization (SECTION 5)

#### `calculate_area(df, only_round=False)`
- **Parameters:** `df: pd.GeoDataFrame | None`, `only_round: bool = False`
- **Returns:** `pd.DataFrame` (input with area-derived columns)
- **Uncovered Paths:**
  - `df=None` or empty (returns as-is)
  - Missing 'wwtp_area_rect' column (sets `total_area=1`)
  - Missing 'num_detection_rect' or 'num_detection_circle' (fills with 0)
  - Zero `capacity_proxy` for all rows (uses `fallback_mean` = NaN if all zeros)
  - Regex parsing fails on 'diameters' string (non-numeric values)
  - `only_round=True` logic path

#### `normalize_column_to_rounded_str(series)`
- **Parameters:** `series: pd.Series` (numeric)
- **Returns:** `pd.Series` (string)
- **Uncovered Paths:**
  - All NaN series
  - Mixed integer/float series
  - Very large numbers (rounding precision)

---

### DuckDB & External Data Integration (SECTION 6)

#### `download_overture_maps(url, filepath)`
- **Parameters:** `url: str` (S3 URL), `filepath: str` (local path)
- **Returns:** None (writes to disk)
- **Uncovered Paths:**
  - **CRITICAL:** Network errors (caught as generic Exception)
  - Malformed S3 URL
  - Insufficient disk space
  - Permission errors on parent directory
  - GeoParquet read failures

#### `process_centroid(args)`
- **Parameters:** `args: tuple` (centroid, spatial_index, polygon_gdf, column_name)
- **Returns:** `Any` (value from polygon_gdf[column] or None)
- **Uncovered Paths:**
  - Centroid is None, empty, or invalid
  - Multiple matches (returns first match)
  - No matches (returns None without explicit error)
  - Column not in polygon_gdf

---

### DuckDB Polygon Intersection Variants (SECTION 6 continued)

#### `intersect_with_polygon_sindex(df, polygons, col, concurrency=False)`
- **Parameters:** `df: geopandas.GeoDataFrame`, `polygons: geopandas.GeoDataFrame`, `col: str`, `concurrency: bool = False`
- **Returns:** `geopandas.GeoDataFrame`
- **Uncovered Paths:**
  - `df=None` or empty
  - Invalid/missing geometries (separated and re-concatenated)
  - **CRITICAL:** Spatial index creation fails
  - concurrency=True path with ThreadPoolExecutor errors
  - No polygon matches found (all values are None)
  - **CRITICAL:** Buffer geometry fails during centroid processing

#### `intersect_with_polygons_db(df, polygons, cols, df_join_col='ISO_2', polygon_join_col='ISO_2')`
- **Parameters:** `df: geopandas.GeoDataFrame`, `polygons: geopandas.GeoDataFrame`, `cols: list | str`, `df_join_col: str`, `polygon_join_col: str`
- **Returns:** `geopandas.GeoDataFrame`
- **Uncovered Paths:**
  - Missing join columns (raises KeyError)
  - Empty DataFrames
  - **CRITICAL:** DuckDB connection fails
  - Query syntax errors
  - Invalid WKT geometries in conversion

#### `intersect_with_polygons_parallelized(df, polygons, cols, use_duckdb=False, max_workers=16, df_join_col='ISO_2', polygon_join_col='ISO_2')`
- **Parameters:** Parallel polygon intersection with UTM zone partitioning
- **Returns:** `geopandas.GeoDataFrame`
- **Uncovered Paths:**
  - Empty or None inputs
  - No UTM zones found (empty unique_utms)
  - Mixed use_duckdb paths with different worker behavior
  - Per-zone processing failures (continues processing remaining zones)

#### `intersects_with_country_db(df, filepath, polygon_country_col='country', output_country_col='ISO_2')`
- **Parameters:** Points to enrich with country codes, boundary parquet path
- **Returns:** `geopandas.GeoDataFrame`
- **Uncovered Paths:**
  - Empty dataframe
  - Parquet file doesn't exist (DuckDB read_parquet fails)
  - **CRITICAL:** Bounding box filtering returns no matches
  - Point outside all country boundaries (returns NaN for country col)

---

### Buffer & Geometry Dissolution (SECTION 7)

#### `dissolve_overlapping_geometries(subdf, radius, convex=False, recursion_lim=50000)`
- **Parameters:** `subdf: geopandas.GeoDataFrame`, `radius: float`, `convex: bool`, `recursion_lim: int`
- **Returns:** `tuple[list[set], geopandas.GeoDataFrame] | None`
- **Uncovered Paths:**
  - `subdf=None` or empty
  - UTM CRS estimation fails (returns None)
  - **CRITICAL:** Recursion limit too low for deep graphs (DFS stack overflow)
  - No overlaps found (empty graphs and components)
  - Mixed convex=True/False geometry handling
  - **CRITICAL:** Latitude/longitude grouping with boundary conditions
  - Ties in DFS traversal (order undefined)

#### `dissolve_overlapping_geometries_fast(subdf, radius, convex=False)`
- **Parameters:** `subdf: geopandas.GeoDataFrame`, `radius: float`, `convex: bool`
- **Returns:** `tuple[list[set], geopandas.GeoDataFrame | None]`
- **Uncovered Paths:**
  - Empty input (returns [], None)
  - Spatial index construction fails
  - **CRITICAL:** No intersecting geometries (empty graph)
  - NetworkX connected_components with single/isolated nodes

#### `orchestrate_overlaps(df, max_workers, buffers_filepath, radius, convex=False, country_col='ISO_2')`
- **Parameters:** Input geometries, worker count, cache path, buffer radius
- **Returns:** Unknown (signature cut off)
- **Uncovered Paths:**
  - Max_workers invalid values
  - Cache file I/O errors
  - Country partitioning with no matching countries

---

### Voronoi Computation & Orchestration (SECTIONS 8-9)

#### `resolve_polygon_overlaps(region_polygons)`
- **Parameters:** `region_polygons: list[shapely.Polygon]`
- **Returns:** Unknown (likely dissolved polygons)
- **Uncovered Paths:**
  - Empty list
  - Non-polygon geometries
  - Self-intersecting polygons

#### `extract_site_coordinates(df, centroid_points)`
- **Parameters:** `df: geopandas.GeoDataFrame`, `centroid_points: np.ndarray`
- **Returns:** Unknown
- **Uncovered Paths:**
  - Empty dataframe or centroid array
  - Centroid coordinates outside grid bounds

#### `calculate_buffer(df, weights, *args, **kwargs)`
- **Parameters:** Points, weights, variable args/kwargs
- **Returns:** Unknown (likely buffer distances or masks)
- **Uncovered Paths:**
  - **CRITICAL:** Complex parameter interaction (*args, **kwargs)
  - Zero or negative weights
  - Empty weights array

#### `initialize_voronoi_weights(df, distance_fn, scale_weights, points)`
- **Parameters:** Dataframe, distance function, scale flag, point array
- **Returns:** Unknown (likely initialized weights)
- **Uncovered Paths:**
  - Invalid distance_fn
  - scale_weights edge cases
  - Empty points array

#### `extract_contours_scipy(region_mask_2d, n_points, grid_minx, grid_miny)`
- **Parameters:** 2D binary mask, point count, grid coordinates
- **Returns:** Unknown (likely polygon coordinates)
- **Uncovered Paths:**
  - Empty or all-zero mask
  - Invalid n_points values
  - **CRITICAL:** scikit-image find_contours may fail

#### `extract_contours_cv2(region_mask_2d, n_points, grid_minx, grid_miny)`
- **Parameters:** 2D binary mask, point count, grid coordinates
- **Returns:** Unknown (likely polygon coordinates)
- **Uncovered Paths:**
  - Empty mask
  - OpenCV contour detection failures

#### `extract_contours_rasterio(region_mask_2d, n_points, grid_minx, grid_miny)`
- **Parameters:** 2D binary mask, point count, grid coordinates
- **Returns:** Unknown (likely polygon coordinates)
- **Uncovered Paths:**
  - rasterio.features.shapes parsing failures

#### `finalize_gdf(df_list, cols)`
- **Parameters:** `df_list: list[pd.DataFrame]`, `cols: list[str]`
- **Returns:** Unknown (likely concatenated GeoDataFrame)
- **Uncovered Paths:**
  - Empty df_list
  - Inconsistent column sets across dataframes
  - Column not in cols list

#### `assign_sites_streaming(valid_points, points, weights, distance_fn, factor)`
- **Parameters:** Valid grid points, site coordinates, weights, distance function, factor
- **Returns:** Unknown (likely site assignments)
- **Uncovered Paths:**
  - Empty valid_points or points
  - Division by zero in distance calculation
  - Invalid weights

#### `weighted_voronoi(df, col, country_clip, scale_weights=False, clipping=None, n_points=100, distance_fn=default_distance_multiplicative, ...)`
- **Parameters:** Points, weight column, country clipping, scale flag, clipping geometry, point count, distance function
- **Returns:** Unknown (likely Voronoi polygons)
- **Uncovered Paths:**
  - **CRITICAL:** Complex orchestration with many conditional paths
  - scale_weights=True optimization path
  - Different clipping geometries (None vs GeometryCollection vs Polygon)
  - n_points=0 or very large values
  - **CRITICAL:** distance_fn parameter - custom functions may fail

#### `voronoi_worker(args)`
- **Parameters:** `args: tuple` (unknown composition)
- **Returns:** Unknown (likely worker results)
- **Uncovered Paths:**
  - Worker exception handling

#### `create_weights(sub_df, sigma=3, percent_threshold=10, method='linear')`
- **Parameters:** `sub_df: pd.DataFrame`, `sigma: float`, `percent_threshold: float`, `method: str`
- **Returns:** Unknown (likely weight series)
- **Uncovered Paths:**
  - `method` not 'linear' or recognized value
  - Zero sigma values
  - All values below percent_threshold

#### `orchestrate_voronoi_weights(df, col, country_df, workers=12, scale_weights=False, clipping=None, n_points=100, distance_fn=default_distance_multiplicative, ...)`
- **Parameters:** Complex orchestration parameters
- **Returns:** Unknown (likely final Voronoi layer)
- **Uncovered Paths:**
  - **CRITICAL:** Multi-worker coordination failures
  - **CRITICAL:** 598 missing statements likely in this function and its dependencies
  - Partial worker failures (some workers fail, others succeed)
  - Country partitioning edge cases

#### `_filter_requested_approaches(requested_approaches, cfg, paths_dict, only_round=False)`
- **Parameters:** `requested_approaches: list`, `cfg: dict`, `paths_dict: dict`, `only_round: bool`
- **Returns:** Unknown
- **Uncovered Paths:**
  - Invalid approach names
  - Missing config keys
  - Missing path entries

---

## 2. create_rasters.py (Population Extraction)

### Core Functions

#### `_sanitize_polygon_geom(geom)`
- **Parameters:** `geom: shapely.BaseGeometry | None`
- **Returns:** `shapely.Polygon | shapely.MultiPolygon | None`
- **Uncovered Paths:**
  - `geom=None` or `.is_empty`
  - `make_valid()` or `buffer(0)` fails and returns None
  - GeometryCollection with mixed types (filters non-polygon parts)
  - All polygon parts empty after filtering

#### `geotiff_exists_and_valid(path)`
- **Parameters:** `path: str` (file path)
- **Returns:** `bool`
- **Uncovered Paths:**
  - File not found
  - Corrupted GeoTIFF (rasterio.open raises Exception)

#### `extract_worldpop_universal(raster_path, hybas_gdf, exclude_gdf, min_pixels=9, zoom_level=8, basin_col='HYBAS_ID')`
- **Parameters:** Complex raster extraction with many edge cases
- **Returns:** `geopandas.GeoDataFrame | None`
- **Uncovered Paths:**
  - **CRITICAL:** Empty raster (no blocks)
  - **CRITICAL:** No spatial index (sindex creation fails)
  - CRS mismatch between raster and GeoDataFrames
  - Window processing with no intersecting basins
  - Invalid polygon geometries in basin/exclude masks
  - exact_extract returns None or empty stats
  - Chunk boundary conditions (start_idx > len(gdf))
  - **CRITICAL:** MERGE_THRESHOLD hits exactly (list length == threshold)

#### `polygon_raster_sign_from_gdf(raster_path, polygons_gdf, output_path)`
- **Parameters:** `raster_path: str`, `polygons_gdf: geopandas.GeoDataFrame`, `output_path: str`
- **Returns:** `tuple[str, int | None, int | None]` (output_path, sum_pos, sum_neg)
- **Uncovered Paths:**
  - No valid polygons in polygons_gdf
  - nodata value handling edge cases
  - Block window with no intersecting polygons
  - Signed integer overflow (large raster sums)

#### `orchestrate_country_intersection(raster_path, polygons_gdf, watershed_gdf, output_path, min_pixels=9, zoom_level=8, basin_col='HYBAS_ID')`
- **Parameters:** Country raster processing orchestration
- **Returns:** `tuple[str, int | None, int | None, geopandas.GeoDataFrame | None]`
- **Uncovered Paths:**
  - Both sign and extract functions fail
  - Raster reading raises exception

#### `orchestrate_intersections(tif_dict, gdf, watershed_gdf, output_dir, csv_output_filepath, non_served_outpath, max_workers=4, ...)`
- **Parameters:** Multi-country parallel orchestration
- **Returns:** `dict[str, bool]` (country → success flag)
- **Uncovered Paths:**
  - **CRITICAL:** CSV already exists but header mismatch
  - **CRITICAL:** Worker futures raise exceptions (caught but logged)
  - Empty country shard
  - CSV write permissions denied
  - Concurrent write to same CSV (no locking)

#### `parse_args()`
- **Parameters:** None (reads sys.argv)
- **Returns:** argparse.Namespace
- **Uncovered Paths:**
  - No arguments provided (uses defaults)
  - Invalid argument types (e.g., "abc" for job_index)

#### `shard_tif_dict(tif_dict, job_index, total_jobs, seed)`
- **Parameters:** `tif_dict: dict`, `job_index: int`, `total_jobs: int`, `seed: int`
- **Returns:** `dict` (sharded subset)
- **Uncovered Paths:**
  - `total_jobs < 1` (raises ValueError)
  - `job_index` out of range (raises ValueError)
  - Empty `tif_dict`

#### `main()`
- **Parameters:** None (reads config and sys.argv)
- **Returns:** None (writes outputs)
- **Uncovered Paths:**
  - Config load fails
  - Voronoi filepath doesn't exist (FileNotFoundError not caught)
  - Watershed file missing and Overture download fails

---

## 3. piechart_figure.py

### Core Functions

#### `aggregate_by_country(gdf, country_column, agg_column, industrial_column=None, is_pop=False)`
- **Parameters:** `gdf: geopandas.GeoDataFrame`, `country_column: str`, `agg_column: str`, `industrial_column: str | None`, `is_pop: bool = False`
- **Returns:** `pd.DataFrame`
- **Uncovered Paths:**
  - `is_pop=False` but `industrial_column=None` (raises ValueError)
  - All values are NaN in agg_column
  - Empty dataframe after dropna()
  - **CRITICAL:** industrial_column has values other than True/False (groupby behavior undefined)

#### `plot_splitted_piechart(dist_tag1, dist_tag2, ax, size_tag1, size_tag2, min_size, labels=False, labels_text=['Paved', 'Unpaved', ''], cmap="tab20c")`
- **Parameters:** Two distribution lists, axis, sizes, labels, colormap
- **Returns:** None (modifies axis)
- **Uncovered Paths:**
  - `dist_tag1` or `dist_tag2` are empty
  - `size_tag1` or `size_tag2` are zero or negative
  - `sum(dist_tag1)/2 < min_size` (skips pie rendering)
  - Invalid colormap name

#### `get_pos(geometry)`
- **Parameters:** `geometry: shapely.BaseGeometry`
- **Returns:** `tuple[float, float]` (x, y)
- **Uncovered Paths:**
  - Polygon or MultiPolygon with area=0
  - **CRITICAL:** Invalid geometry type (raises ValueError)
  - MultiPolygon with all parts empty

#### `calculate_size(value, min_value, max_value, min_size, max_size, scale='log')`
- **Parameters:** Numeric value, bounds, size range, scale method
- **Returns:** `float` (scaled size)
- **Uncovered Paths:**
  - `value`, `min_value`, or `max_value` are non-finite
  - `max_value <= min_value` (returns average size)
  - `scale='log'` but values are ≤ 0 (returns min_size)
  - **CRITICAL:** `scale` value not 'log' or 'linear' (raises ValueError)
  - Extremely small log differences (underflow)

#### `round_numbers(arr, breaks)`
- **Parameters:** `arr: np.ndarray`, `breaks: list` (number of breaks)
- **Returns:** `list` (rounded break values)
- **Uncovered Paths:**
  - `arr` all non-finite (arr.size == 0)
  - Empty breaks list
  - Single element in arr

#### `ensure_population_percentage_column(df, preferred_col="population_served_index", zonal_sum_col="2024_zonal_sum")`
- **Parameters:** `df: pd.DataFrame`, `preferred_col: str`, `zonal_sum_col: str`
- **Returns:** `str` (column name)
- **Uncovered Paths:**
  - None of the expected columns present (raises KeyError)
  - **CRITICAL:** population_total is 0 (replaced with NaN)
  - **CRITICAL:** population_served/zonal_sum_col are negative

#### `resolve_zonal_sum_columns(df, preferred)`
- **Parameters:** `df: pd.DataFrame`, `preferred: str`
- **Returns:** `str` (column name)
- **Uncovered Paths:**
  - No columns matching '*_zonal_sum' pattern (raises KeyError)
  - `preferred` not in columns
  - Year extraction fails (ValueError in int())

#### `main()`
- **Parameters:** None (reads config, loads files)
- **Returns:** None (writes figure)
- **Uncovered Paths:**
  - Config load fails
  - Population filepath doesn't exist
  - Stats filepath missing (FileNotFoundError)
  - Invalid zonal_sum column after resolution
  - Boundary geometries not valid (plotting fails)
  - No valid size data (all NaN or zero)

---

## 4. download_bing_annotate.py

### Core Functions

#### `safe_wkt_load(wkt_wtr)`
- **Parameters:** `wkt_wtr: str | None`
- **Returns:** `shapely.BaseGeometry | None`
- **Uncovered Paths:**
  - Invalid WKT string (shapely.wkt.loads raises Exception)
  - `wkt_wtr=None` or not a string
  - Empty string

#### `download_bing_image(center_lon, center_lat)`
- **Parameters:** `center_lon: float`, `center_lat: float`
- **Returns:** `PIL.Image` (RGB)
- **Uncovered Paths:**
  - **CRITICAL:** HTTP 401/403 errors (API key invalid - raises Exception)
  - Network timeout (timeout=15)
  - Bing returns non-image content

#### `download_random_image(center_lon, center_lat)`
- **Parameters:** `center_lon: float`, `center_lat: float`
- **Returns:** `PIL.Image` (black image)
- **Uncovered Paths:**
  - Never called in production (test-only fallback)

#### `get_image(idx, images_dir)`
- **Parameters:** `idx: int | str`, `images_dir: str`
- **Returns:** `PIL.Image | None`
- **Unccovered Paths:**
  - File exists but is corrupted (Image.open raises Exception)
  - Partial file exists (truncated PNG)

#### `mercator_to_pixel(x, y, cx, cy, IMAGE_SIZE, wrap=True)`
- **Parameters:** Web Mercator coords, center, image size, wrap flag
- **Returns:** `tuple[int, int]` (pixel coordinates)
- **Uncovered Paths:**
  - **CRITICAL:** Coordinate wrapping at International Date Line
  - Very large dx/dy values (rounding overflow)
  - `wrap=False` with coordinates outside ±WORLD_CIRCUMFERENCE/2

#### `image_bounds_mercator(center_lon, center_lat)`
- **Parameters:** `center_lon: float`, `center_lat: float`
- **Returns:** `tuple[float, float, float, float, float]` (xmin, ymin, xmax, ymax, res)
- **Uncovered Paths:**
  - Invalid coordinates

#### `draw_text_with_padding(draw, xy, text, font, fill, pad_fill, pad=2)`
- **Parameters:** ImageDraw, position, text, font, colors, padding
- **Returns:** None (modifies image)
- **Uncovered Paths:**
  - Invalid xy coordinates
  - `text` is None or empty

#### `draw_rotated_text_with_padding(image, xy, text, angle, font, fill, pad_fill, pad=2)`
- **Parameters:** Image, position, text, rotation angle, font, colors
- **Returns:** None (modifies image)
- **Uncovered Paths:**
  - `angle > 180` or `angle < -180` (behavior undefined)
  - Very large pad_dim (memory allocation)
  - Font rendering fails

#### `linestring_angle(line)`
- **Parameters:** `line: shapely.LineString`
- **Returns:** `float` (angle in degrees)
- **Uncovered Paths:**
  - LineString with 0 or 1 vertices (IndexError on line.coords[-1])
  - Zero-length line (atan2(0, 0))

#### `log_gdf_preview(name, gdf, columns, n=5)`
- **Parameters:** `name: str`, `gdf: geopandas.GeoDataFrame`, `columns: list`, `n: int = 5`
- **Returns:** None (logs)
- **Uncovered Paths:**
  - Empty GeoDataFrame
  - No matching columns
  - Very large GeoDataFrame (head(n) performance)

#### `split_grids_for_instance(grids, instance_id, num_instances=10, split_seed=42)`
- **Parameters:** Grid list, worker index, total workers, random seed
- **Returns:** `list` (sharded grids)
- **Uncovered Paths:**
  - `num_instances <= 0` (raises ValueError)
  - `instance_id >= num_instances` (raises ValueError)
  - Empty grids list

#### `draw_annotations(image, annotations, fontsize=12)`
- **Parameters:** `image: PIL.Image`, `annotations: list[dict]`, `fontsize: int`
- **Returns:** `PIL.Image` (modified, RGBA)
- **Uncovered Paths:**
  - No matching font file ('dejavu-sans.book.ttf')
  - Annotations list is empty
  - **CRITICAL:** Mixed annotation styles (line vs poly) - ordering matters

#### `georef_write(image, center_lon, center_lat, out_path)`
- **Parameters:** Image, center coords, output path
- **Returns:** None (writes GeoTIFF)
- **Uncovered Paths:**
  - Invalid output path (permission denied)
  - Parent directory doesn't exist

#### `process_bbox(idx, bbox_geom, img_idx, poly_gdf, cols, line_gdf, line_cols, output_dir, images_dir)`
- **Parameters:** Grid cell, geometries, feature tables, output directories
- **Returns:** `tuple[int, int, str | None]` (idx, annotation_count, error_message)
- **Uncovered Paths:**
  - Image not found (returns early with error message)
  - No intersecting polygons or lines
  - Clipped geometry is empty after intersection
  - **CRITICAL:** mercator_to_pixel returns out-of-bounds coordinates

#### `annotate_bboxes_parallel(bbox_gdf, poly_gdf, cols, line_gdf, line_cols, output_dir, images_dir, files)`
- **Parameters:** GeoDataFrames, column lists, output directories, unused files
- **Returns:** None (spawns threads, writes outputs)
- **Uncovered Paths:**
  - ThreadPoolExecutor max_workers exceeded
  - Futures raise exceptions (logged but continues)

---

## 5. impact_polygons_pop.py (Environmental Impact Modeling)

### Core Functions

#### `create_dicts(river_gdf, next_col, id_col, main_riv_col, discharge_col, weight_col='weight')`
- **Parameters:** River network GeoDataFrame, column names, weight column
- **Returns:** None (initializes globals: next_dict, geom_dict, lat_dict, level_dict, discharge_dict)
- **Uncovered Paths:**
  - Missing columns in river_gdf
  - Rows with NaN in id_col or next_col (dropped)
  - NaN weight values (filled with 0.0)
  - **CRITICAL:** UTM zone value is NaN (skip in geom_dict update)
  - Empty upstream_adj (no tributaries)
  - **CRITICAL:** Multiple basins with same MAIN_RIV (should not happen but not validated)

#### `init_worker(shared_next, shared_geom, shared_lat, shared_level, shared_dis)`
- **Parameters:** Global dictionaries (shared between processes)
- **Returns:** None (initializes worker-local globals)
- **Uncovered Paths:**
  - Called outside ProcessPoolExecutor context

#### `get_runtime_params(cfg)`
- **Parameters:** `cfg: dict | Any` (config object)
- **Returns:** `dict` (validated parameters with defaults)
- **Uncovered Paths:**
  - `cfg` is not a dict (wrapped in isinstance check)
  - Invalid parameter values (ValueError caught, uses default)
  - `impact_radii` is not a list
  - Negative parameter values not validated

#### `batch_estimate_utm_epsg(gdf)`
- **Parameters:** `gdf: geopandas.GeoDataFrame` (must have geometry)
- **Returns:** `tuple[np.ndarray, np.ndarray]` (epsg_codes, lats)
- **Uncovered Paths:**
  - **CRITICAL:** Latitudes > 84 or < -80 (set to 3857)
  - Longitudes outside ±180 (set to 3857)
  - Empty GeoDataFrame

#### `calculate_load_ratio(pop, dis_av_cms, org_per_pop=60.0, c_limit=5.0, least_discharge_cms=0.269, load=None)`
- **Parameters:** Population or load, discharge, parameters
- **Returns:** `float | np.ndarray | pd.Series` (normalized load ratio)
- **Uncovered Paths:**
  - `pop=None` and `load=None` (returns 0)
  - `dis_av_cms=None` (set to least_discharge_cms)
  - **CRITICAL:** Series input with NaN values (fillna in vectorized path)
  - Zero discharge edge case (converts to least_discharge_cms)
  - **CRITICAL:** Vectorized mode with mismatched array shapes

#### `invert_calculate_load(load_ratio, c_limit=5.0)`
- **Parameters:** `load_ratio: float`, `c_limit: float`
- **Returns:** `float` (absolute concentration load)
- **Uncovered Paths:**
  - Negative load_ratio

#### `calculate_radius(load_ratio, impact_radius=1000)`
- **Parameters:** `load_ratio: float`, `impact_radius: float = 1000`
- **Returns:** `float` (0.0 or impact_radius)
- **Uncovered Paths:**
  - `load_ratio < 1` (returns 0.0 - not testable path?)

#### `calculate_kt(lat, base_k=0.23, theta=1.047)`
- **Parameters:** Latitude, base decay coefficient, temperature factor
- **Returns:** `float` (temperature-adjusted decay coefficient)
- **Uncovered Paths:**
  - `lat` outside ±90 range
  - Very extreme latitudes (cos underflow)

#### `generate_single_segment_plume(rid, lat, start_load_ratio=None, step_m=100.0, c_limit=5.0, base_k=0.23, theta=1.047, impact_radii=[1000, 2000])`
- **Parameters:** River segment ID, latitude, load, parameters
- **Returns:** `tuple[list[Polygon] | None, float]` (plumes, exit_load)
- **Uncovered Paths:**
  - `rid` not in next_dict, geom_dict, or discharge_dict (returns None, 0.0)
  - `start_load_ratio < 0` or NaN
  - **CRITICAL:** Plume dies at first point (stop_idx < 2)
  - Zero-length segment (seg_len == 0)
  - **CRITICAL:** Polygon construction from coords fails
  - Division by zero in tangent normalization (caught with if norms == 0)

#### `create_impact_polygons(pop_chunk, main_riv, nxt_dis_col, model_params=None)`
- **Parameters:** Population subset, basin ID, column name, parameters
- **Returns:** `dict[radius, geopandas.GeoDataFrame]` (results per radius)
- **Uncovered Paths:**
  - Empty pop_chunk (returns {})
  - No levels for basin (returns {})
  - Exception during generate_single_segment_plume (caught, returns {})
  - **CRITICAL:** No matching rid in level_dict
  - Downstream segment has zero discharge (clamped to least_discharge)

#### `parallel_dissolve(subset_df, crs_code)`
- **Parameters:** DataFrame with geometry, CRS code
- **Returns:** `geopandas.GeoDataFrame` (dissolved, reprojected to 4326)
- **Uncovered Paths:**
  - Empty subset_df (returns empty GeoDataFrame)
  - Invalid crs_code (int conversion fails)
  - Dissolve operation fails (no exception handling)

#### `orchestrate_logic(pop_gdf, nxt_dis_col, main_riv_col, max_workers, model_params=None)`
- **Parameters:** Population data, column names, worker count, parameters
- **Returns:** `dict[radius, geopandas.GeoDataFrame] | None`
- **Uncovered Paths:**
  - No plumes generated (STAGE 1 failure, returns None)
  - Dissolve fails for all UTM groups (STAGE 2)
  - **CRITICAL:** Combined_gdf is empty after filtering (continue loop)
  - Buffer/dissolve standard path fails (attempts robust union fallback)
  - Robust union also fails (no recovery)
  - Final gdfs is empty (returns None)

#### `main()`
- **Parameters:** None (reads sys.argv, config)
- **Returns:** None (writes GeoPackage files)
- **Uncovered Paths:**
  - max_workers not provided (defaults to 64)
  - Config load fails
  - Datasets don't exist (gpd.read_file raises exception)
  - No matching MAIN_RIV in datasets (empty after merge)
  - orchestrate_logic returns None

---

## Summary of HIGHEST PRIORITY Uncovered Areas

### 598 Missing Statements Primary Locations (create_voronoi.py)

The 598 missing statements in create_voronoi.py are concentrated in:

1. **orchestrate_voronoi_weights()** (~200-250 statements)
   - Multi-worker parallel processing orchestration
   - Complex worker result merging and error handling
   - Conditional paths for different clipping modes

2. **weighted_voronoi()** (~150-200 statements)
   - Grid generation and Voronoi diagram computation
   - Site assignment and polygon extraction
   - Buffer dissolution and overlap resolution

3. **calculate_buffer()** (~50-100 statements)
   - Complex parameter handling (*args, **kwargs)
   - Weight-based buffer calculation logic

4. **dissolve_overlapping_geometries()** (~80-120 statements)
   - Nested loop latitude/longitude grouping
   - DFS-based graph traversal and connected components

5. **assign_sites_streaming()** (~30-50 statements)
   - Vectorized distance calculation
   - Site-to-polygon assignment logic

### Across All Files (Test-First Candidates)

1. **create_voronoi.py - normalize_plane()**: Denominator clamping to 1 when max==min
2. **create_voronoi.py - cluster_point_indices()**: Empty/single-point edge cases
3. **create_voronoi.py - create_ranges()**: Infinite loop risk when min_step is very small
4. **create_voronoi.py - default_distance_multiplicative()**: Division by zero (weight=0)
5. **create_rasters.py - extract_worldpop_universal()**: Complex raster window processing with basin masking
6. **download_bing_annotate.py - mercator_to_pixel()**: World wrapping logic at date line
7. **impact_polygons_pop.py - generate_single_segment_plume()**: Plume dies mid-segment (stop_idx < 2)
8. **piechart_figure.py - calculate_size()**: Non-finite value handling in log scale

### Missing Full Signatures (Need Additional Read)

- `create_voronoi.py - intersect_with_polygon_sindex()` - requires file read to line ~900+
- `create_voronoi.py` - remaining ~200+ statements (SECTION 7, 8, 9)

---

## Recommended Test Structure

### Unit Tests (by complexity/coverage gap)

1. Geometry validation edge cases (is_valid_geom, buffer_geometry, create_centroid_points)
2. Clustering algorithms (cluster_point_indices, cluster_points)
3. Distance metrics (default_distance_additive, default_distance_multiplicative, normalize_plane)
4. Coordinate transformations (estimate_utm_epsg, estimate_utm_crs)
5. Population extraction (extract_worldpop_universal with window boundaries)
6. Impact modeling (generate_single_segment_plume, create_impact_polygons)
7. Annotation rendering (mercator_to_pixel, draw_annotations)

### Integration Tests

1. End-to-end country raster processing (orchestrate_intersections)
2. Multi-worker basin impact polygon generation
3. Figure generation with missing/invalid data

---

## CRITICAL TESTING GAPS - Exact Function Signatures for Test Implementation

### Pattern 1: Boundary Condition Testing

These functions need explicit boundary tests:

```python
# normalize_plane() - when max == min
assert normalize_plane(np.array([[1, 2]]), (1, 2)) == (np.array([[0, 0]]), np.array([0, 0]))

# create_ranges() - when step > range
create_ranges(1, 2, 100)  # should adaptively reduce step

# calculate_size() - when min_value == max_value
calculate_size(5, 5, 5, 0.1, 1.0, scale='log')  # returns (0.1 + 1.0)/2
```

### Pattern 2: Division-by-Zero Edge Cases

```python
# default_distance_multiplicative - weight=0
default_distance_multiplicative(np.array([[0, 1]]), (0, 0), 0, 1)  # CRASHES

# mercator_to_pixel - dx/dy rounding at world wrap
mercator_to_pixel(20037508.34, 0, 0, 0, [3072, 3072], wrap=True)
```

### Pattern 3: Worker Failure Scenarios

```python
# orchestrate_logic - partial worker failures
# Create scenarios where some basin workers fail, others succeed
# Expected: partial output GeoDataFrame or None

# annotate_bboxes_parallel - thread executor errors
# Mock ThreadPoolExecutor.submit to raise exceptions
# Expected: continues processing, logs errors
```

### Pattern 4: File I/O & Resource Cleanup

```python
# intersect_with_polygons_db - cleanup on DuckDB error
# Expected: temp DB file is removed even if query fails

# download_overture_maps - network failures
# Mock requests to raise Timeout, ConnectionError
# Expected: logged warnings, graceful fallback
```

### Pattern 5: Geometry Edge Cases

```python
# extract_contours_scipy/cv2/rasterio - empty or all-zero mask
# Expected: returns None or empty polygon list

# dissolve_overlapping_geometries - deep recursion
# Create scenario with 10K+ nested components
# Expected: respects recursion_lim parameter
```

### Pattern 6: Complex Parameter Interactions

```python
# calculate_buffer(df, weights, *args, **kwargs)
# Signature suggests flexible parameter handling
# Need to document what args/kwargs are supported

# orchestrate_voronoi_weights(..., distance_fn=custom_function)
# Test with: default_distance_multiplicative, default_distance_additive, custom lambdas
# Expected: all distance functions properly vectorized
```

---

## File-Specific Testing Priorities

### create_voronoi.py (598 missing statements)
- **MUST TEST:** normalize_plane, default_distance_multiplicative, orchestrate_voronoi_weights
- **SHOULD TEST:** dissolve_overlapping_geometries, weighted_voronoi, calculate_buffer
- **NICE-TO-TEST:** estimate_utm_epsg, auto_weight_scale, create_ranges

### create_rasters.py
- **MUST TEST:** extract_worldpop_universal (window boundary conditions), orchestrate_intersections (concurrent writes)
- **SHOULD TEST:** polygon_raster_sign_from_gdf, shard_tif_dict
- **NICE-TO-TEST:** geotiff_exists_and_valid, _sanitize_polygon_geom

### download_bing_annotate.py
- **MUST TEST:** mercator_to_pixel (world wrap), process_bbox (out-of-bounds coordinates)
- **SHOULD TEST:** draw_rotated_text_with_padding, split_grids_for_instance
- **NICE-TO-TEST:** safe_wkt_load, linestring_angle

### impact_polygons_pop.py
- **MUST TEST:** generate_single_segment_plume (plume dies mid-segment), create_impact_polygons (basin topology)
- **SHOULD TEST:** calculate_load_ratio (vectorized vs scalar), orchestrate_logic (partial failures)
- **NICE-TO-TEST:** batch_estimate_utm_epsg, init_worker

### piechart_figure.py
- **MUST TEST:** calculate_size (non-finite values, log scale edge cases), aggregate_by_country (industrial column logic)
- **SHOULD TEST:** ensure_population_percentage_column (column resolution), resolve_zonal_sum_columns
- **NICE-TO-TEST:** get_pos (MultiPolygon areas), round_numbers (empty arrays)

---

## Additional Notes for Test Implementation

1. **Mock External Dependencies:**
   - requests library for download_bing_image
   - DuckDB for all DuckDB operations (use in-memory :memory: or temp files)
   - rasterio for GeoTIFF operations
   - Threading/ProcessPoolExecutor for concurrent tests

2. **Use Fixtures for Common Data:**
   - Valid/invalid GeoDataFrames with various geometry types
   - Sample rasters (small numpy arrays with GeoTIFF metadata)
   - Configuration dictionaries (with and without required keys)

3. **Test Parametrization Opportunities:**
   - Distance functions: multiplicative vs additive
   - Geometry types: Point, Polygon, LineString, MultiX, GeometryCollection
   - CRS systems: EPSG:4326, UTM zones, invalid codes
   - Missing/invalid data patterns: None, NaN, empty, corrupted

4. **Performance Regression Tests:**
   - cluster_point_indices with 100K+ points
   - extract_worldpop_universal with large rasters
   - dissolve_overlapping_geometries with deep recursion
   - orchestrate_voronoi_weights with many workers



