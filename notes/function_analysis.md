# Function/Method Extraction & Complexity Analysis

> **Stale in places.** This is a point-in-time analysis snapshot. Since it was
> written, `create_voronoi.dissolve_overlapping_geometries` (the slow recursive
> variant) has been deleted along with its `recursion_lim` config key — only
> `dissolve_overlapping_geometries_fast` remains — and several helpers have moved
> into `src/geo_utils.py` / `src/utils.py`. Verify against the code before acting
> on any entry here.

## src/create_voronoi.py

### COMPLEXITY RANKING (Most Complex â†’ Least Complex)

---

#### 1. **weighted_voronoi**
- **Signature:** `(df, col, country_clip, scale_weights=False, clipping=None, n_points=100, distance_fn=default_distance_multiplicative, scipy_true=False, cv2_true=False, centroid_points=False, buffering=False, threshold=500, calculate_buffer_fn=calculate_buffer, buffer_fn_kwargs=None, site_id_col='WASTE_ID')`
- **Line Range:** 2085â€“2357
- **Complexity:** âš ï¸âš ï¸âš ï¸ EXTREMELY HIGH
- **Indicators:**
  - 14 sequential phases (CRS validation, site preprocessing, weight initialization, grid generation, masking, assignment, boundary extraction, overlap resolution, clipping, standardization)
  - 3 nested for loops (grid point assignment, contour extraction per site, boundary clipping)
  - 5 conditional branches (single vs multi-site handling, contour method selection, buffering logic)
  - Multiple external function calls (cluster_points, estimate_utm_crs, geometry_contains_points, assign_sites_streaming, resolve_polygon_overlaps, gpd.clip)
  - Array reshaping and masking operations
  - Dynamic dispatch to 3 different contour extraction methods

---

#### 2. **calculate_buffer**
- **Signature:** `(df, weights, *args, **kwargs)`
- **Line Range:** 1567â€“1822
- **Complexity:** âš ï¸âš ï¸âš ï¸ VERY HIGH
- **Indicators:**
  - 3+ nested functions (_size_ceiling, _compute_k, _site_detection_counts, _detection_confidence)
  - 2 major conditional branches (fixed vs dynamic buffering)
  - Loop over all sites (for i, (_, row) in enumerate(df.iterrows()))
  - Complex k-value computation with blended signals (density + sophistication with confidence gating)
  - 8+ nested if-elif logic for area ceiling thresholds
  - Fallback handling (basin median, detection confidence thresholds)
  - Multiple array operations (np.clip, np.isfinite, np.where)

---

#### 3. **orchestrate_voronoi_weights**
- **Signature:** `(df, col, country_df, workers=12, scale_weights=False, clipping=None, n_points=100, distance_fn=default_distance_multiplicative, scipy_true=False, cv2_true=False, centroid_points=False, buffering=False, threshold=500, sigma=3, percent_threshold=10, area_fn=None, area_fn_kwargs=None, method='linear', output_path=None, overwrite=False, flush_size=None, calculate_buffer_fn=calculate_buffer, buffer_fn_kwargs=None, site_country_col='ISO_2', country_boundary_col='country', site_id_col='WASTE_ID')`
- **Line Range:** 2462â€“2807
- **Complexity:** âš ï¸âš ï¸âš ï¸ VERY HIGH
- **Indicators:**
  - ProcessPoolExecutor with dynamic task batching
  - Nested loop: for each group â†’ extract country boundaries â†’ create parallel tasks
  - Checkpoint resume logic with conditional file I/O
  - Generator function (iter_voronoi_args) with batch buffering
  - Nested closure (flush_results) with nonlocal state management
  - DuckDB integration for checkpoint queries
  - Multiple groupby and filter operations
  - Complex state tracking (task_stats dict)
  - Parallel result collection with as_completed

---

#### 4. **intersect_with_polygons_parallelized**
- **Signature:** `(df, polygons, cols, use_duckdb=False, max_workers=16, df_join_col='ISO_2', polygon_join_col='ISO_2')`
- **Line Range:** 987â€“1065
- **Complexity:** âš ï¸âš ï¸âš ï¸ VERY HIGH
- **Indicators:**
  - UTM zone partitioning with nested loop (for utm in unique_utms)
  - Conditional dual-path execution (DuckDB vs spatial indexing)
  - 3+ nested for loops (unique UTMs, cols extraction, nested function calls)
  - Separate handling of valid vs invalid geometries
  - Multiple CRS conversions and resets
  - GeoDataFrame concatenation logic
  - Fallback handling for empty results

---

#### 5. **dissolve_overlapping_geometries**
- **Signature:** `(subdf, radius, convex=False, recursion_lim=50000)`
- **Line Range:** 1161â€“1312
- **Complexity:** âš ï¸âš ï¸âš ï¸ VERY HIGH
- **Indicators:**
  - 2 separate sorting + grouping passes (by longitude, then latitude)
  - 2 tqdm-wrapped for loops with conditional updates
  - DFS (depth-first search) graph traversal for connected components
  - Dictionary-based graph construction (defaultdict with bidirectional edges)
  - Complex boundary-box matching logic (lat/lon comparisons within loops)
  - Set operations for intersection grouping
  - Recursion limit management
  - Multiple array creation and indexing operations

---

#### 6. **orchestrate_overlaps**
- **Signature:** `(df, max_workers, buffers_filepath, radius, convex=False, country_col='ISO_2')`
- **Line Range:** 1379â€“1479
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - ProcessPoolExecutor with as_completed() loop
  - Country-based partitioning
  - Checkpoint/cache file management
  - Error collection and reporting (error_count tracking)
  - Multiple conditional branches (cache check, result merging, file I/O)
  - Dictionary mapping (final_dict) from group memberships to group IDs
  - Dissolve operation with area-based weighting

---

#### 7. **assign_sites_streaming**
- **Signature:** `(valid_points, points, weights, distance_fn, factor)`
- **Line Range:** 2038â€“2083
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - Nested loop: for each site, compute distance to all grid points
  - Two loops (site loop + implicit point loop via vectorized distance)
  - Mask-based array updates (np.sum, best_distances, assignments)
  - Vectorized distance computation with streaming updates

---

#### 8. **intersect_with_polygons_db**
- **Signature:** `(df, polygons, cols, df_join_col='ISO_2', polygon_join_col='ISO_2')`
- **Line Range:** 882â€“985
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - Complex DuckDB spatial SQL query construction
  - WKT serialization/deserialization loop
  - Helper function (_quote_identifier) for SQL escaping
  - Multiple CRS conversions (to_crs)
  - Separate handling of valid/invalid geometries
  - Try-finally block with temp file cleanup
  - Multiple conditional checks (crs None, polygon_join_col validation)
  - CTE-based SQL query (WITH data AS, WITH polygons AS)

---

#### 9. **intersect_with_polygon_sindex**
- **Signature:** `(df, polygons, col, concurrency=False)`
- **Line Range:** 821â€“880
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - ThreadPoolExecutor conditional execution
  - R-tree spatial index construction and querying
  - Argument list generation (args_list comprehension)
  - Two parallel paths (concurrent vs sequential) with list comprehension
  - Separate handling of NaN/invalid geometries
  - Buffer geometry operations
  - Centroid computation and concatenation

---

#### 10. **create_weights**
- **Signature:** `(sub_df, sigma=3, percent_threshold=10, method='linear')`
- **Line Range:** 2388â€“2460
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - 4 conditional transformation methods (logarithmic, square_root, sigmoid, linear)
  - Nested sigmoid computation with Z-score normalization
  - 2+ normalization passes (initial + clipping + re-normalization)
  - Standard deviation and median calculations with fallback
  - Clipping logic with conditional bounds (upper/lower thresholds)
  - Series operations with NaN handling

---

#### 11. **dissolve_overlapping_geometries_fast**
- **Signature:** `(subdf, radius, convex=False)`
- **Line Range:** 1314â€“1377
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - Spatial index intersection query
  - NetworkX connected components extraction
  - Bounding box/buffer geometry logic
  - Two separate geometry preparation branches (convex vs buffer)
  - UTM CRS estimation and conversion
  - Component-based grouping with set operations

---

#### 12. **cluster_point_indices**
- **Signature:** `(geoms, threshold)`
- **Line Range:** 284â€“329
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - K-D tree construction and query_ball_point() call
  - Union-Find algorithm with path compression
  - 2 for loops (Union-Find union operations, cluster building)
  - Coordinate extraction from Point geometries
  - Multiple logging calls with computed statistics (min/max/mean)

---

#### 13. **estimate_utm_crs**
- **Signature:** `(gdf)`
- **Line Range:** 572â€“635
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - Multiple fallback attempts (centroid â†’ valid Point â†’ valid polygon)
  - Complex conditional logic with nested if-elif chains
  - 3 separate CRS validation tries (try-except blocks)
  - Geometry filtering (is_valid, notna, is_empty checks)
  - Finite-value checking (np.isfinite for lon/lat)
  - Multiple return paths with different CRS fallbacks

---

#### 14. **calculate_area**
- **Signature:** `(df, only_round=False)`
- **Line Range:** 637â€“700
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - Regex parsing of diameter values (re.findall)
  - Nested area calculation (sum of circle areas)
  - Multiple column transformations and aggregations
  - Conditional logic (only_round flag)
  - Numeric parsing with error handling (pd.to_numeric, astype(int), clip)
  - Complex detection logic (num_detections = circles + rects, with fallback means)

---

#### 15. **nearest_neighbor_distances_and_median**
- **Signature:** `(df)`
- **Line Range:** 415â€“465
- **Complexity:** âš ï¸âš ï¸ HIGH
- **Indicators:**
  - K-D tree construction
  - Conditional k value selection (k=3 or k=2 based on point count)
  - Type-based geometry handling (Point vs LineString/Polygon branches)
  - 2 loops (coordinate extraction, k-nearest averaging)
  - Array operations (np.nanmean, np.nanmedian)
  - Multiple fallback returns (empty array, single point NaN)

---

#### 16. **voronoi_worker**
- **Signature:** `(args)`
- **Line Range:** 2359â€“2387
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - Tuple unpacking with error handling
  - Wrapper that delegates to weighted_voronoi
  - Try-except with detailed error logging

---

#### 17. **orchestrate_overlaps** (already detailed above)

---

#### 18. **process_centroid**
- **Signature:** `(args)`
- **Line Range:** 772â€“819
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - Tuple unpacking
  - Spatial index intersection query
  - Try-except for precise intersection matching
  - Boolean masking and filtering
  - Multiple conditional branches

---

#### 19. **intersects_with_country_db**
- **Signature:** `(df, filepath, polygon_country_col='country', output_country_col='ISO_2')`
- **Line Range:** 1068â€“1159
- **Complexity:** âš ï¸ MODERATE-HIGH
- **Indicators:**
  - Complex DuckDB spatial SQL with bounding box pre-filtering
  - WKT serialization/deserialization
  - CRS validation and conversion
  - LEFT JOIN with 4-part spatial condition
  - Helper function for SQL escaping

---

#### 20. **resolve_polygon_overlaps**
- **Signature:** `(region_polygons)`
- **Line Range:** 1481â€“1530
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - 2 nested for loops (pairwise geometry comparison)
  - Conditional area comparison (polygon.area)
  - Geometry difference operations
  - Array mutation in-place

---

#### 21. **download_overture_maps**
- **Signature:** `(url, filepath)`
- **Line Range:** 730â€“770
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - DuckDB SQL string assembly (2 separate queries)
  - Directory creation
  - Try-except error handling
  - Parquet file I/O via DuckDB

---

#### 22. **create_ranges**
- **Signature:** `(x, y, step, min_step=100)`
- **Line Range:** 379â€“413
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - While loop with adaptive step reduction
  - 2 conditional branches (step adjustment logic)
  - np.linspace and array operations

---

#### 23. **cluster_points**
- **Signature:** `(df, threshold)`
- **Line Range:** 330â€“377
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - Calls cluster_point_indices (delegates complexity)
  - Loop over cluster sets with conditional aggregation
  - Multiple DataFrame manipulations (concat, drop columns)
  - Null-counting and column aggregation

---

#### 24. **initialize_voronoi_weights**
- **Signature:** `(df, distance_fn, scale_weights, points)`
- **Line Range:** 1823â€“1872
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - Conditional distance function dispatch (additive vs multiplicative)
  - 2 major branches (scale_weights True/False)
  - Weight scaling computation

---

#### 25. **extract_contours_scipy**
- **Signature:** `(region_mask_2d, n_points, grid_minx, grid_miny)`
- **Line Range:** 1874â€“1918
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - find_contours algorithm from scipy
  - Loop over contours with coordinate transformation
  - Polygon closing and validation
  - Buffer geometry operations

---

#### 26. **extract_contours_cv2**
- **Signature:** `(region_mask_2d, n_points, grid_minx, grid_miny)`
- **Line Range:** 1920â€“1969
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - OpenCV contour detection (cv2.findContours)
  - Loop with ndim and shape validation
  - Coordinate transformation
  - Polygon creation and buffering

---

#### 27. **extract_contours_rasterio**
- **Signature:** `(region_mask_2d, n_points, grid_minx, grid_miny)`
- **Line Range:** 1971â€“2007
- **Complexity:** âš ï¸ MODERATE
- **Indicators:**
  - rasterio.features.shapes() conversion
  - Loop over shapes with affine transforms
  - Geometry scaling and translation
  - Validation and buffering

---

#### 28. **extract_site_coordinates**
- **Signature:** `(df, centroid_points)`
- **Line Range:** 1532â€“1565
- **Complexity:** ðŸŸ¡ LOW-MODERATE
- **Indicators:**
  - Loop over geometries
  - Type checking (Point vs Line/Polygon)
  - Centroid extraction

---

#### 29. **is_valid_geom**
- **Signature:** `(geom)`
- **Line Range:** 161â€“189
- **Complexity:** ðŸŸ¡ LOW-MODERATE
- **Indicators:**
  - Try-except with multiple fallbacks
  - 4 sequential if checks
  - Coordinate iteration

---

#### 30. **geometry_contains_points**
- **Signature:** `(geometry, points)`
- **Line Range:** 98â€“121
- **Complexity:** ðŸŸ¡ LOW-MODERATE
- **Indicators:**
  - 3 nested try-except blocks (graceful degradation)
  - Vectorized shapely operations
  - Fallback to Point creation loop

---

#### 31. **normalize_plane**
- **Signature:** `(a, b)`
- **Line Range:** 134â€“159
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - NumPy min/max/denom operations
  - np.where conditional normalization
  - Simple array math

---

#### 32. **auto_weight_scale**
- **Signature:** `(points)`
- **Line Range:** 467â€“489
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - pdist and squareform distance matrix computation
  - np.fill_diagonal NaN insertion
  - np.nanmin and np.nanmean

---

#### 33. **normalize_column_to_rounded_str**
- **Signature:** `(series)`
- **Line Range:** 702â€“728
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - pd.to_numeric conversion
  - Simple rounding and type casting
  - NaN preservation

---

#### 34. **finalize_gdf**
- **Signature:** `(df_list, cols)`
- **Line Range:** 2009â€“2036
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - Conditional concatenation
  - GeoDataFrame constructor
  - Buffer geometry operation

---

#### 35. **buffer_geometry**
- **Signature:** `(geom)`
- **Line Range:** 213â€“242
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - Multiple if-elif branches (Point vs LineString vs Polygon)
  - Zero-buffer operation for polygons
  - Simple type dispatch

---

#### 36. **drop_duplicates**
- **Signature:** `(df, col)`
- **Line Range:** 191â€“211
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - Conditional DataFrame filtering
  - drop_duplicates() method call
  - NaN preservation with concat

---

#### 37. **create_centroid_points**
- **Signature:** `(geom)`
- **Line Range:** 244â€“282
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - Type checking and dispatch (Point vs Polygon vs LineString)
  - Centroid extraction
  - Multiple fallback return values

---

#### 38. **default_distance_additive**
- **Signature:** `(a, b, weight, factor)`
- **Line Range:** 491â€“513
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - normalize_plane call
  - Simple array math (sum of squares)
  - np.sqrt and np.where operations

---

#### 39. **estimate_utm_epsg**
- **Signature:** `(lon, lat)`
- **Line Range:** 537â€“570
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - Zone calculation from lon
  - Simple hemisphere logic
  - EPSG code lookup with fallback

---

#### 40. **default_distance_multiplicative**
- **Signature:** `(a, b, weight, factor)`
- **Line Range:** 515â€“535
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - normalize_plane call
  - Simple Euclidean distance with weight scaling

---

#### 41. **ensure_output_dir_for_file**
- **Signature:** `(filepath)`
- **Line Range:** 123â€“131
- **Complexity:** âšª TRIVIAL
- **Indicators:**
  - Path parent extraction
  - mkdir with exist_ok flag

---

#### 42. **_filter_requested_approaches**
- **Signature:** `(requested_approaches, cfg, paths_dict, only_round=False)`
- **Line Range:** 2811â€“2844
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:**
  - Simple loop with conditional filtering
  - Path key derivation
  - File existence check

---

### CLASS METHODS

#### UnionFind (lines 36â€“57)

**UnionFind.__init__**
- **Signature:** `(self, n)`
- **Line Range:** 37â€“39
- **Complexity:** âšª TRIVIAL
- **Indicators:** Simple list initialization

**UnionFind.find**
- **Signature:** `(self, x)`
- **Line Range:** 41â€“45
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:** Path compression recursion

**UnionFind.union**
- **Signature:** `(self, x, y)`
- **Line Range:** 47â€“57
- **Complexity:** ðŸŸ¡ LOW
- **Indicators:** Rank-based union with 2 if statements

---

## SUMMARY STATISTICS

- **Total Functions:** 42
- **Total Methods:** 3 (UnionFind class)
- **Extremely High Complexity:** 1 (weighted_voronoi)
- **Very High Complexity:** 4 (calculate_buffer, orchestrate_voronoi_weights, intersect_with_polygons_parallelized, dissolve_overlapping_geometries)
- **High Complexity:** 11
- **Moderate Complexity:** 11
- **Low Complexity:** 12
- **Trivial Complexity:** 6
