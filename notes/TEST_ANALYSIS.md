# Test Implementation Analysis: Four Python Modules

## Executive Summary
This document provides a detailed breakdown of 4 key modules for test development. The analysis covers function signatures, data structures, logic branches, external dependencies, testability levels, and recommended test strategies.

---

## 1. research_code/figures_scripts/piechart_figure.py

### Module Purpose
Creates a static world map with choropleth background and country-level donut pie chart markers comparing residential vs industrial WWTP area indicators.

### Main Function Signatures

#### `aggregate_by_country(gdf, country_column, agg_column, industrial_column=None, is_pop=False)`
- **Parameters**: GeoDataFrame, str (country column), str (aggregation column), optional str (industrial column), bool
- **Returns**: DataFrame with aggregated statistics (`*_sum`, `*_mean`, `*_median`, `*_std`)
- **Key Logic**:
  - Conditional branching on `is_pop=True` vs `False`
  - If `is_pop=False` and no `industrial_column` provided → raises ValueError
  - Splits data by industrial/residential status and renames columns with prefix
  - Uses `groupby()` + `.agg()` dict pattern
- **Edge Cases**: 
  - Empty groups after dropna
  - NaN handling in numeric columns
  - Merges on missing industrial indicator

#### `plot_splitted_piechart(dist_tag1, dist_tag2, ax, size_tag1, size_tag2, min_size, labels=False, labels_text=['Paved', 'Unpaved', ''], cmap="tab20c")`
- **Parameters**: 2 distribution lists, matplotlib axis, 2 size floats, float (min_size), bool, list, str
- **Returns**: None (modifies matplotlib axis in place)
- **Key Logic**:
  - Calculates relative radii based on size comparison
  - Applies colormap with conditional indices
  - Filters pies below `min_size` threshold using radius check
  - Handles both regular and donut-style pie charts
  - Edge handling: radius <= 0, sum(values)/2 < min_size → skip rendering
- **Edge Cases**:
  - `size_tag1 == size_tag2` (equal sizes)
  - Non-finite values in distributions
  - Grid and axis operations on non-existent geometries

#### `calculate_size(value, min_value, max_value, min_size, max_size, scale='log')`
- **Parameters**: float (value), float (min), float (max), 2 floats (sizes), str (scale type)
- **Returns**: float (mapped size)
- **Key Logic**:
  - Validates finiteness of inputs with `np.isfinite()`
  - Returns `min_size` for any non-finite input
  - Handles `max_value <= min_value` case (returns midpoint)
  - Two scale paths: `'log'` (checks value/min/max > 0) vs `'linear'` (direct mapping)
- **Edge Cases**:
  - Non-positive values with log scale → returns `min_size`
  - Zero division protected by `max_value <= min_value` check
  - Invalid scale type → raises ValueError

#### `get_pos(geometry)`
- **Parameters**: shapely geometry (Polygon or MultiPolygon)
- **Returns**: tuple (x, y) representative position
- **Key Logic**:
  - Polygon → centroid coordinates
  - MultiPolygon → centroid of largest sub-polygon by area
  - Unknown type → raises ValueError
- **Edge Cases**:
  - Empty geometries
  - MultiPolygon with single part

#### `round_numbers(arr, breaks)`
- **Parameters**: numpy array, list of break counts
- **Returns**: list of rounded numbers spanning value range
- **Key Logic**:
  - Filters array to finite, positive values only
  - Generates log-scale-based rounding with power calculation
  - Handles empty arrays by returning input breaks
- **Edge Cases**:
  - All values ≤ 0 or NaN → returns input breaks unchanged
  - Single value in array

#### `ensure_population_percentage_column(df, preferred_col="population_served_index", zonal_sum_col="2024_zonal_sum")`
- **Parameters**: DataFrame, 2 str (column names)
- **Returns**: str (column name used)
- **Key Logic**:
  - Priority chain: 
    1. Check preferred column exists
    2. Calculate from `population_served` / `population_total`
    3. Calculate from `{zonal_sum_col}_sum` / `population_total`
  - Handles zero denominators with `.replace(0, np.nan)`
- **Edge Cases**:
  - Missing denominators → fill with 0
  - None of the three scenarios available → raises KeyError

#### `resolve_zonal_sum_columns(df, preferred)`
- **Parameters**: DataFrame, str (preferred column)
- **Returns**: str (resolved column name)
- **Key Logic**:
  - Returns preferred if exists
  - Fallback: scans for `*_zonal_sum` columns, extracts year from prefix
  - Sorts by year and returns latest (highest year)
- **Edge Cases**:
  - No `*_zonal_sum` columns → raises KeyError
  - Year extraction fails (non-numeric) → defaults to year=-1
  - Empty candidates list

#### `main()`
- **Parameters**: None (reads config from disk)
- **Returns**: None (saves figure to disk)
- **Key Logic**:
  - Heavy orchestration: loads config, data files, applies multiple merges
  - Merges boundaries with aggregated stats from multiple approaches
  - Builds Cartopy projection (Robinson) map with choropleth + inset pie charts
  - Color normalization with log or linear scale
  - Iterates through all countries to place pie markers at representative positions
  - Creates size legends using Circle patches
- **Edge Cases**:
  - Missing stats file → raises FileNotFoundError
  - Empty data after filtering → skipped in marker loop
  - Non-finite sizes → skips rendering
  - Negative or zero total size → continues iteration

### Key Data Structures
- **boundaries** (GeoDataFrame): country polygons with ISO codes, merged with stats
- **pop_gdf** (GeoDataFrame): WWTP Voronoi regions with population data, indexed by country
- **stats_df** (DataFrame): CSV raster statistics by country
- **agg_datasets** (list of DataFrames): results from `aggregate_by_country()` for each column pair
- **size_df** (DataFrame): numeric conversion of size columns for total calculation

### External Dependencies
- **geopandas**: GeoDataFrame operations, file I/O (.gpkg)
- **cartopy**: Robinson projection, coastlines, gridlines
- **matplotlib**: figure/axes, pie chart rendering, inset axes, colormaps
- **numpy**: isfinite, log operations, percentile calculations
- **shapely**: geometry type checking
- **config system** (`starter.load_config`, `pipelines.create_pop_output_paths`): configuration orchestration

### Testability Assessment
- **Unit Testable** (HIGH): `aggregate_by_country`, `calculate_size`, `get_pos`, `round_numbers`, `ensure_population_percentage_column`, `resolve_zonal_sum_columns`, `plot_splitted_piechart`
- **Integration Tested** (MEDIUM): `main()` (requires mock files + config)
- **Hard to Unit Test** (LOW): Cartopy/matplotlib rendering pipeline (requires mocking axes/projections)

### Recommended Test Count: ~18-22 tests
- Core logic functions (7-8 unit tests per function): ~15-18 tests
- Integration tests for `main()`: 2-4 tests
- Mock strategy: GeoDataFrames for geospatial ops, matplotlib mock for axis operations

---

## 2. research_code/figures_scripts/composite_area_population_plots.py

### Module Purpose
Generates diagnostic histograms and scatter plots comparing facility-level vs country-level area/population ratios.

### Main Function Signatures

#### `resolve_zonal_sum_column(df, preferred)`
- **Parameters**: DataFrame, str (preferred column name)
- **Returns**: str (resolved column name)
- **Key Logic**: Identical to `piechart_figure.py` version (code duplication candidate)
- **Edge Cases**: No `*_zonal_sum` columns → raises KeyError

#### `clip_outliers(series, lower_q, upper_q)`
- **Parameters**: pd.Series, 2 floats (quantile bounds 0-1)
- **Returns**: pd.Series (filtered to quantile range)
- **Key Logic**:
  - Converts to numeric, replaces inf with NaN, drops NaN
  - Calculates quantile thresholds
  - Returns only values within range
- **Edge Cases**:
  - Empty series → returns empty
  - All NaN/inf → returns empty Series
  - `lower_q == upper_q` → single value result

#### `_bleach_color(color, amount=0.35)`
- **Parameters**: tuple (RGB) or matplotlib color, float (blend amount 0-1)
- **Returns**: tuple (RGB blended toward white)
- **Key Logic**: Linear blend: `(1-amount) * original + amount * white`
- **Edge Cases**:
  - amount=0 → original color
  - amount=1 → pure white
  - Non-RGB colors → array conversion may fail

#### `make_category_color_map(values)`
- **Parameters**: array-like (category values)
- **Returns**: dict {category_str → RGB tuple}
- **Key Logic**:
  - Converts to unique string values (handles NaN as "Unknown")
  - Cycles through tab20 colormap
  - Applies bleach transform to all colors
- **Edge Cases**:
  - All NaN input → {"Unknown": bleached_color}
  - Single category
  - >20 unique categories (cycles colormap)

#### `add_one_to_one_line(ax, x_vals, y_vals)`
- **Parameters**: matplotlib axis, 2 pd.Series (x and y values)
- **Returns**: None (modifies axis in place)
- **Key Logic**:
  - Combines both series, filters to valid finite values
  - Uses min/max to determine plot range
  - Plots y=x dashed line if range is valid
- **Edge Cases**:
  - Empty combined series → returns early
  - `hi == lo` → adds small epsilon to avoid degenerate line
  - Non-finite values in either series

#### `build_country_table(pop_df, boundaries, zonal_col, color_col)`
- **Parameters**: GeoDataFrame, GeoDataFrame, str, str
- **Returns**: DataFrame with computed ratios and aggregate/median statistics
- **Key Logic**:
  - Computes two ratios: `total_area / zonal_sum` and `round_area / total_area`
  - Groups by ISO_2 and aggregates (sum, median)
  - Merges with boundary colors by ISO code
  - Fills missing colors with "Unknown"
- **Edge Cases**:
  - Division by zero (zonal_sum or total_area = 0) → NaN
  - Missing ISO_A2/ISO_A2_EH column → uses fallback logic
  - Missing color column → KeyError

#### `make_histogram_plot(pop_df, zonal_col, out_path, lower_q, upper_q)`
- **Parameters**: GeoDataFrame, str, str (filepath), 2 floats
- **Returns**: None (saves PNG file)
- **Key Logic**:
  - Computes two facility-level ratios
  - Clips outliers using quantile range
  - Creates side-by-side histograms (60 bins each)
  - Adds grid, titles, labels
- **Edge Cases**:
  - Missing output directory → created by `ensure_output_dir_for_file()`
  - All values clipped (empty result) → empty histogram

#### `make_scatter_plot(country_df, color_col, out_path)`
- **Parameters**: DataFrame, str, str (filepath)
- **Returns**: None (saves PNG file)
- **Key Logic**:
  - Creates 2 side-by-side scatter plots (aggregate vs median ratios)
  - Maps colors by category from boundary column
  - Adds 1:1 reference lines using `add_one_to_one_line()`
  - Annotates each point with country ISO code
  - Creates legend if ≤20 unique categories
- **Edge Cases**:
  - Non-finite x/y values → skipped in annotation loop
  - >20 categories → no legend
  - Empty country_df → empty plots

#### `parse_args()`
- **Parameters**: None (reads sys.argv)
- **Returns**: argparse.Namespace with parsed arguments
- **Key Logic**:
  - Positional args: level, version, buffer, weight_method, weight_func, dynamic_buffering, dynamic_buffer_k
  - Optional args: --approach, --color-col, --zonal-col, --hist-lower-q, --hist-upper-q
- **Edge Cases**:
  - Missing positional args → defaults to None
  - Invalid type for floats → argparse error

#### `main()`
- **Parameters**: None (CLI entry point)
- **Returns**: None (saves two plots)
- **Key Logic**:
  - Parses args and config overrides
  - Loads Voronoi population file and boundaries
  - Validates required columns (ISO_2, total_area, round_area)
  - Resolves zonal-sum column with fallback
  - Calls `make_histogram_plot()` then `make_scatter_plot()`
- **Edge Cases**:
  - Missing required columns → raises KeyError
  - Invalid color column in boundaries → raises KeyError
  - Missing output filepath config → handled by `ensure_output_dir_for_file()`

### Key Data Structures
- **pop_df** (GeoDataFrame): Voronoi population file with facility-level ratios
- **boundaries** (GeoDataFrame): country polygons with color column (ECONOMY, etc.)
- **country_df** (DataFrame): aggregated country-level statistics and medians
- **color_map** (dict): category → RGB tuple mapping

### External Dependencies
- **geopandas**: GeoDataFrame I/O, geometry operations
- **matplotlib**: figure creation, scatter/hist plots, legend, grid
- **pandas**: ratio calculation, groupby/agg, numeric conversion
- **numpy**: operations on arrays, percentile calculations
- **argparse**: CLI argument parsing
- **config system**: `starter.load_config`, `pipelines.create_pop_output_paths`

### Testability Assessment
- **Unit Testable** (HIGH): `clip_outliers`, `_bleach_color`, `make_category_color_map`, `add_one_to_one_line`, `build_country_table`, `resolve_zonal_sum_column`
- **Integration Tested** (MEDIUM): `make_histogram_plot`, `make_scatter_plot` (mock plt.savefig)
- **Hard to Unit Test** (LOW): `main()`, `parse_args()` (need to mock file I/O and plt)

### Recommended Test Count: ~20-25 tests
- Helper functions (5-6 unit tests per function): ~12-15 tests
- Plot functions (2-3 integration tests each): ~4-6 tests
- Argument parsing: 2-3 tests
- Main orchestration: 1-2 tests

---

## 3. research_code/figures_scripts/pop_at_risk_figures.py

### Module Purpose
Creates geospatial choropleth maps showing population at risk from pollution/untreated wastewater, with multiple radius/year combinations and unserved population visualization.

### Main Function Signatures

#### `_robust_bounds(values, positive_only=False, quantile_range=(0.02, 0.98), iqr_factor=1.5)`
- **Parameters**: array-like, bool, tuple (2 floats), float
- **Returns**: tuple (vmin, vmax) for robust normalization
- **Key Logic**:
  - Converts to Series, replaces inf with NaN, drops NaN
  - Applies IQR method: Q1 - 1.5×IQR to Q3 + 1.5×IQR
  - Overlays quantile-based bounds
  - Uses max of IQR_low and q_low, min of IQR_high and q_high
  - Handles `iqr == 0` by falling back to min/max
  - Optional positive-only filtering (clips low to machine epsilon)
- **Edge Cases**:
  - Empty values → raises ValueError
  - All values identical → `iqr==0`, uses min/max
  - Non-finite bounds after calculation → recalculates from scratch
  - `high <= low` → multiplies high by 10 (positive) or 1.000001 (linear)

#### `create_single_plot(z8_stats, column, title, output_filename, ...)`
- **Parameters**: GeoDataFrame, str (column name), str, str (filename), + many optional kwargs
- **Returns**: tuple (fig, ax) matplotlib objects
- **Key Logic**:
  - Validates column exists in GeoDataFrame
  - Sets CRS to WGS84 if missing
  - Transforms values using optional value_transform function
  - Removes invalid geometries, reprojects to target projection
  - Optional masking by min_count_col threshold
  - Builds normalization: LogNorm (requires positive values) or Normalize (linear)
  - Clamps out-of-range values to NaN
  - Plots choropleth with ListedColormap and missing_kwds
  - Adds gridlines and labels (if projection has gridlines method)
  - Saves to PNG with configurable DPI
- **Edge Cases**:
  - Column not found → raises KeyError
  - No valid positive values for log scale → raises ValueError
  - vmin == vmax for linear scale → raises ValueError
  - Invalid geometries removed silently
  - Missing projection → defaults to Robinson
  - Author note placement uses negative y offset

#### `create_impact_polygon_plots(pop_at_risk_gdf, tiles_gdf, output_filepath)`
- **Parameters**: 2 GeoDataFrames, str (directory)
- **Returns**: None (creates multiple PNG files)
- **Key Logic**:
  - Merges pop_at_risk data onto tiles by 'tile' column
  - Identifies all `*_zonal_sum` columns (one per radius/year combo)
  - Cycles through colormaps (8-colormap cycle)
  - Parses column name to extract radius and year (expects format: `{radius}_{year}_zonal_sum`)
  - Calls `create_single_plot()` for each column with unique title/output file
- **Edge Cases**:
  - No `*_zonal_sum` columns → logs warning and returns early
  - Column format unparseable → logs warning and skips column
  - Non-numeric radius/year → caught by try/except, logs warning

#### `main()`
- **Parameters**: None (CLI entry point)
- **Returns**: None (creates multiple figure files)
- **Key Logic**:
  - Loads config from overrides
  - Loads country boundaries and calls `find_tiles_in_countries()` to generate tile grid
  - Loads pre-computed pop_at_risk parquet file
  - Calls `create_impact_polygon_plots()` to generate all radius/year maps
  - Loads unserved population CSV, groups by tile, sums pop
  - Calls `create_single_plot()` once more for unserved population map
- **Edge Cases**:
  - Hardcoded filepaths (not from config) override config settings
  - pop_threshold filtering (pop_sum > 100) may remove all tiles
  - Missing column handling in DuckDB query

### Key Data Structures
- **z8_stats** (GeoDataFrame): Z8 tile grid with zonal-sum columns per radius/year
- **pop_at_risk_gdf** (GeoDataFrame): impact data merged onto tiles
- **unserved_df** (DataFrame): tile ID → population count mapping from CSV
- **tiles_gdf** (GeoDataFrame): H3/S2 tile boundaries for a given zoom level

### External Dependencies
- **geopandas**: GeoDataFrame I/O (.parquet), geometry validation
- **matplotlib**: figure creation, choropleth plots, normalization (LogNorm/Normalize)
- **cartopy**: projections, gridlines, formatters
- **pandas**: DataFrame operations, groupby, numeric conversion
- **duckdb**: SQL-based CSV query and filtering
- **logging**: module-level logging setup
- **config system**: `starter.load_config`, `parse_config_overrides`
- **research_code.pop_at_risk_river_calculations.find_pop_in_danger_pop**: tile generation

### Testability Assessment
- **Unit Testable** (HIGH): `_robust_bounds` (pure math)
- **Integration Tested** (MEDIUM): `create_single_plot` (mock plt.savefig, mock CRS)
- **Hard to Unit Test** (LOW): `create_impact_polygon_plots`, `main()` (dependency on DuckDB, file I/O, external config)

### Recommended Test Count: ~15-20 tests
- `_robust_bounds` unit tests: 8-10 tests (edge cases with IQR, quantiles, inf, empty)
- `create_single_plot` integration tests: 4-6 tests (mock plt, GeoDataFrame validation)
- `create_impact_polygon_plots`: 1-2 tests (mock create_single_plot)
- `main()`: 1-2 tests (full integration with mocked file I/O)

---

## 4. research_code/create_voronoi.py

### Module Purpose
Comprehensive geospatial utilities for WWTP capacity analysis: geometry validation, coordinate transformations, Voronoi diagrams, spatial clustering, DuckDB integration, and buffer dissolution with multi-process orchestration.

### Main Class

#### `UnionFind` (class)
- **Methods**:
  - `__init__(n)`: Initialize with n elements
  - `find(x)`: Return root with path compression
  - `union(x, y)`: Union two sets by rank
- **Used for**: Efficient spatial clustering (O(n log n) vs O(n²))

### Core Function Signatures

#### **SECTION 1: Geometry Validation & Manipulation**

##### `geometry_contains_points(geometry, points)`
- **Parameters**: shapely geometry, np.ndarray shape (n, 2)
- **Returns**: np.ndarray bool (length n)
- **Key Logic**:
  - Uses vectorized shapely operations if available (`shapely.contains_xy`)
  - Fallback to vectorized.contains, then fallback to loop
  - Handles empty/None inputs
- **Edge Cases**: 
  - None or empty points → returns empty bool array
  - Vectorized ops not available → graceful degradation

##### `is_valid_geom(geom)`
- **Parameters**: shapely geometry or None
- **Returns**: bool
- **Key Logic**:
  - Checks: not None, is_valid, all coordinates finite
  - Catches all exceptions, returns False
- **Edge Cases**:
  - Any exception during check → False
  - Non-Point geometries without coords attribute → handled

##### `drop_duplicates(df, col)`
- **Parameters**: DataFrame, str (column name)
- **Returns**: DataFrame with non-NaN duplicates removed
- **Key Logic**:
  - Separates NaN rows (always kept) from non-NaN rows
  - Deduplicates non-NaN by column, keeps first
  - Concatenates back
- **Edge Cases**:
  - All NaN column → returns original df with NaN rows
  - None input → returns None

##### `buffer_geometry(geom)`
- **Parameters**: shapely geometry
- **Returns**: geometry (buffered if polygon/multipolygon)
- **Key Logic**:
  - Point/LineString/MultiLineString → return unchanged
  - Polygon/MultiPolygon → apply buffer(0) to fix topology
  - Unknown types → log and return as-is
- **Edge Cases**:
  - Buffer fails → logs error, returns original geom
  - None input → returns None

##### `create_centroid_points(geom)`
- **Parameters**: shapely geometry or NaN
- **Returns**: shapely.Point or None
- **Key Logic**:
  - Point → return as-is
  - Polygon/LineString/MultiPolygon → return centroid if valid
  - Invalid centroid → returns None
- **Edge Cases**:
  - Empty geometry → returns None
  - NaN/None input → returns None

##### `normalize_plane(a, b)`
- **Parameters**: np.ndarray (n, 2), tuple/array (2,)
- **Returns**: tuple (a_normalized, b_normalized) to [0,1] range
- **Key Logic**:
  - Combines both arrays to find global min/max
  - Divides by range (clamps denominator to 1 if range is 0)
- **Edge Cases**:
  - Identical x or y across all points → denominator becomes 1
  - Single point → denominator becomes 1

#### **SECTION 2: Coordinate Transformation & Projection**

##### `estimate_utm_epsg(lon, lat)`
- **Parameters**: 2 floats (degrees)
- **Returns**: int (EPSG code)
- **Key Logic**:
  - Validates lon (-180 to 180), lat (-90 to 90)
  - Calculates UTM zone: `int((lon + 180) // 6) + 1`
  - Selects northern (32600+zone) or southern (32700+zone) EPSG
  - Validates EPSG, fallback to Web Mercator (3857)
- **Edge Cases**:
  - Out-of-bounds coordinates → raises ValueError
  - Invalid EPSG → falls back to 3857

##### `estimate_utm_crs(gdf)`
- **Parameters**: GeoDataFrame
- **Returns**: pyproj.CRS (UTM or fallback to EPSG:3857)
- **Key Logic**:
  - Filters valid, non-empty geometries
  - Extracts centroid from union of all geometries
  - Calls `estimate_utm_epsg()` with centroid
  - Searches for Point geometry if centroid has non-finite coords
  - Returns CRS object, fallback to 3857 if any error
- **Edge Cases**:
  - No valid geometries → returns 3857
  - Non-finite centroid → searches for Point, then falls back

#### **SECTION 3: Spatial Clustering**

##### `cluster_point_indices(geoms, threshold)`
- **Parameters**: iterable of Points, float (distance threshold)
- **Returns**: list of sets (point index clusters)
- **Key Logic**:
  - Builds cKDTree from point coordinates
  - Uses `query_ball_point()` to find all neighbors within threshold
  - UnionFind to group connected components
  - Logs cluster statistics (sizes, count)
- **Edge Cases**:
  - Zero threshold → each point in separate cluster (or all if overlapping)
  - Single point → one cluster with one point
  - All points identical coordinates → potential single large cluster

##### `cluster_points(df, threshold)`
- **Parameters**: GeoDataFrame with 'weights' column, float
- **Returns**: GeoDataFrame with clustered points merged
- **Key Logic**:
  - Calls `cluster_point_indices()`
  - For each cluster: keeps row with fewest NaNs, sums weights, sums POP_SERVED
  - Concatenates results
- **Edge Cases**:
  - Single-point cluster → kept as-is
  - Multiple points with same NaN count → uses first (arbitrary)
  - Missing weights/POP_SERVED columns → handled

#### **SECTION 4: Grid & Distance Utilities**

##### `create_ranges(x, y, step, min_step=100)`
- **Parameters**: 2 floats, float (step), float (min_step)
- **Returns**: np.ndarray (coordinate range)
- **Key Logic**:
  - Calculates range = max(x,y) - min(x,y)
  - While loop: if range >= step, return linspace; else halve step
  - Exits loop if step < min_step (returns [min, max])
- **Edge Cases**:
  - x == y (range=0) → returns [x, y]
  - Infinite loop risk: no max iteration limit (inefficiency noted)
  - Very small min_step can cause many iterations

##### `nearest_neighbor_distances_and_median(df)`
- **Parameters**: GeoDataFrame with 'geometry' column
- **Returns**: tuple (np.ndarray of distances, float median)
- **Key Logic**:
  - Extracts coordinates from geometries (uses centroids for non-points)
  - Builds cKDTree
  - Queries k=3 neighbors (or k=2 if <3 points)
  - Averages distances to 2nd and 3rd nearest neighbors
  - Returns median of these averaged distances
- **Edge Cases**:
  - <2 valid geometries → returns (empty array, NaN)
  - All geometries empty → returns (empty array, NaN)

##### `auto_weight_scale(points)`
- **Parameters**: list/array of (x, y) tuples
- **Returns**: float (median of min distances)
- **Key Logic**:
  - Removes None/non-finite coordinates
  - Computes pairwise distances, fills diagonal with NaN
  - Returns mean of min distances per point
- **Edge Cases**:
  - All None/non-finite → empty array
  - Single point → empty min distances

##### `default_distance_additive(a, b, weight, factor)`
- **Parameters**: np.ndarray (n, 2), tuple (2,), float, float
- **Returns**: np.ndarray (weighted distances)
- **Key Logic**:
  - Normalizes to [0,1] plane
  - Computes: sqrt(sum((a-b)²) - weight²)
  - Clamps negative results to 0.01 (numerical stability)
- **Edge Cases**:
  - weight > Euclidean distance → sqrt(negative) → clamped to 0.01
  - All points identical → distance = weight (or 0.01 if weight > 0)

##### `default_distance_multiplicative(a, b, weight, factor)`
- **Parameters**: np.ndarray (n, 2), tuple (2,), float, float
- **Returns**: np.ndarray (weighted distances)
- **Key Logic**:
  - Normalizes to [0,1] plane
  - Returns Euclidean distance / weight
- **Edge Cases**:
  - weight=0 → division by zero (NOT handled - will raise)
  - weight<0 → negative distances possible (numeric issue)

#### **SECTION 5: Data Processing & Normalization**

##### `calculate_area(df, only_round=False)`
- **Parameters**: GeoDataFrame, bool
- **Returns**: GeoDataFrame with area-derived columns added
- **Key Logic**:
  - Parses 'wwtp_area_rect' column (comma/space-separated floats)
  - Parses 'diameters' column → computes round area from circles
  - Combined or round-only total_area based on `only_round`
  - Computes capacity_proxy = total_area × sqrt(num_detections)
  - Fills zeros in capacity_proxy with column mean
- **Edge Cases**:
  - Empty df → returns as-is
  - Missing 'wwtp_area_rect' column → total_area = 1
  - Parsing errors in area strings → NaN converted to 0
  - Zero detections → sqrt(0) = 0, multiplied by area

##### `normalize_column_to_rounded_str(series)`
- **Parameters**: pd.Series (numeric)
- **Returns**: pd.Series of strings (rounded integers)
- **Key Logic**:
  - Converts to numeric (coerce errors to NaN)
  - Rounds and converts to Int64 (preserves NaN)
  - Converts to string
- **Edge Cases**:
  - All NaN → returns all 'NaN' strings
  - Float IDs rounded → precision loss

#### **SECTION 6: DuckDB & External Data Integration**

##### `download_overture_maps(url, filepath)`
- **Parameters**: str (S3 URL), str (local path)
- **Returns**: None (saves parquet to disk)
- **Key Logic**:
  - Creates parent directories
  - DuckDB SPATIAL extension: downloads S3 parquet, filters by subtype='country'
  - Saves as ZSTD-compressed parquet
- **Edge Cases**:
  - Network error → logs warning, continues
  - S3 access denied → DuckDB error caught and logged
  - Existing file → overwrites

##### `process_centroid(args)` (worker function)
- **Parameters**: tuple (Point, rtree.Index, GeoDataFrame, column_name)
- **Returns**: value or None
- **Key Logic**:
  - Uses spatial index bounding box query for candidates
  - Precise intersect check on valid candidates
  - Returns column value from first match
- **Edge Cases**:
  - Invalid/empty centroid → returns None
  - No spatial index matches → returns None
  - Invalid polygons filtered out before intersect

##### `intersect_with_polygon_sindex(df, polygons, col, concurrency=False)`
- **Parameters**: GeoDataFrame, GeoDataFrame, str, bool
- **Returns**: GeoDataFrame with new column `col` added
- **Key Logic**:
  - Separates invalid/missing geometries
  - Buffers geometries (topology fix)
  - Creates centroids
  - Builds spatial index on polygons
  - Maps process_centroid across all centroids (optional threading)
  - Reconstructs GeoDataFrame with results
- **Edge Cases**:
  - No valid geometries → returns NaN rows only
  - ThreadPoolExecutor: parallel overhead for small datasets

##### `intersect_with_polygons_db(df, polygons, cols, df_join_col='ISO_2', polygon_join_col='ISO_2')`
- **Parameters**: GeoDataFrame, GeoDataFrame, list/str, 2 str
- **Returns**: GeoDataFrame with polygon columns transferred
- **Key Logic**:
  - Converts geometries to WKT
  - Uses DuckDB spatial SQL with ST_Intersects
  - Joins on df_join_col == polygon_join_col + spatial intersection
  - Converts WKT back to geometries
- **Edge Cases**:
  - Missing join columns → raises KeyError
  - WKT conversion fails → geometry becomes None
  - DuckDB temp database file cleanup if exception

##### `intersect_with_polygons_parallelized(df, polygons, cols, use_duckdb=False, max_workers=16, ...)`
- **Parameters**: GeoDataFrame, GeoDataFrame, list/str, bool, int, 2 str
- **Returns**: GeoDataFrame with polygon columns added
- **Key Logic**:
  - Partitions both df and polygons by UTM zone
  - Processes each zone independently in parallel (or sequentially if use_duckdb=False)
  - Uses either spatial index or DuckDB based on flag
  - Concatenates results from all zones
- **Edge Cases**:
  - Some zones have no data → skipped
  - UTM estimation fails → may use fallback projection
  - No parallel workers assigned → sequential processing

##### `intersects_with_country_db(df, filepath, polygon_country_col='country', output_country_col='ISO_2')`
- **Parameters**: GeoDataFrame, str (parquet path), 2 str
- **Returns**: GeoDataFrame with country column added
- **Key Logic**:
  - DuckDB spatial join: bounding box filter + precise ST_Intersects
  - Uses parquet file (assumed to be country boundaries)
  - Converts WKT back to geometries
- **Edge Cases**:
  - Parquet file missing → DuckDB error
  - Non-WGS84 input → converted to EPSG:4326

#### **SECTION 7: Buffer & Geometry Dissolution**

##### `dissolve_overlapping_geometries(subdf, radius, convex=False, recursion_lim=50000)`
- **Parameters**: GeoDataFrame with 'some_id', float, bool, int
- **Returns**: tuple (list of overlap groups, GeoDataFrame) or None
- **Key Logic**:
  - Slow nested-loop variant (slower than fast version)
  - Groups by latitude and longitude bounds
  - Builds intersection graph
  - Uses DFS to find connected components
  - Returns merged groups of overlapping IDs
- **Edge Cases**:
  - Empty df → returns None
  - UTM estimation fails → returns None
  - Recursion limit set to 50000 (risky for large datasets)

##### `dissolve_overlapping_geometries_fast(subdf, radius, convex=False)`
- **Parameters**: GeoDataFrame with 'some_id', float, bool
- **Returns**: tuple (list of overlap groups, GeoDataFrame)
- **Key Logic**:
  - Projects to UTM (assumes method exists on GeoDataFrame: `.estimate_utm_crs()`)
  - Buffers centroids or uses bounding boxes
  - Uses spatial index + NetworkX for fast component detection
  - Returns groups mapping
- **Edge Cases**:
  - Missing estimate_utm_crs method → AttributeError
  - Empty df → returns ([], None)

##### `orchestrate_overlaps(df, max_workers, buffers_filepath, radius, convex=False, country_col='ISO_2')`
- **Parameters**: GeoDataFrame, int, str (filepath), float, bool, str
- **Returns**: GeoDataFrame (dissolved buffers)
- **Key Logic**:
  - Checks cache first; if exists, returns cached file
  - Partitions by country
  - Submits each country to ProcessPoolExecutor
  - Waits for all futures, collects results
  - Maps overlap groups to group IDs
  - Dissolves geometries by group
  - Saves to cache file
- **Edge Cases**:
  - Cache file already exists → returns cached version
  - Some countries fail in parallel → continues with others (logs error)
  - ProcessPoolExecutor issues → futures may fail

#### **SECTION 8: Voronoi Computation & Orchestration**

##### `resolve_polygon_overlaps(region_polygons)`
- **Parameters**: GeoDataFrame
- **Returns**: np.ndarray of geometries (with overlaps resolved)
- **Key Logic**:
  - For each pair of overlapping polygons, removes intersection from smaller polygon
  - Operates on copy to avoid modifying input
  - (Implementation incomplete in read)

### Key Data Structures
- **UnionFind**: for clustering (internal data structure)
- **cKDTree**: spatial indexing for neighbor queries
- **rtree.Index**: spatial indexing for bounding box queries
- **NetworkX Graph**: for connected component detection in dissolve operations
- **GeoDataFrame**: primary container for spatial operations

### External Dependencies
- **geopandas/shapely**: core geometry operations, I/O, validation
- **duckdb**: spatial SQL queries, parquet I/O
- **scipy** (cKDTree, pdist, squareform): spatial distance calculations
- **pyproj** (CRS, Transformer): coordinate system operations
- **rasterio**: raster-to-vector conversion (shapes function)
- **skimage, cv2**: image processing utilities (find_contours, cv2 operations)
- **networkx**: graph operations for overlap resolution
- **joblib**: parallel execution utilities
- **multiprocessing** (Pool, ProcessPoolExecutor): parallel processing

### Testability Assessment
- **Unit Testable** (HIGH): Geometry validation functions, distance functions, normalization, clustering indices
  - `is_valid_geom`, `buffer_geometry`, `normalize_plane`, `calculate_size` variants, `cluster_point_indices`, `nearest_neighbor_distances_and_median`
- **Integration Tested** (MEDIUM): Data processing, DuckDB queries, UTM estimation
  - `calculate_area`, `estimate_utm_crs`, `estimate_utm_epsg`, `intersect_with_polygon_sindex`
- **Hard to Unit Test** (LOW): Orchestration functions, parallel processing, complex graph operations
  - `orchestrate_overlaps`, `dissolve_overlapping_geometries`, `intersect_with_polygons_parallelized`

### Recommended Test Count: ~30-40 tests
- Pure math/utility functions: 15-18 tests
- Geometry validation: 8-10 tests
- Clustering functions: 6-8 tests
- DuckDB/file I/O integration: 3-4 tests (mocked)
- Orchestration: 2-3 tests (mocked parallel execution)

---

## Summary: Test Strategy by Module

| Module | Testable Functions | Complexity | Mocking Requirements | Estimated Tests |
|--------|-------------------|-----------|---------------------|-----------------|
| piechart_figure.py | 7 core + main | Medium | GeoDataFrame, matplotlib | 18-22 |
| composite_area_population_plots.py | 6 core + plot funcs + main | Medium | GeoDataFrame, matplotlib, argparse | 20-25 |
| pop_at_risk_figures.py | 1 math + plot funcs + main | Medium-High | GeoDataFrame, matplotlib, cartopy, config | 15-20 |
| create_voronoi.py | 15+ core utilities | High | Geospatial libs, DuckDB, parallel executors | 30-40 |
| **TOTAL** | **~40 testable functions** | **Medium-High** | **Config, File I/O, Spatial Libs** | **~85-110 tests** |

### Priority for Implementation
1. **Highest Priority**: Core utility functions with deterministic outputs (geometry validation, normalization, distance calculations)
2. **Medium Priority**: Data transformation functions (aggregation, ratio calculation, area parsing)
3. **Lower Priority**: Full-pipeline integration (main functions requiring mocked file I/O and config)

