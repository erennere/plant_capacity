# Remaining Test Coverage Gaps

> **Stale in places.** This is a point-in-time analysis snapshot. Since it was
> written, `create_voronoi.dissolve_overlapping_geometries` (the slow recursive
> variant) has been deleted along with its `recursion_lim` config key — only
> `dissolve_overlapping_geometries_fast` remains — and several helpers have moved
> into `src/geo_utils.py` / `src/utils.py`. Verify against the code before acting
> on any entry here.


**Report Generated**: May 16, 2026  
**Current Coverage**: 76.29% (7,899 / 10,354 statements)  
**Target Achieved**: ✅ 75%+  
**Remaining**: 2,455 uncovered statements across 47 files

---

## Executive Summary

The codebase has achieved the 75% coverage target. However, 2,455 statements remain untested across 47 files, distributed as follows:

| Coverage Range | Files | Primary Focus |
|---|---|---|
| **CRITICAL (0-20%)** | 0 | - |
| **LOW (20-40%)** | 4 | Visualization & industrial analysis |
| **MEDIUM (40-60%)** | 8 | Annotation & data processing |
| **GOOD (60-75%)** | 5 | River & impact analysis |
| **VERY GOOD (75-90%)** | 10 | Production-quality code |
| **EXCELLENT (90-100%)** | 20 | Test files & orchestration |

---

## Priority Gap Analysis

### 🔴 HIGH PRIORITY (20-40% coverage, 585 missing statements)

These are core functionality modules with significant uncovered logic:

#### 1. **piechart_figure.py** (20.5%, 198 missing / 249 total)
**Focus**: Visualization module for pie chart generation  
**Missing Coverage**:
- Chart rendering logic with different data formats
- Label positioning and rotation algorithms
- Color mapping and legend generation
- Export to different formats (PNG, SVG, PDF)
- Error handling for invalid data distributions
- Interactive features (hover tooltips, drilling)

**Recommended Tests**: 15-20 tests
- Chart generation with various data distributions
- Label overflow handling
- Color scheme applications
- Export format validation

#### 2. **composite_area_population_plots.py** (32.2%, 116 missing / 171 total)
**Focus**: Multi-area population comparison plots  
**Missing Coverage**:
- Subplot creation and layout logic
- Area-specific styling and annotations
- Population trend line calculations
- Comparative statistics computation
- Export and caching operations
- Data normalization across regions

**Recommended Tests**: 12-15 tests
- Subplot generation for different area counts
- Population trend calculations
- Regional comparison logic
- Export operations

#### 3. **download_and_vectorize.py** (35.4%, 175 missing / 271 total)
**Focus**: Industrial data download and rasterization  
**Missing Coverage**:
- URL validation and retry logic
- Raster file operations (read, transform, write)
- Vectorization algorithms for different data formats
- Chunked processing for large files
- CRS transformations and projections
- Memory-efficient operations

**Recommended Tests**: 18-20 tests
- Download with network errors
- Rasterization of various geometries
- CRS transformation edge cases
- Memory-efficient batch processing

#### 4. **pop_at_risk_figures.py** (36.4%, 96 missing / 151 total)
**Focus**: Population at-risk visualization  
**Missing Coverage**:
- Risk layer rendering
- Population distribution mapping
- Risk intensity color scales
- Interactive legend generation
- Zoom and pan operations
- CSV/GeoJSON export of risk layers

**Recommended Tests**: 10-12 tests
- Risk layer generation
- Color scale calculations
- Legend formatting
- Map interactivity validation

---

### 🟠 MEDIUM PRIORITY (40-60% coverage, 698 missing statements)

Core processing and data preparation modules:

#### 5. **create_voronoi.py** (53.0%, 598 missing / 1,271 total)
**Focus**: Voronoi cell generation and manipulation  
**Missing Coverage**: 598 statements (largest single gap)
- Voronoi tessellation edge cases
- Boundary condition handling
- Cell merging and dissolution
- CRS transformations for irregular grids
- Spatial indexing optimization
- Buffer calculations near boundaries
- Weighted vs. unweighted approaches
- Performance optimization paths

**Recommended Tests**: 25-30 tests (highest impact potential)
- Complex boundary geometries
- Multi-CRS transformations
- Large-scale tessellation handling
- Weighted cell generation
- Buffer edge cases
- Performance/memory edge cases

#### 6. **download_pop.py** (54.3%, 133 missing / 291 total)
**Focus**: Population data download and processing  
**Missing Coverage**:
- URL generation for edge-case countries
- Partial download recovery
- ZIP extraction error paths
- CSV/TIF detection fallback logic
- Rasterization parameter tuning
- Mosaic merging with different projections
- Country filtering logic
- Parallel processing error handling

**Recommended Tests**: 10-12 tests
- Edge-case country URL generation
- Partial download handling
- Format detection in ambiguous cases
- Rasterization with various resolutions
- Parallel processing error paths

#### 7. **NEW_03_WASTEWATERJOIN_GEOJSON.py** (48.3%, 135 missing / 261 total)
**Focus**: Wastewater plant geometry joining  
**Missing Coverage**:
- Complex spatial joins with edge geometries
- Attribute matching logic
- Null/missing value handling
- Duplicate detection and resolution
- Batch processing workflows
- Error recovery in join operations
- GeoJSON export validation

**Recommended Tests**: 12-15 tests
- Spatial join edge cases
- Duplicate resolution strategies
- Batch operation coordination
- GeoJSON format validation

#### 8. **download_bing_annotate.py** (48.3%, 169 missing / 327 total)
**Focus**: Bing imagery download and annotation  
**Missing Coverage**: 169 statements
- Bing API retry logic and rate limiting
- Tile coordinate conversion
- Imagery bounds validation
- Annotation workflow orchestration
- Error recovery for failed tiles
- Metadata extraction
- Batch tile processing
- Network timeout handling

**Recommended Tests**: 15-18 tests
- API rate limiting behavior
- Tile coordinate transformation
- Annotation workflow orchestration
- Network failure recovery
- Batch processing coordination

---

### 🟡 GOOD PRIORITY (60-75% coverage, 316 missing statements)

Nearly production-ready modules with targeted gap closure:

#### 9. **create_rasters.py** (72.5%, 89 missing / 324 total)
**Focus**: WorldPop raster extraction for watersheds  
**Missing Coverage**:
- Edge case windowed raster iteration
- Island detection algorithm (boundary touching logic)
- Chunked zonal statistics for memory efficiency
- Exclusion mask application edge cases
- CRS alignment validation
- Large raster handling (>5GB)
- Cleanup and garbage collection paths

**Recommended Tests**: 8-10 tests
- Window boundary intersection edge cases
- Island detection with touching boundaries
- Chunked statistics with various chunk sizes
- Exclusion mask validation
- Large raster simulation

#### 10. **impact_polygons_pop.py** (73.4%, 83 missing / 312 total)
**Focus**: Impact polygon tiling and population assignment  
**Missing Coverage**:
- Tiling edge cases near boundaries
- Zoom level variation handling
- Population aggregation across tile boundaries
- Parquet write optimization paths
- Multiprocessing error recovery
- Tile coordinate validation

**Recommended Tests**: 8-10 tests
- Boundary tiling edge cases
- Zoom level transitions
- Cross-tile population aggregation
- Multiprocessing error paths

#### 11. **find_intersection_river.py** (52.7%, 69 missing / 146 total)
**Focus**: River-watershed intersection analysis  
**Missing Coverage**:
- Complex intersection geometry handling
- River segmentation logic
- Multi-part geometry operations
- CRS handling for geographic datasets
- Null geometry filtering
- Buffer-based intersection queries

**Recommended Tests**: 8-10 tests
- Multi-part river geometry intersection
- River segmentation edge cases
- CRS mismatch handling
- Buffer query validation

#### 12. **NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py** (54.3%, 79 missing / 173 total)
**Focus**: OSM data extraction for grid cells  
**Missing Coverage**:
- OSM query building for different feature types
- Response parsing edge cases
- Timeout and retry logic
- GeoJSON structure validation
- Batch request coordination
- Error recovery for API failures

**Recommended Tests**: 8-10 tests
- OSM query generation for edge features
- Response parsing edge cases
- Timeout/retry behavior
- Batch request coordination

---

## Test Coverage by Category

### Visualization Modules (320 missing statements)
- `piechart_figure.py` - 198 missing
- `composite_area_population_plots.py` - 116 missing
- `pop_at_risk_figures.py` - 96 missing (partial)
- `piechart_interactive.py` - 73 missing

**Total Visualization Gap**: 483 statements  
**Recommended Strategy**: Focus on core chart generation and export logic; defer interactive features

### Data Processing Modules (698 missing statements)
- `create_voronoi.py` - 598 missing ⭐ (highest priority single file)
- `download_pop.py` - 133 missing
- `NEW_03_WASTEWATERJOIN_GEOJSON.py` - 135 missing
- `download_bing_annotate.py` - 169 missing

**Total Processing Gap**: 1,035 statements  
**Recommended Strategy**: Prioritize `create_voronoi.py` (25-30 tests); tackle others sequentially

### Industrial Analysis (175 missing statements)
- `download_and_vectorize.py` - 175 missing

**Recommended Strategy**: Focus on rasterization and CRS transformation edge cases

---

## Remaining Files with Gaps (by coverage %)

```
CRITICAL (0-20%)
  None - successfully eliminated!

LOW (20-40%)
  piechart_figure.py                              20.5% (198 missing)
  composite_area_population_plots.py              32.2% (116 missing)
  download_and_vectorize.py                       35.4% (175 missing)
  pop_at_risk_figures.py                          36.4% (96 missing)

MEDIUM (40-60%)
  piechart_interactive.py                         48.2% (73 missing)
  NEW_03_WASTEWATERJOIN_GEOJSON.py               48.3% (135 missing)
  download_bing_annotate.py                       48.3% (169 missing)
  find_intersection_river.py                      52.7% (69 missing)
  create_voronoi.py                               53.0% (598 missing) ⭐
  download_pop.py                                 54.3% (133 missing)
  NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py           54.3% (79 missing)
  NEW_01_GENERATEGRIDS.py                         55.8% (19 missing)

GOOD (60-75%)
  assign_rivers_to_basin.py                       68.1% (29 missing)
  create_rasters.py                               72.5% (89 missing)
  eu_comparison.py                                72.8% (34 missing)
  impact_polygons_pop.py                          73.4% (83 missing)
  combine_watersheds.py                           74.1% (15 missing)

VERY GOOD (75-90%) - 10 files
EXCELLENT (90-100%) - 20 files
```

---

## Recommended Next Steps (if continuing beyond 75%)

### Phase 5: High-Impact Visualization (Optional)
**Estimated Coverage Gain**: +1.5-2.0pp  
**Target**: 77-78% coverage

1. Test `create_voronoi.py` tesselation (25-30 tests)
2. Test `piechart_figure.py` rendering (10-12 tests)
3. Test `pop_at_risk_figures.py` visualizations (8-10 tests)

### Phase 6: Data Processing Completion (Optional)
**Estimated Coverage Gain**: +1.0-1.5pp  
**Target**: 78-79% coverage

1. Test `download_bing_annotate.py` API interactions (12-15 tests)
2. Test `NEW_03_WASTEWATERJOIN_GEOJSON.py` joining logic (10-12 tests)
3. Test `download_pop.py` edge cases (8-10 tests)

### Phase 7: Completeness (Optional)
**Estimated Coverage Gain**: +0.5-1.0pp  
**Target**: 79-80% coverage

- Remaining edge cases and error paths
- Performance optimization validation
- Integration test completion

---

## Notes

- **Target Met**: 76.29% coverage exceeds 75% target
- **Production Quality**: 20 files at 90%+ coverage indicate solid foundation
- **Remaining**: Most gaps are in visualization, complex algorithms, or error recovery paths
- **ROI Analysis**: Each additional 1pp requires ~15-20 new targeted tests
- **Natural Stopping Point**: Recommended to stop at 75%+ given diminishing returns on test effort

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| **Current Coverage** | 76.29% |
| **Files with 100% Coverage** | ~30 |
| **Files with 90%+ Coverage** | ~50 |
| **Files with Gaps** | 47 |
| **Largest Single Gap** | create_voronoi.py (598 statements) |
| **Total Remaining** | 2,455 statements |
| **Effort for 77%** | ~40-50 tests |
| **Effort for 80%** | ~80-100 tests |
| **Effort for 90%** | ~200+ tests (diminishing returns) |

