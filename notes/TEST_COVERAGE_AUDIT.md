# Test Coverage Audit

Generated automatically from AST inventory + coverage.json

| File | Functions | Classes | Methods | Branch Nodes | Coverage | Missing Lines |
|---|---:|---:|---:|---:|---:|---:|
| src/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/add_pop.py | 6 | 0 | 0 | 45 | 79 | 34 |
| src/annotation_scripts/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/annotation_scripts/annotations_inspection.py | 5 | 0 | 0 | 14 | 96 | 4 |
| src/annotation_scripts/copy_falsy_images.py | 1 | 0 | 0 | 6 | 88 | 4 |
| src/annotation_scripts/download_bing_annotate.py | 15 | 0 | 0 | 59 | 0 | 327 |
| src/annotation_scripts/merge_annotations.py | 4 | 0 | 0 | 10 | 98 | 1 |
| src/annotation_scripts/NEW_01_GENERATEGRIDS.py | 2 | 0 | 0 | 7 | 0 | 43 |
| src/annotation_scripts/NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py | 10 | 0 | 0 | 50 | 0 | 173 |
| src/annotation_scripts/NEW_03_WASTEWATERJOIN_GEOJSON.py | 16 | 0 | 0 | 52 | 0 | 261 |
| src/annotation_scripts/NEW_04_EXPORTGEOTIFF.py | 0 | 0 | 0 | 3 | 0 | 44 |
| src/combine_watersheds.py | 2 | 0 | 0 | 10 | 74 | 15 |
| src/create_voronoi.py | 50 | 1 | 3 | 333 | 45 | 697 |
| src/data_merge/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/data_merge/correct_locations_w_OSM.py | 6 | 0 | 0 | 39 | 83 | 23 |
| src/data_merge/final_data_merge.py | 7 | 0 | 0 | 30 | 94 | 9 |
| src/data_merge/merge_seg_results.py | 5 | 0 | 0 | 14 | 84 | 17 |
| src/download_pop.py | 17 | 0 | 0 | 51 | 12 | 256 |
| src/figures_scripts/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/figures_scripts/composite_area_population_plots.py | 10 | 0 | 0 | 24 | 32 | 116 |
| src/figures_scripts/convert_voronoi_to_geojson_for_map.py | 1 | 0 | 0 | 5 | 85 | 5 |
| src/figures_scripts/piechart_figure.py | 9 | 0 | 0 | 49 | 0 | 249 |
| src/figures_scripts/piechart_interactive.py | 8 | 0 | 0 | 29 | 0 | 141 |
| src/figures_scripts/pop_at_risk_figures.py | 4 | 0 | 0 | 34 | 0 | 151 |
| src/industrial_analysis/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/industrial_analysis/download_and_vectorize.py | 10 | 0 | 0 | 67 | 35 | 177 |
| src/industrial_analysis/find_unconnected_industrial_areas.py | 6 | 0 | 0 | 32 | 84 | 27 |
| src/pipelines.py | 6 | 0 | 0 | 40 | 91 | 15 |
| src/pop_at_risk_river_calculations/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/pop_at_risk_river_calculations/assign_rivers_to_basin.py | 4 | 0 | 0 | 14 | 68 | 29 |
| src/pop_at_risk_river_calculations/create_rasters.py | 9 | 0 | 0 | 63 | 0 | 324 |
| src/pop_at_risk_river_calculations/find_diff_pop.py | 6 | 0 | 0 | 21 | 95 | 6 |
| src/pop_at_risk_river_calculations/find_intersection_river.py | 9 | 0 | 0 | 30 | 0 | 146 |
| src/pop_at_risk_river_calculations/find_pop_in_danger_pop.py | 9 | 0 | 0 | 15 | 83 | 19 |
| src/pop_at_risk_river_calculations/find_unserved_pop.py | 2 | 0 | 0 | 7 | 91 | 4 |
| src/pop_at_risk_river_calculations/impact_polygons_pop.py | 13 | 0 | 0 | 72 | 73 | 83 |
| src/pop_validation_scripts/__init__.py | 0 | 0 | 0 | 0 | 100 | 0 |
| src/pop_validation_scripts/eu_comparison.py | 3 | 0 | 0 | 19 | 73 | 34 |
| src/pop_validation_scripts/hw_comparison.py | 7 | 0 | 0 | 31 | 89 | 17 |
| src/pop_validation_scripts/verification_script.py | 2 | 0 | 0 | 7 | 91 | 4 |
| src/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py | 14 | 0 | 0 | 37 | - | - |
| src/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py | 8 | 0 | 0 | 40 | - | - |
| src/starter.py | 9 | 0 | 0 | 42 | 95 | 7 |

## Exported Symbols By File

### src/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/add_pop.py
- functions: find_country_tif_files, find_newest_country_tif_files, intersect_all_files, intersect_single_file, main, orchestrate_intersections
- classes: (none)
- methods: (none)
- branch_nodes: 45

### src/annotation_scripts/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/annotation_scripts/annotations_inspection.py
- functions: get_stratified_sample, main, organize_files_by_category, plot_category_distribution, sanitize_folder_name
- classes: (none)
- methods: (none)
- branch_nodes: 14

### src/annotation_scripts/copy_falsy_images.py
- functions: main
- classes: (none)
- methods: (none)
- branch_nodes: 6

### src/annotation_scripts/download_bing_annotate.py
- functions: annotate_bboxes_parallel, download_bing_image, download_random_image, draw_annotations, draw_rotated_text_with_padding, draw_text_with_padding, georef_write, get_image, image_bounds_mercator, linestring_angle, log_gdf_preview, mercator_to_pixel, process_bbox, safe_wkt_load, split_grids_for_instance
- classes: (none)
- methods: (none)
- branch_nodes: 59

### src/annotation_scripts/merge_annotations.py
- functions: _clean_field, decode_gen_text, main, parse_idx_from_image_name
- classes: (none)
- methods: (none)
- branch_nodes: 10

### src/annotation_scripts/NEW_01_GENERATEGRIDS.py
- functions: main, point_to_square
- classes: (none)
- methods: (none)
- branch_nodes: 7

### src/annotation_scripts/NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py
- functions: clean_columns, create_tasks, elements_to_gdf, find_bbox, inner, main, query_overpass, row_operation, timer, wrap
- classes: (none)
- methods: (none)
- branch_nodes: 50

### src/annotation_scripts/NEW_03_WASTEWATERJOIN_GEOJSON.py
- functions: build_cast_expr, build_spatial_index, cluster_points, clusters_to_bboxes, compute_centroids, convert_geojson_to_parquet, discover_parquet_schema, from_wkb_modified, get_parquet_schema_info, load_geodata, main, merge_bboxes_sql, merge_parquets_sql, parallel_convert_geojsons, sanitize_gdf_columns, write_geodata
- classes: (none)
- methods: (none)
- branch_nodes: 52

### src/annotation_scripts/NEW_04_EXPORTGEOTIFF.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 3

### src/combine_watersheds.py
- functions: extract_and_merge_geodata, main
- classes: (none)
- methods: (none)
- branch_nodes: 10

### src/create_voronoi.py
- functions: _compute_k, _detection_confidence, _filter_requested_approaches, _quote_identifier, _site_detection_counts, _size_ceiling, assign_sites_streaming, auto_weight_scale, buffer_geometry, calculate_area, calculate_buffer, cluster_point_indices, cluster_points, create_centroid_points, create_ranges, create_weights, default_distance_additive, default_distance_multiplicative, dfs, dissolve_overlapping_geometries, dissolve_overlapping_geometries_fast, download_overture_maps, drop_duplicates, ensure_output_dir_for_file, estimate_utm_crs, estimate_utm_epsg, extract_contours_cv2, extract_contours_rasterio, extract_contours_scipy, extract_site_coordinates, finalize_gdf, flush_results, geometry_contains_points, initialize_voronoi_weights, intersect_with_polygon_sindex, intersect_with_polygons_db, intersect_with_polygons_parallelized, intersects_with_country_db, is_valid_geom, iter_voronoi_args, nearest_neighbor_distances_and_median, normalize_column_to_rounded_str, normalize_plane, orchestrate_overlaps, orchestrate_voronoi_weights, process_centroid, resolve_polygon_overlaps, round_function, voronoi_worker, weighted_voronoi
- classes: UnionFind
- methods: UnionFind.__init__, UnionFind.find, UnionFind.union
- branch_nodes: 333

### src/data_merge/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/data_merge/correct_locations_w_OSM.py
- functions: coordinate_corr_locations_wOSM, corr_locations_wOSM, create_HW_geom, create_corrected_geom, enrich_country_with_duckdb, main
- classes: (none)
- methods: (none)
- branch_nodes: 39

### src/data_merge/final_data_merge.py
- functions: cluster_point_indices, cluster_points, find_meter_coordinates, find_safe_epsg, find_unmatched_targets, get_best_points, main
- classes: (none)
- methods: (none)
- branch_nodes: 30

### src/data_merge/merge_seg_results.py
- functions: assign_to_nearest, main, merge_new, merge_old, parse_args
- classes: (none)
- methods: (none)
- branch_nodes: 14

### src/download_pop.py
- functions: add_country_url, download_file, download_save_and_unzip_pop, download_save_and_unzip_pops, extract_first_wildcard, find_files, find_type, get_iso_codes, get_urls, get_urls_from_hdx, main, mosaic_large_rasters, process_all_countries, process_single_country, rasterize_csv, resample_raster, try_extract_country
- classes: (none)
- methods: (none)
- branch_nodes: 51

### src/figures_scripts/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/figures_scripts/composite_area_population_plots.py
- functions: _bleach_color, add_one_to_one_line, build_country_table, clip_outliers, main, make_category_color_map, make_histogram_plot, make_scatter_plot, parse_args, resolve_zonal_sum_column
- classes: (none)
- methods: (none)
- branch_nodes: 24

### src/figures_scripts/convert_voronoi_to_geojson_for_map.py
- functions: main
- classes: (none)
- methods: (none)
- branch_nodes: 5

### src/figures_scripts/piechart_figure.py
- functions: aggregate_by_country, calculate_size, ensure_population_percentage_column, get_pos, main, plot_pie, plot_splitted_piechart, resolve_zonal_sum_columns, round_numbers
- classes: (none)
- methods: (none)
- branch_nodes: 49

### src/figures_scripts/piechart_interactive.py
- functions: aggregate_by_country, calculate_size, ensure_population_percentage_column, get_pie_svg, main, polar_to_cartesian, resolve_zonal_sum_column, sector_path
- classes: (none)
- methods: (none)
- branch_nodes: 29

### src/figures_scripts/pop_at_risk_figures.py
- functions: _robust_bounds, create_impact_polygon_plots, create_single_plot, main
- classes: (none)
- methods: (none)
- branch_nodes: 34

### src/industrial_analysis/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/industrial_analysis/download_and_vectorize.py
- functions: _dissolve_by_overlap_groups, _find_raster_dirs, _repair_geometry, _vectorize_and_merge, add_boundary_info, download_file, main, merge_geodataframes, vectorize_raster_file, vectorize_rasters_parallel
- classes: (none)
- methods: (none)
- branch_nodes: 67

### src/industrial_analysis/find_unconnected_industrial_areas.py
- functions: filter_industrial_wwtps, find_unconnected_areas, load_industrial_areas, load_wwtps, main, run_voronoi_for_wwtps
- classes: (none)
- methods: (none)
- branch_nodes: 32

### src/pipelines.py
- functions: _compute_mean_2_nnd_web_mercator, _resolve_configured_callable, create_output_paths, create_pop_output_paths, prepare_data, run_voronoi_approach
- classes: (none)
- methods: (none)
- branch_nodes: 40

### src/pop_at_risk_river_calculations/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/pop_at_risk_river_calculations/assign_rivers_to_basin.py
- functions: assign_hybas_id_by_length, extract_first_digit, main, orchestrate_intersections
- classes: (none)
- methods: (none)
- branch_nodes: 14

### src/pop_at_risk_river_calculations/create_rasters.py
- functions: _sanitize_polygon_geom, extract_worldpop_universal, geotiff_exists_and_valid, main, orchestrate_country_intersection, orchestrate_intersections, parse_args, polygon_raster_sign_from_gdf, shard_tif_dict
- classes: (none)
- methods: (none)
- branch_nodes: 63

### src/pop_at_risk_river_calculations/find_diff_pop.py
- functions: find_difference, find_differences, main, parse_args, parse_bool, process_epsg_group
- classes: (none)
- methods: (none)
- branch_nodes: 21

### src/pop_at_risk_river_calculations/find_intersection_river.py
- functions: assign_main_riv, assign_river_juncture, build_graph, find_common_intersection, find_intersection_id, main, optimize_river_lookup, orchestrate_river_assignment, orchestrate_settlement_river_intersections
- classes: (none)
- methods: (none)
- branch_nodes: 30

### src/pop_at_risk_river_calculations/find_pop_in_danger_pop.py
- functions: assign_tile_to_df, assign_tile_to_df_worker, find_bbox, find_tiles_in_a_country, find_tiles_in_countries, finding_tiles, group_tile_population_sums, main, rename_cols
- classes: (none)
- methods: (none)
- branch_nodes: 15

### src/pop_at_risk_river_calculations/find_unserved_pop.py
- functions: create_unserved_pop, main
- classes: (none)
- methods: (none)
- branch_nodes: 7

### src/pop_at_risk_river_calculations/impact_polygons_pop.py
- functions: batch_estimate_utm_epsg, calculate_kt, calculate_load_ratio, calculate_radius, create_dicts, create_impact_polygons, generate_single_segment_plume, get_runtime_params, init_worker, invert_calculate_load, main, orchestrate_logic, parallel_dissolve
- classes: (none)
- methods: (none)
- branch_nodes: 72

### src/pop_validation_scripts/__init__.py
- functions: (none)
- classes: (none)
- methods: (none)
- branch_nodes: 0

### src/pop_validation_scripts/eu_comparison.py
- functions: composite_histogram, main, orchestrate_single
- classes: (none)
- methods: (none)
- branch_nodes: 19

### src/pop_validation_scripts/hw_comparison.py
- functions: composite_histogram, extract_voronoi_parameters, main, multiples, ndvi, orchestrate_single, replace_inf
- classes: (none)
- methods: (none)
- branch_nodes: 31

### src/pop_validation_scripts/verification_script.py
- functions: find_verification_watersheds, main
- classes: (none)
- methods: (none)
- branch_nodes: 7

### src/sensitivity_analysis_scripts/compare_pop_sweep_hw_eu.py
- functions: _init_summary_worker, _process_single_record, _safe_zscore, _to_percentile_goodness, build_alias_order, build_summary_table, compute_sensitivity_metrics, get_latest_year_column, list_pop_output_files, main, make_aliases, parse_pop_output_path, plot_split_metric_profiles, plot_split_score_bars
- classes: (none)
- methods: (none)
- branch_nodes: 37

### src/sensitivity_analysis_scripts/create_voronoi_parallel_sweep.py
- functions: execute_with_job_count, filter_combinations_by_task, generate_parameter_combinations, main, output_exists_for_combo, run_voronoi_job, setup_logging, split_combinations_into_jobs
- classes: (none)
- methods: (none)
- branch_nodes: 40

### src/starter.py
- functions: _normalize_cfg_path, _normalize_optional_cli_value, _parse_optional_bool, _parse_optional_float, _parse_optional_int, _parse_optional_weight_func, f, load_config, parse_config_overrides
- classes: (none)
- methods: (none)
- branch_nodes: 42
