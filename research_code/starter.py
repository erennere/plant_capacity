"""
Configuration loader module for plant capacity spatial data science.

Provides centralized configuration loading with CLI argument parsing,
path template expansion, and parameter initialization.
"""

import os
import sys
import yaml


def _normalize_cfg_path(path_value, base_dir):
    """Return absolute filesystem path for cfg path entries, keeping URLs unchanged."""
    if not isinstance(path_value, str):
        return path_value

    # Keep URI-like values (e.g. s3://...) unchanged.
    if "://" in path_value:
        return path_value

    expanded = os.path.expanduser(path_value)
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    return os.path.abspath(os.path.join(base_dir, expanded))


def load_config(config="config.yaml"):
    """
    Load and parse YAML configuration file, construct paths with variable expansion.
    
    Builds complete configuration dictionary from config.yaml with CLI argument
    overrides. Expands all path templates using data_dir, version, and other
    configuration values.
    
    Args:
        config (str): Path to YAML configuration file (default: config.yaml)
        
    Returns:
        dict: Configuration dictionary containing:
            - weights_cond (bool): False if "all_world" passed as CLI arg, else True
            - particle (str): "all_world" or "global_south" based on weights_cond
            - level (str): Processing level from CLI or config default
            - version (str): Data version from CLI or config default
            - paths (dict): 40+ expanded file/directory paths for processing
            - buffer (int): Buffer distance in meters
            - max_workers (int): Number of parallel workers to use
            - n_points (int): Grid resolution for Voronoi generation
            - threshold, sigma, percent_threshold: Voronoi parameters
            - percent_verification: Verification split ratio
            - osm_threshold, eu_utm, rad: Additional processing parameters
            - scipy_true, cv2_true: Boolean flags for contour extraction methods
            - city_voronoi, csv_files, duckdb_cond: Processing flags
            - sindex_concurrency, eu_correction: Feature flags
            - distance_fn: Distance function for Voronoi weighting
            - annotations, figures_params, credentials: Metadata & auth
            
    Path Templates (variable expansion):
        Uses string.format() to expand {data_dir}, {version}, {level}, {buffer},
        {final_data_dir}, {extra_points_dir}, {annotations_dir}, {latest_url}
        
    CLI Arguments:
        sys.argv[1]: "all_world" for global dataset (default: global_south)
        sys.argv[2]: Processing level override (default: config default)
        sys.argv[3]: Data version override (default: config default)
        
    Notes:
        COMPLEX: 40+ paths constructed with format string expansion
        
        ASSUMPTIONS:
        - config.yaml exists in current directory with required structure
        - Expected sections: paths, params, booleans, s3, arguments, annotations, figures, credentials
        
        HARDCODED LOGIC: particle = "all_world" OR "global_south" based on
        single boolean flag. Other geographic splits handled elsewhere.
        
        RETURNS OBJECT: All config values in single dict for convenient
        unpacking: cfg = load_config(); paths = cfg['paths']; buffer = cfg['buffer']
    """
    # Lazy import to avoid circular import issues
    try:
        from .create_voronoi import default_distance_multiplicative
    except ImportError:  # Support running as a top-level script
        from create_voronoi import default_distance_multiplicative
    
    config_path = os.path.abspath(config)
    config_dir = os.path.dirname(config_path)

    with open(config_path) as stream:
        cfg = yaml.safe_load(stream)

    # CLI arguments
    #weights_cond = False if len(sys.argv) > 1 and sys.argv[1] == "all_world" else True
    weights_cond = True
    level = cfg["arguments"]["default_level"]
    version = cfg["arguments"]["default_version"]
    particle = "all_world" if weights_cond == False else "global_south"

    # paths
    data_dir = cfg["paths"]["data_dir"]
    extra_points_dir = cfg["paths"]["extra_points_dir"]
    buffer = cfg['params']['buffer']
    final_data_dir = cfg["paths"]["final_data_dir"]
    annotations_dir = cfg["paths"]["annotations_dir"]
    dl_dir = cfg["paths"]["dl_dir"]
    def f(path):
        return path.format(
            data_dir=data_dir,
            version=version,
            latest_url=cfg["s3"]["latest_url"],
            extra_points_dir=extra_points_dir,
            level=level,
            buffer=buffer,
            final_data_dir=final_data_dir,
            annotations_dir=annotations_dir,
            dl_dir=dl_dir
        )

    paths = {
        "data_dir": data_dir,
        "pop_dir" : f(cfg["paths"]["pop_dir"]),
        "watersheds_zip_dir" : f(cfg["paths"]["watersheds_zip_dir"]),
        "voronoi_dir": f(cfg["paths"]["voronoi_dir"]),
        "verification_dir": f(cfg["paths"]["verification_dir"]),
        "bboxes": f(cfg["paths"]["bboxes"]),
        "cities": f(cfg["paths"]["cities"]),
        "watershed": f(cfg["paths"]["watershed"]),
        "rivershed": f(cfg["paths"]["rivershed"]),
        "rivershed_output_path": f(cfg["paths"]["rivershed_output_path"]),
        "overture": f(cfg["paths"]["overture"]),
        "hydrowaste": f(cfg["paths"]["hydrowaste"]),
        "corrected": f(cfg["paths"]["corrected_all"])
                    if not weights_cond
                    else f(cfg["paths"]["seg_corrected_south"]),
        "overture_s3_url": cfg["s3"]["divisions"].format(latest_url=cfg["s3"]["latest_url"]),
        "dl_dir": f(cfg["paths"]["dl_dir"]),
        "dl_zipfile": f(cfg["paths"]["dl_zipfile"]),
        "dl_mapfile": f(cfg["paths"]["dl_mapfile"]),
        "seg_corrected_south": f(cfg["paths"]["seg_corrected_south"]),
        "corrected_south": f(cfg["paths"]["corrected_south"]),
        "corrected_all_filepath": f(cfg["paths"]["corrected_all"]),
        "new_points_filepath": f(cfg["paths"]["new_points_filepath"]),
        "eu_ref_filepath" : f(cfg["paths"]["eu_ref_filepath"]),
        "canada_filepath" : f(cfg["paths"]["canada_filepath"]),
        "us_filepath" : f(cfg["paths"]["us_filepath"]),
        "germany_filepath" : f(cfg["paths"]["germany_filepath"]),
        "osmgeo_filepath" : f(cfg["paths"]["osmgeo_filepath"]),
        "paul_corrected_filepath": f(cfg["paths"]["paul_corrected_filepath"]),
        "pop_tif_dir": f(cfg["paths"]["pop_tif_dir"]),
        "pop_output_dir": f(cfg["paths"]["pop_output_dir"]),
        "pop_dif_output_dir": f(cfg["paths"]["pop_dif_output_dir"]),
        "WWTP_tif_dir": f(cfg["paths"]["WWTP_tif_dir"]),
        "hw_plots_dir": f(cfg["paths"]["hw_plots_dir"]),
        "eu_plots_dir": f(cfg["paths"]["eu_plots_dir"]),
        "us_new_filepath" : f(cfg["paths"]["us_new_filepath"]),
        "eu_new_filepath" : f(cfg["paths"]["eu_new_filepath"]),
        "thailand_filepath" : f(cfg["paths"]["thailand_filepath"]),
        "annotations_grid_dir":  f(cfg["paths"]["annotations_grid_dir"]),
        "annotations_by_osm_dir": f(cfg["paths"]["annotations_by_osm_dir"]),
        "csv_output_filepath" : f(cfg['paths']['csv_output_filepath']), 
        "non_served_outpath" : f(cfg['paths']['non_served_outpath']),
        "non_served_above_threshold_outpath" : f(cfg['paths']['non_served_above_threshold_outpath']),
        "non_served_nxt_river_outpath" : f(cfg['paths']['non_served_nxt_river_outpath']),
        "impact_pop_polygons_outpath": f(cfg['paths']['impact_pop_polygons_outpath']),
        "industrial_areas_temp_db_path" : f(cfg['paths']['industrial_areas_temp_db_path']),
        "industrial_areas_ohsome_parquet_filepath": f(cfg['paths']['industrial_areas_ohsome_parquet_filepath']),
        "seg_results_filepath": f(cfg['paths']['seg_results_filepath']),
        "pop_at_risk_output_filepath": f(cfg['paths']['pop_at_risk_output_filepath']),

        "annotated_images_output_dir": f(cfg['paths']['annotated_images_output_dir']),
        "annotations_verf_image_outpath_dir":  f(cfg['paths']['annotations_verf_image_outpath_dir']),
        "annotations_results_filepath" : f(cfg['paths']['annotations_results_filepath']),
        "annotations_images_dir" : f(cfg['paths']['annotations_images_dir']),
        "annotations_temp_parquet_dir" : f(cfg['paths']['annotations_temp_parquet_dir']),

        "country_boundaries_filepath": f(cfg['paths']['country_boundaries_filepath']),
        "interactive_piechart_html_filepath": f(cfg['paths']['interactive_piechart_html_filepath']),
        "static_piechart_filepath": f(cfg['paths']['static_piechart_filepath']),
        "leaflet_geojson_filepath": f(cfg['paths']['leaflet_geojson_filepath'])
    }

    # Normalize all configured filesystem paths once at load time.
    paths = {k: _normalize_cfg_path(v, config_dir) for k, v in paths.items()}

    params = cfg["params"]
    flags = cfg["booleans"]

    # Return everything in one object
    return {
        "weights_cond": weights_cond,
        "particle": particle,
        "level": level,
        "version": version,
        "paths": paths,
        "buffer": params["buffer"],
        "max_workers": params["max_workers"],
        "n_points": params["n_points"],
        "threshold": params["threshold"],
        "sigma": params["sigma"],
        "percent_threshold": params["percent_threshold"],
        "percent_verification": params["percent_verification"], 
        "osm_threshold": params["osm_threshold"],
        "eu_utm": params["eu_utm"], 
        "rad": params["rad"],
        "scipy_true": flags["scipy"],
        "cv2_true": flags["cv2"],
        "city_voronoi": flags["city_voronoi"],
        "csv_files": flags["csv_files"],
        "duckdb_cond": flags["duckdb"],
        "sindex_concurrency": flags["sindex_concurrency"],
        "eu_correction": flags["eu_correction"],
        "distance_fn": default_distance_multiplicative,
        "annotations": cfg["annotations"],
        "figures": cfg["figures"],
        "credentials": cfg["credentials"],
        "add_pop_max_workers": cfg["params"]["add_pop_max_workers"],
        "weight_method": cfg["params"]["weight_method"],
        "zoom_level": cfg["params"]["zoom_level"],
        "remove_industrial": flags['remove_industrial'],
        "industrial_category_numbers": cfg['params']['industrial_category_numbers'],
        "min_pixels": cfg['params']['min_pixels'],
        "impact_polygons_pop_params": cfg['impact_polygons_pop_params']
        
    }
