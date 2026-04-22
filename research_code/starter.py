"""
Configuration loader module for plant capacity spatial data science.

Provides centralized configuration loading with CLI argument parsing,
path template expansion, and parameter initialization.
"""

import os
import sys
import yaml


def _parse_optional_weight_func(value, field_name="weight_func"):
    """Parse optional weight function mode override.

    Accepted values are ``"mult"``, ``"add"``, or ``""``.
    """
    if value is None:
        return None

    normalized = str(value).strip().lower()
    if normalized in {"mult", "add", ""}:
        return normalized
    raise ValueError(f"Invalid {field_name} '{value}'. Must be one of: mult, add, ''.")


def parse_config_overrides(args=None, argv=None, start_index=1):
    """Parse optional config overrides for ``load_config``.

    Parameters
    ----------
    args : argparse.Namespace or None, optional
        Parsed argparse namespace with optional ``level``, ``version``,
        ``buffer``, ``weight_method``, and ``weight_func`` attributes.
        When provided, ``argv`` and ``start_index`` are ignored.
    argv : list or None, optional
        ``sys.argv``-style sequence.  Defaults to ``sys.argv``.
    start_index : int, default=1
        Index in ``argv`` where ``level`` begins (use ``2`` when a required
        positional argument precedes the overrides).

    Returns
    -------
    dict
        Optional ``load_config`` overrides keyed by parameter name.
    """
    if args is not None:
        level = getattr(args, "level", None) or None
        version = getattr(args, "version", None) or None
        raw_buffer = getattr(args, "buffer", None)
        weight_method = getattr(args, "weight_method", None) or None
        raw_weight_func = getattr(args, "weight_func", None)
    else:
        argv = sys.argv if argv is None else argv
        level = argv[start_index] if len(argv) > start_index and argv[start_index] else None
        version = argv[start_index + 1] if len(argv) > start_index + 1 and argv[start_index + 1] else None
        raw_buffer = argv[start_index + 2] if len(argv) > start_index + 2 and argv[start_index + 2] else None
        weight_method = argv[start_index + 3] if len(argv) > start_index + 3 and argv[start_index + 3] else None
        raw_weight_func = argv[start_index + 4] if len(argv) > start_index + 4 and argv[start_index + 4] else None

    buffer = None
    if raw_buffer is not None:
        try:
            buffer = int(raw_buffer)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid buffer '{raw_buffer}'. Must be an integer.")

    weight_func = _parse_optional_weight_func(raw_weight_func, "weight_func")

    return {
        "level": level,
        "version": version,
        "buffer": buffer,
        "weight_method": weight_method,
        "weight_func": weight_func,
    }


def _normalize_cfg_path(path_value, base_dir):
    """Return an absolute filesystem path for a config entry, passing URLs through unchanged."""
    if not isinstance(path_value, str):
        return path_value

    # Keep URI-like values (e.g. s3://...) unchanged.
    if "://" in path_value:
        return path_value

    expanded = os.path.expanduser(path_value)
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    return os.path.abspath(os.path.join(base_dir, expanded))


def load_config(
    config="config.yaml",
    level=None,
    version=None,
    buffer=None,
    weight_method=None,
    weight_func=None,
):
    """Load and parse YAML configuration with optional CLI overrides.

    Constructs paths by expanding template strings in ``config.yaml`` using
    ``{data_dir}``, ``{version}``, ``{level}``, ``{buffer}``, and related
    variables.

    Parameters
    ----------
    config : str, default="config.yaml"
        Path to the YAML configuration file.
    level : str or None, optional
        Processing level override; falls back to ``arguments.default_level``.
    version : str or None, optional
        Data version override; falls back to ``arguments.default_version``.
    buffer : int or None, optional
        Buffer distance in metres override; falls back to ``params.buffer``.
    weight_method : str or None, optional
        Weight transformation override; falls back to ``params.weight_method``.
    weight_func : str or None, optional
        Weighted-distance mode override. Accepted values are ``"mult"``,
        ``"add"``, or ``""``. Falls back to ``params.weight_func``.

    Returns
    -------
    dict
        Flat configuration dictionary with keys including ``level``,
        ``version``, ``buffer``, ``weight_method``, ``weight_func``,
        ``max_workers``, ``n_points``, numeric Voronoi parameters, processing
        flags, ``distance_fn``, and a ``paths`` sub-dict of 40+ expanded
        filesystem paths.

    Notes
    -----
    For CLI-driven scripts use ``parse_config_overrides()`` to derive
    runtime overrides from argparse or ``sys.argv`` before passing them here.
    """
    # Lazy import to avoid circular import issues
    try:
        from .create_voronoi import default_distance_additive, default_distance_multiplicative
    except ImportError:  # Support running as a top-level script
        from create_voronoi import default_distance_additive, default_distance_multiplicative
    
    config_path = os.path.abspath(config)
    config_dir = os.path.dirname(config_path)

    with open(config_path) as stream:
        cfg = yaml.safe_load(stream)

    # Runtime flags
    level = cfg["arguments"]["default_level"] if level is None else level
    version = cfg["arguments"]["default_version"] if version is None else version

    # paths
    data_dir = cfg["paths"]["data_dir"]
    extra_points_dir = cfg["paths"]["extra_points_dir"]
    buffer = cfg['params']['buffer'] if buffer is None else buffer
    weight_method = cfg['params']['weight_method'] if weight_method is None else weight_method

    if weight_func is None:
        weight_func = cfg['params']['weight_func']
    weight_func = _parse_optional_weight_func(weight_func, "weight_func")
    if weight_func is None:
        weight_func = ""

    final_data_dir = cfg["paths"]["final_data_dir"]
    annotations_dir = cfg["paths"]["annotations_dir"]
    dl_dir = cfg["paths"]["dl_dir"]
    
    weight_type = {
        'linear': 'li',
        'square_root': 'sq',
        'logarithmic': 'log',
        'sigmoid': 'sig'
    }[weight_method]
    weight_func_suffix = {
        "mult": "_mult",
        "add": "_add",
        "": "",
    }[weight_func]

    distance_fn = default_distance_multiplicative if weight_func in {"", "mult"} else default_distance_additive

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
            dl_dir=dl_dir,
            weight_type=weight_type,
            weight_func=weight_func_suffix,
        )

    paths = {
        "data_dir": data_dir,
        "buffers_dir": f(cfg["paths"]["buffers_dir"]),
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
        "level": level,
        "version": version,
        "paths": paths,
        "buffer": buffer,
        "weight_method": weight_method, # linear, logarithmic, square_root, sigmoid
        "weight_type": weight_type, # li, log, sq, sig
        "weight_func": weight_func,
        "weight_func_suffix": weight_func_suffix, # _mult, _add, or ''
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
        "distance_fn": distance_fn,
        "annotations": cfg["annotations"],
        "figures": cfg["figures"],
        "credentials": cfg["credentials"],
        "add_pop_max_workers": cfg["params"]["add_pop_max_workers"],
        "weight_method": weight_method,
        "weight_func": weight_func,
        "zoom_level": cfg["params"]["zoom_level"],
        "remove_industrial": flags['remove_industrial'],
        "industrial_category_numbers": cfg['params']['industrial_category_numbers'],
        "min_pixels": cfg['params']['min_pixels'],
        "impact_polygons_pop_params": cfg['impact_polygons_pop_params'],
        "legacy_merge": flags['legacy_merge'],
        "overwrite": cfg['params']['overwrite'],
        "voronoi_overwrite": cfg['params']['voronoi_overwrite']
    }
