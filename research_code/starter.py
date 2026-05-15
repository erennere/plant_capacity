"""
Configuration loader module for plant capacity spatial data science.

Provides centralized configuration loading with CLI argument parsing,
path template expansion, and parameter initialization.
"""

import os
import sys
import yaml
import math


def _normalize_optional_cli_value(value, preserve_empty=False):
    """Normalize optional CLI values, treating empty/None/NaN/null as omitted."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if normalized == "":
            return "" if preserve_empty else None
        if normalized.lower() in {"none", "nan", "null"}:
            return None
        return normalized
    return value


def _parse_optional_int(value, field_name):
    """Parse optional integer overrides."""
    value = _normalize_optional_cli_value(value)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field_name} '{value}'. Must be an integer.")

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


def _parse_optional_bool(value, field_name):
    """Parse optional boolean overrides from common truthy/falsey strings."""
    value = _normalize_optional_cli_value(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid {field_name} '{value}'. Must be a boolean value.")


def _parse_optional_float(value, field_name):
    """Parse optional float overrides."""
    value = _normalize_optional_cli_value(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field_name} '{value}'. Must be a number.")


def parse_config_overrides(args=None, argv=None, start_index=1):
    """Parse optional config overrides for ``load_config``.

    Parameters
    ----------
    args : argparse.Namespace or None, optional
        Parsed argparse namespace with optional ``level``, ``version``,
        ``buffer``, ``weight_method``, ``weight_func``,
        ``dynamic_buffering``, and ``dynamic_buffer_k`` attributes.
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
        level = _normalize_optional_cli_value(getattr(args, "level", None))
        version = _normalize_optional_cli_value(getattr(args, "version", None))
        raw_buffer = _normalize_optional_cli_value(getattr(args, "buffer", None))
        weight_method = _normalize_optional_cli_value(getattr(args, "weight_method", None))
        raw_weight_func = _normalize_optional_cli_value(getattr(args, "weight_func", None), preserve_empty=True)
        raw_dynamic_buffering = _normalize_optional_cli_value(getattr(args, "dynamic_buffering", None))
        raw_dynamic_buffer_k = _normalize_optional_cli_value(getattr(args, "dynamic_buffer_k", None))
    else:
        argv = sys.argv if argv is None else argv
        level = _normalize_optional_cli_value(argv[start_index] if len(argv) > start_index else None)
        version = _normalize_optional_cli_value(argv[start_index + 1] if len(argv) > start_index + 1 else None)
        raw_buffer = _normalize_optional_cli_value(argv[start_index + 2] if len(argv) > start_index + 2 else None)
        weight_method = _normalize_optional_cli_value(argv[start_index + 3] if len(argv) > start_index + 3 else None)
        raw_weight_func = _normalize_optional_cli_value(
            argv[start_index + 4] if len(argv) > start_index + 4 else None,
            preserve_empty=True,
        )
        raw_dynamic_buffering = _normalize_optional_cli_value(argv[start_index + 5] if len(argv) > start_index + 5 else None)
        raw_dynamic_buffer_k = _normalize_optional_cli_value(argv[start_index + 6] if len(argv) > start_index + 6 else None)

    buffer = _parse_optional_int(raw_buffer, "buffer")

    weight_func = _parse_optional_weight_func(raw_weight_func, "weight_func")
    dynamic_buffering = _parse_optional_bool(raw_dynamic_buffering, "dynamic_buffering")
    dynamic_buffer_k = _parse_optional_float(raw_dynamic_buffer_k, "dynamic_buffer_k")

    return {
        "level": level,
        "version": version,
        "buffer": buffer,
        "weight_method": weight_method,
        "weight_func": weight_func,
        "dynamic_buffering": dynamic_buffering,
        "dynamic_buffer_k": dynamic_buffer_k,
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
    dynamic_buffering=None,
    dynamic_buffer_k=None,
):
    """Load and parse YAML configuration with optional CLI overrides.

    All parameter defaults live exclusively in ``config.yaml``.  The optional
    keyword arguments are *runtime overrides* that the shell wrappers inject;
    when a value is ``None`` the YAML default is used unchanged.  No fallback
    defaults are hard-coded in this function.

    Path templates in ``config.yaml`` are expanded using ``str.format`` with
    the variables ``{data_dir}``, ``{version}``, ``{level}``, ``{buffer}``,
    ``{weight_type}``, ``{weight_func}``, ``{final_data_dir}``,
    ``{extra_points_dir}``, ``{annotations_dir}``, ``{dl_dir}``,
    ``{figures_dir}``, and ``{industrial_min_cells}``.

    Parameters
    ----------
    config : str, default="config.yaml"
        Path to the YAML configuration file.
    level : str or None, optional
        Processing level override (e.g. ``"7"``).  Falls back to
        ``arguments.default_level`` in the YAML.
    version : str or None, optional
        Data-version override (e.g. ``"2"``).  Falls back to
        ``arguments.default_version`` in the YAML.
    buffer : int or None, optional
        Buffer radius override in metres.  Falls back to ``params.buffer``.
    weight_method : str or None, optional
        Weight-transform override (``'linear'`` | ``'square_root'`` |
        ``'logarithmic'`` | ``'sigmoid'``).  Falls back to
        ``params.weight_method``.
    weight_func : str or None, optional
        Distance-weighting mode override.  Accepted values are ``"mult"``,
        ``"add"``, or ``""``.  Falls back to ``params.weight_func``.
    dynamic_buffering : bool or None, optional
        Per-site dynamic buffering flag override.  Falls back to
        ``params.dynamic_buffering``.
    dynamic_buffer_k : float or None, optional
        Dynamic buffer scale-factor override.  Falls back to
        ``params.dynamic_buffer_k``.

    Returns
    -------
    dict
        Flat configuration dictionary consumed by all pipeline modules.
        Keys include ``level``, ``version``, ``buffer``, ``buffer_path_token``,
        ``weight_method``, ``weight_func``, ``weight_type``,
        ``weight_func_suffix``, ``max_workers``, ``n_points``, all numeric
        Voronoi tuning parameters, ``dynamic_buffering``, ``dynamic_buffer_k``,
        all boolean flags, ``distance_fn`` (resolved callable),
        ``prepare_data_fn``, ``calculate_area_fn``, ``calculate_buffer_fn``
        (all as raw string names for deferred resolution by
        ``_resolve_configured_callable``), and a ``paths`` sub-dict of 50+
        expanded absolute filesystem paths.

    Notes
    -----
    For CLI-driven scripts use ``parse_config_overrides()`` to collect
    overrides from argparse or ``sys.argv`` before passing them here.
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

    dynamic_buffering = cfg['params']['dynamic_buffering'] if dynamic_buffering is None else dynamic_buffering
    dynamic_buffering = _parse_optional_bool(dynamic_buffering, "dynamic_buffering")

    dynamic_buffer_k = cfg['params']['dynamic_buffer_k'] if dynamic_buffer_k is None else dynamic_buffer_k
    dynamic_buffer_k = _parse_optional_float(dynamic_buffer_k, "dynamic_buffer_k")

    final_data_dir = cfg["paths"]["final_data_dir"]
    annotations_dir = cfg["paths"]["annotations_dir"]
    dl_dir = cfg["paths"]["dl_dir"]
    figures_dir = cfg["paths"]["figures_dir"]
    
    weight_type_map = {
        'linear': 'li',
        'square_root': 'sq',
        'logarithmic': 'log',
        'sigmoid': 'sig'
    }
    if weight_method not in weight_type_map:
        valid_methods = ', '.join(weight_type_map.keys())
        raise ValueError(
            f"Invalid weight_method '{weight_method}'. Must be one of: {valid_methods}."
        )
    weight_type = weight_type_map[weight_method]
    weight_func_suffix = {
        "mult": "_mult",
        "add": "_add",
        "": "",
    }[weight_func]

    distance_fn = default_distance_multiplicative if weight_func in {"", "mult"} else default_distance_additive

    # Keep numeric buffer for computation, but use a dedicated token for
    # path formatting so dynamic-buffer runs are grouped by k-value.
    if dynamic_buffering:
        buffer_path_token = f"k{str(dynamic_buffer_k).replace('.', '_')}"
    else:
        buffer_path_token = str(int(buffer))
    industrial_min_cells = str(cfg['params']['industrial_min_cells'])

    def f(path):
        return path.format(
            data_dir=data_dir,
            version=version,
            latest_url=cfg["s3"]["latest_url"],
            extra_points_dir=extra_points_dir,
            level=level,
            buffer=buffer_path_token,
            final_data_dir=final_data_dir,
            annotations_dir=annotations_dir,
            dl_dir=dl_dir,
            weight_type=weight_type,
            weight_func=weight_func_suffix,
            figures_dir=figures_dir,
            industrial_min_cells=industrial_min_cells
        )

    paths = {
        "data_dir": data_dir,
        "figures_dir": os.path.join(data_dir, figures_dir),
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
        "raster_country_stats_filepath": f(cfg['paths']['raster_country_stats_filepath']),
        "non_served_outpath" : f(cfg['paths']['non_served_outpath']),
        "non_served_above_threshold_outpath" : f(cfg['paths']['non_served_above_threshold_outpath']),
        "non_served_nxt_river_outpath" : f(cfg['paths']['non_served_nxt_river_outpath']),
        "impact_pop_polygons_outpath": f(cfg['paths']['impact_pop_polygons_outpath']),
        "industrial_areas_temp_db_path" : f(cfg['paths']['industrial_areas_temp_db_path']),
        "industrial_areas_ohsome_parquet_filepath": f(cfg['paths']['industrial_areas_ohsome_parquet_filepath']),
        "industrial_raster_persistent_dir": f(cfg['paths']['industrial_raster_persistent_dir']),
        "industrial_merged_filepath": f(cfg['paths']['industrial_merged_filepath']),
        "industrial_unconnected_output": f(cfg['paths']['industrial_unconnected_output']),
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
        "leaflet_geojson_filepath": f(cfg['paths']['leaflet_geojson_filepath']),
        "composite_histogram_filepath": f(cfg['paths']['composite_histogram_filepath']),
        "composite_scatter_filepath": f(cfg['paths']['composite_scatter_filepath'])
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
        "buffer_path_token": buffer_path_token,
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
        "zoom_level": cfg["params"]["zoom_level"],
        "remove_industrial": flags['remove_industrial'],  
        "industrial_category_numbers": cfg['params']['industrial_category_numbers'],
        "zonal_sum_default_column": cfg['params']['zonal_sum_default_column'],
        "basin_column_name": cfg['params']['basin_column_name'],
        "country_output_column": cfg['params']['country_output_column'],
        "country_boundary_column": cfg['params']['country_boundary_column'],
        "site_id_column": cfg['params']['site_id_column'],
        "old_site_id_column": cfg['params']['old_site_id_column'],
        "calculate_area_fn": cfg['params']['calculate_area_fn'],
        "calculate_buffer_fn": cfg['params']['calculate_buffer_fn'],
        "area_fn_kwargs": cfg['params']['area_fn_kwargs'],
        "calculate_buffer_kwargs": {
                "buffer": buffer,
                "dynamic_buffering": dynamic_buffering,
                "min_buffer": cfg['params']['min_buffer'],
                "max_buffer": cfg['params']['max_buffer'],
                "k_min": cfg['params']['k_min'],
                "k_max": cfg['params']['k_max'],
                "conf_threshold": cfg['params']['detection_confidence_threshold'],
                "k_value": dynamic_buffer_k
        },
        "prepare_data_fn": cfg['params']['prepare_data_fn'],
        "min_pixels": cfg['params']['min_pixels'],
        "impact_polygons_pop_params": cfg['impact_polygons_pop_params'],
        "legacy_merge": flags['legacy_merge'],
        "overwrite": cfg['params']['overwrite'],
        "voronoi_overwrite": cfg['params']['voronoi_overwrite'],
        "pop_voronoi_overwrite": cfg['params']['pop_voronoi_overwrite'],
        "temp_voronoi_overwrite": cfg['params']['temp_voronoi_overwrite'],
        "industrial_vectorize_overwrite": cfg['params']['industrial_vectorize_overwrite'],
        "industrial_unconnected_overwrite": cfg['params']['industrial_unconnected_overwrite'],
        "return_boolean": flags['return_boolean'],
        "flush_size": cfg['params']['flush_size'],
        "dynamic_buffering": dynamic_buffering,
        "dynamic_buffer_k": float(dynamic_buffer_k) if dynamic_buffer_k is not None else None,
        "min_buffer": cfg['params']['min_buffer'],
        "industrial_zenodo_url": cfg['params']['industrial_zenodo_url'],
        "industrial_min_cells": cfg['params']['industrial_min_cells'],
        "industrial_persist_rasters": cfg['params']['industrial_persist_rasters'],
        "industrial_simplify_tolerance": cfg['params']['industrial_simplify_tolerance']
    }
