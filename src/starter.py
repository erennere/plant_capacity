"""Per-script configuration loading and resolution for plant capacity workflows."""

from __future__ import annotations

import math
import os
from string import Formatter
import sys
from copy import deepcopy

import yaml


_MISSING = object()
_FORMATTER = Formatter()
_OVERRIDE_FIELDS = (
    ("level", 0, False),
    ("version", 1, False),
    ("buffer", 2, False),
    ("weight_method", 3, False),
    ("weight_func", 4, True),
    ("dynamic_buffering", 5, False),
    ("dynamic_buffer_k", 6, False),
)


class ConfigResolutionError(KeyError):
    """Raised when a script config section or key cannot be resolved."""

    def __init__(self, script_name: str, key: str, message: str):
        self.script_name = script_name
        self.key = key
        super().__init__(message)


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
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name} '{value}'. Must be an integer.") from exc


def _parse_optional_weight_func(value, field_name="weight_func"):
    """Parse optional weight function mode override."""
    if value is None:
        return None

    normalized = str(value).strip().lower()
    if normalized in {"mult", "add", ""}:
        return normalized
    raise ValueError(f"Invalid {field_name} '{value}'. Must be one of: mult, add, ''.")


def _normalize_weight_func(value, field_name="weight_func"):
    """Return a normalized weight function token, defaulting missing values to ''."""
    normalized = _parse_optional_weight_func(value, field_name)
    return "" if normalized is None else normalized


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
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name} '{value}'. Must be a number.") from exc


def _collect_raw_overrides(args=None, argv=None, start_index=1):
    start_index = int(start_index)
    if start_index < 0:
        raise ValueError("start_index must be >= 0")

    if args is None:
        argv = sys.argv if argv is None else argv

    raw_overrides = {}
    for field_name, offset, preserve_empty in _OVERRIDE_FIELDS:
        if args is not None:
            raw_value = getattr(args, field_name, None)
        else:
            index = start_index + offset
            raw_value = argv[index] if len(argv) > index else None
        raw_overrides[field_name] = _normalize_optional_cli_value(
            raw_value,
            preserve_empty=preserve_empty,
        )
    return raw_overrides


def parse_config_overrides(args=None, argv=None, start_index=1):
    """Parse optional config overrides for ``load_config``."""
    raw_overrides = _collect_raw_overrides(args=args, argv=argv, start_index=start_index)

    return {
        "level": raw_overrides["level"],
        "version": raw_overrides["version"],
        "buffer": _parse_optional_int(raw_overrides["buffer"], "buffer"),
        "weight_method": raw_overrides["weight_method"],
        "weight_func": _parse_optional_weight_func(raw_overrides["weight_func"], "weight_func"),
        "dynamic_buffering": _parse_optional_bool(raw_overrides["dynamic_buffering"], "dynamic_buffering"),
        "dynamic_buffer_k": _parse_optional_float(raw_overrides["dynamic_buffer_k"], "dynamic_buffer_k"),
    }


def _normalize_cfg_path(path_value, base_dir):
    """Return an absolute filesystem path for a config entry, passing URLs through unchanged."""
    if not isinstance(path_value, str):
        return path_value
    if "://" in path_value:
        return path_value

    expanded = os.path.expanduser(path_value)
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    return os.path.abspath(os.path.join(base_dir, expanded))


def _load_raw_config(config_path: str) -> dict:
    with open(config_path, encoding="utf-8") as stream:
        raw_config = yaml.safe_load(stream)
    if not isinstance(raw_config, dict):
        raise TypeError("config.yaml must contain a top-level mapping of script sections")
    return raw_config


def _require_mapping_section(section_name: str, section_cfg) -> dict:
    if not isinstance(section_cfg, dict):
        raise ConfigResolutionError(
            section_name,
            "<section>",
            f"Config section '{section_name}' must be a mapping.",
        )
    return section_cfg


def _ordered_sections(raw_config: dict) -> list[str]:
    return list(raw_config)


def _nested_value_or_missing(section_cfg, key_path):
    current = section_cfg
    for key in key_path:
        if not isinstance(current, dict) or key not in current:
            return _MISSING
        current = current[key]
    return current


def _merge_first_defined(base: dict, candidate: dict) -> dict:
    merged = deepcopy(base)
    for key, value in candidate.items():
        if value is None:
            continue

        if isinstance(value, dict):
            existing = merged.get(key, _MISSING)
            if existing is _MISSING or existing is None:
                merged[key] = _merge_first_defined({}, value)
                continue
            if isinstance(existing, dict):
                merged[key] = _merge_first_defined(existing, value)
            continue

        if key not in merged or merged[key] is None:
            merged[key] = deepcopy(value)
    return merged


def _build_base_config(script_name: str, raw_config: dict, ordered_sections: list[str]) -> dict:
    base_cfg = {}
    for section_name in ordered_sections:
        if section_name == script_name:
            break

        section_cfg = _require_mapping_section(section_name, raw_config[section_name])
        base_cfg = _merge_first_defined(base_cfg, section_cfg)
    return base_cfg


def _resolve_value_from_base(script_name: str, key_path: tuple[str, ...], base_cfg: dict):
    candidate = _nested_value_or_missing(base_cfg, key_path)
    if candidate is not _MISSING:
        return deepcopy(candidate)

    dotted_key = ".".join(key_path)
    raise ConfigResolutionError(
        script_name,
        dotted_key,
        f"Config key '{dotted_key}' for script '{script_name}' could not be resolved from earlier canonical sections.",
    )


def _resolve_mapping(script_name: str, key_path: tuple[str, ...], mapping: dict, base_cfg: dict) -> dict:
    resolved = {}

    for key, value in mapping.items():
        current_path = (*key_path, key)
        if isinstance(value, dict):
            resolved[key] = _resolve_mapping(script_name, current_path, value, base_cfg)
            continue
        if value is None:
            resolved[key] = _resolve_value_from_base(script_name, current_path, base_cfg)
            continue
        resolved[key] = deepcopy(value)
    return resolved


def resolve_config(script_name: str, raw_config: dict) -> dict:
    """Resolve a script section using earlier YAML sections as inheritance sources."""
    if script_name not in raw_config:
        raise ConfigResolutionError(
            script_name,
            "<section>",
            f"Config section '{script_name}' is missing from config.yaml.",
        )

    section_cfg = _require_mapping_section(script_name, raw_config[script_name])

    ordered_sections = _ordered_sections(raw_config)
    if script_name not in ordered_sections:
        raise ConfigResolutionError(
            script_name,
            "<section>",
            f"Config section '{script_name}' is not part of the config section order.",
        )

    base_cfg = _build_base_config(script_name, raw_config, ordered_sections)
    return _resolve_mapping(script_name, tuple(), section_cfg, base_cfg)


def _apply_optional_override(cfg: dict, key: str, override, parser=None) -> None:
    if override is None and key not in cfg:
        return

    value = cfg[key] if override is None else override
    if parser is not None:
        value = parser(value, key)
    cfg[key] = value


def _apply_runtime_overrides(
    resolved_cfg: dict,
    *,
    level=None,
    version=None,
    buffer=None,
    weight_method=None,
    weight_func=None,
    dynamic_buffering=None,
    dynamic_buffer_k=None,
) -> dict:
    cfg = deepcopy(resolved_cfg)
    _apply_optional_override(cfg, "level", level)
    _apply_optional_override(cfg, "version", version)
    _apply_optional_override(cfg, "buffer", buffer)
    _apply_optional_override(cfg, "weight_method", weight_method)
    _apply_optional_override(
        cfg,
        "weight_func",
        weight_func,
        _normalize_weight_func,
    )
    _apply_optional_override(cfg, "dynamic_buffering", dynamic_buffering, _parse_optional_bool)
    _apply_optional_override(cfg, "dynamic_buffer_k", dynamic_buffer_k, _parse_optional_float)
    return cfg


def _derive_runtime_values(cfg: dict, default_distance_additive, default_distance_multiplicative) -> dict:
    derived = {}
    weight_type_map = {
        "linear": "li",
        "square_root": "sq",
        "logarithmic": "log",
        "sigmoid": "sig",
    }
    if "weight_method" in cfg:
        if cfg["weight_method"] not in weight_type_map:
            valid_methods = ", ".join(weight_type_map)
            raise ValueError(
                f"Invalid weight_method '{cfg['weight_method']}'. Must be one of: {valid_methods}."
            )
        derived["weight_type"] = weight_type_map[cfg["weight_method"]]

    if "weight_func" in cfg:
        weight_func = _normalize_weight_func(cfg["weight_func"], "weight_func")
        derived["weight_func_suffix"] = {
            "mult": "_mult",
            "add": "_add",
            "": "",
        }[weight_func]
        derived["distance_fn"] = (
            default_distance_multiplicative
            if weight_func in {"", "mult"}
            else default_distance_additive
        )

    if "buffer" in cfg and "dynamic_buffering" in cfg:
        if cfg["dynamic_buffering"] and cfg.get("dynamic_buffer_k") is None:
            raise ValueError("dynamic_buffer_k must be configured when dynamic_buffering is true")

        if cfg["dynamic_buffering"]:
            derived["buffer_path_token"] = f"k{str(cfg['dynamic_buffer_k']).replace('.', '_')}"
        else:
            derived["buffer_path_token"] = str(int(cfg["buffer"]))

    calculate_buffer_keys = {
        "buffer",
        "dynamic_buffering",
        "min_buffer",
        "max_buffer",
        "k_min",
        "k_max",
        "detection_confidence_threshold",
        "dynamic_buffer_k",
    }
    if calculate_buffer_keys.issubset(cfg):
        derived["calculate_buffer_kwargs"] = {
            "buffer": cfg["buffer"],
            "dynamic_buffering": cfg["dynamic_buffering"],
            "min_buffer": cfg["min_buffer"],
            "max_buffer": cfg["max_buffer"],
            "k_min": cfg["k_min"],
            "k_max": cfg["k_max"],
            "conf_threshold": cfg["detection_confidence_threshold"],
            "k_value": cfg["dynamic_buffer_k"],
        }

    return derived


def _format_field_names(template: str) -> list[str]:
    field_names = []
    for _, field_name, _, _ in _FORMATTER.parse(template):
        if field_name is None:
            continue
        normalized = field_name.split(".", 1)[0].split("[", 1)[0]
        if normalized:
            field_names.append(normalized)
    return field_names


def _expand_paths(script_name: str, paths_cfg: dict, config_dir: str, cfg: dict, derived: dict) -> dict:
    raw_data_dir = paths_cfg.get("data_dir")
    context = {}
    if raw_data_dir is not None:
        context["data_dir"] = raw_data_dir

    for key in ("level", "version", "industrial_min_cells"):
        if key in cfg:
            value = cfg[key]
            context[key] = str(value) if key == "industrial_min_cells" else value

    if "buffer_path_token" in derived:
        context["buffer"] = derived["buffer_path_token"]
    if "weight_type" in derived:
        context["weight_type"] = derived["weight_type"]
    if "weight_func_suffix" in derived:
        context["weight_func"] = derived["weight_func_suffix"]

    expanded_paths = {}
    for key, value in paths_cfg.items():
        formatted = value
        if isinstance(value, str):
            missing_fields = [field for field in _format_field_names(value) if field not in context]
            if missing_fields:
                missing_list = ", ".join(sorted(set(missing_fields)))
                raise ConfigResolutionError(
                    script_name,
                    f"paths.{key}",
                    f"Path 'paths.{key}' for script '{script_name}' requires unresolved config field(s): {missing_list}.",
                )
            formatted = value.format(**context)
        expanded_paths[key] = _normalize_cfg_path(formatted, config_dir)
    return expanded_paths


def get_runtime_params(cfg: dict) -> dict:
    """Return validated impact-model runtime parameters from the loaded config."""
    section = cfg["impact_polygons_pop_params"]
    if not isinstance(section, dict):
        raise TypeError("cfg['impact_polygons_pop_params'] must be a dict")

    return {
        "org_per_pop": float(section["org_per_pop"]),
        "width": float(section["width"]),
        "c_limit": float(section["c_limit"]),
        "base_k": float(section["base_k"]),
        "theta": float(section["theta"]),
        "step_m": float(section["step_m"]),
        "least_discharge_cms": float(section["least_discharge_cms"]),
        "impact_radii": [float(value) for value in section["impact_radii"]],
    }


def load_config(
    script_name,
    config="config.yaml",
    level=None,
    version=None,
    buffer=None,
    weight_method=None,
    weight_func=None,
    dynamic_buffering=None,
    dynamic_buffer_k=None,
):
    """Load and resolve the per-script configuration section for a workflow."""
    try:
        from .create_voronoi import default_distance_additive, default_distance_multiplicative
    except ImportError:
        from create_voronoi import default_distance_additive, default_distance_multiplicative

    config_path = os.path.abspath(config)
    config_dir = os.path.dirname(config_path)
    raw_config = _load_raw_config(config_path)

    script_cfg = resolve_config(script_name, raw_config)
    script_cfg = _apply_runtime_overrides(
        script_cfg,
        level=level,
        version=version,
        buffer=buffer,
        weight_method=weight_method,
        weight_func=weight_func,
        dynamic_buffering=dynamic_buffering,
        dynamic_buffer_k=dynamic_buffer_k,
    )

    derived = _derive_runtime_values(script_cfg, default_distance_additive, default_distance_multiplicative)
    if "paths" in script_cfg:
        script_cfg["paths"] = _expand_paths(script_name, script_cfg["paths"], config_dir, script_cfg, derived)
    script_cfg.update(derived)
    return script_cfg