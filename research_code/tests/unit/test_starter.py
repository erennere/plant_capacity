from argparse import Namespace
from pathlib import Path

import pytest

from research_code import create_voronoi, starter


pytestmark = pytest.mark.unit


def test_normalize_optional_cli_value_preserves_explicit_empty_string():
    assert starter._normalize_optional_cli_value("   ", preserve_empty=True) == ""


def test_parse_config_overrides_from_argv_parses_all_supported_types():
    overrides = starter.parse_config_overrides(
        argv=["prog", "9", "3", "12000", "square_root", "", "yes", "0.75"]
    )

    assert overrides == {
        "level": "9",
        "version": "3",
        "buffer": 12000,
        "weight_method": "square_root",
        "weight_func": "",
        "dynamic_buffering": True,
        "dynamic_buffer_k": 0.75,
    }


def test_parse_config_overrides_from_namespace_normalizes_optional_values():
    args = Namespace(
        level=" 7 ",
        version=" null ",
        buffer=None,
        weight_method="logarithmic",
        weight_func="add",
        dynamic_buffering="false",
        dynamic_buffer_k="1.25",
    )

    overrides = starter.parse_config_overrides(args=args)

    assert overrides == {
        "level": "7",
        "version": None,
        "buffer": None,
        "weight_method": "logarithmic",
        "weight_func": "add",
        "dynamic_buffering": False,
        "dynamic_buffer_k": 1.25,
    }


def test_parse_config_overrides_honors_custom_start_index():
    overrides = starter.parse_config_overrides(
        argv=["prog", "required_arg", "8", "4", "1500", "linear", "mult", "0", "1.5"],
        start_index=2,
    )

    assert overrides == {
        "level": "8",
        "version": "4",
        "buffer": 1500,
        "weight_method": "linear",
        "weight_func": "mult",
        "dynamic_buffering": False,
        "dynamic_buffer_k": 1.5,
    }


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["prog", "7", "2", "not-an-int"], "Invalid buffer"),
        (["prog", "7", "2", "1200", "linear", "bad-mode"], "Invalid weight_func"),
        (["prog", "7", "2", "1200", "linear", "mult", "maybe"], "Invalid dynamic_buffering"),
        (["prog", "7", "2", "1200", "linear", "mult", "true", "nanmeters"], "Invalid dynamic_buffer_k"),
    ],
)
def test_parse_config_overrides_rejects_invalid_optional_values(argv, message):
    with pytest.raises(ValueError, match=message):
        starter.parse_config_overrides(argv=argv)


def test_normalize_cfg_path_handles_relative_absolute_and_url_inputs(tmp_path):
    base_dir = tmp_path / "config_root"
    base_dir.mkdir()

    relative_result = starter._normalize_cfg_path("data/example.csv", str(base_dir))
    absolute_input = str((tmp_path / "absolute.csv").resolve())

    assert relative_result == str((base_dir / "data" / "example.csv").resolve())
    assert starter._normalize_cfg_path(absolute_input, str(base_dir)) == absolute_input
    assert starter._normalize_cfg_path("s3://bucket/key/file.parquet", str(base_dir)) == "s3://bucket/key/file.parquet"
    assert starter._normalize_cfg_path(42, str(base_dir)) == 42


def test_load_config_expands_relative_paths_and_aliases(write_test_config):
    config_path = write_test_config(
        {
            "arguments": {"default_level": "5", "default_version": "9"},
            "params": {
                "buffer": 1500,
                "weight_method": "square_root",
                "weight_func": "add",
                "dynamic_buffering": False,
            },
        }
    )

    cfg = starter.load_config(config=str(config_path))

    assert cfg["level"] == "5"
    assert cfg["version"] == "9"
    assert cfg["weight_type"] == "sq"
    assert cfg["weight_func"] == "add"
    assert cfg["weight_func_suffix"] == "_add"
    assert cfg["buffer_path_token"] == "1500"
    assert cfg["distance_fn"] is create_voronoi.default_distance_additive
    assert cfg["paths"]["bboxes"] == str((config_path.parent / "data" / "bboxes.csv").resolve())
    assert "bf1500" in cfg["paths"]["voronoi_dir"]


def test_load_config_dynamic_buffering_uses_k_token_and_empty_weight_func(write_test_config):
    config_path = write_test_config(
        {
            "params": {
                "buffer": 2200,
                "weight_method": "logarithmic",
                "weight_func": "mult",
                "dynamic_buffering": True,
                "dynamic_buffer_k": 0.5,
            },
        }
    )

    cfg = starter.load_config(
        config=str(config_path),
        weight_func="",
        dynamic_buffering=True,
        dynamic_buffer_k=0.75,
    )

    assert cfg["weight_func"] == ""
    assert cfg["weight_func_suffix"] == ""
    assert cfg["buffer_path_token"] == "k0_75"
    assert cfg["dynamic_buffering"] is True
    assert cfg["dynamic_buffer_k"] == pytest.approx(0.75)
    assert cfg["distance_fn"] is create_voronoi.default_distance_multiplicative


def test_load_config_rejects_invalid_weight_method(write_test_config):
    config_path = write_test_config(
        {
            "params": {
                "weight_method": "banana",
            },
        }
    )

    with pytest.raises(ValueError, match="Invalid weight_method"):
        starter.load_config(config=str(config_path))