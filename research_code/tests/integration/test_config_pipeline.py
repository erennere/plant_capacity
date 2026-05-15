from pathlib import Path

import pytest

from research_code import pipelines, starter


pytestmark = pytest.mark.integration


def test_load_config_and_output_path_builders_stay_aligned(write_test_config):
    config_path = write_test_config(
        {
            "arguments": {"default_level": "4", "default_version": "3"},
            "params": {
                "buffer": 1800,
                "weight_method": "linear",
                "weight_func": "mult",
                "dynamic_buffering": False,
            },
        }
    )

    cfg = starter.load_config(config=str(config_path))
    output_paths = pipelines.create_output_paths(cfg)
    pop_output_paths = pipelines.create_pop_output_paths(cfg)

    assert Path(output_paths["voronoi"]["0"]).parent == Path(cfg["paths"]["voronoi_dir"])
    assert Path(output_paths["voronoi"]["0"]).name == "appr_0_v3_lvl4_bf1800_li_mult.gpkg"
    assert Path(pop_output_paths["voronoi"]["0"]).parent == Path(cfg["paths"]["pop_output_dir"])
    assert Path(pop_output_paths["voronoi"]["0"]).name == "pop_added_appr_0_v3_lvl4_bf1800_li_mult.gpkg"