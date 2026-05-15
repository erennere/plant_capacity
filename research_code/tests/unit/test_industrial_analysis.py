from __future__ import annotations

import sys

import geopandas as gpd
import pytest
from shapely.geometry import Point, box

from research_code.industrial_analysis import find_unconnected_industrial_areas


pytestmark = pytest.mark.unit


def test_filter_industrial_wwtps_keeps_matching_categories_and_mix(mock_cfg, tiny_points_gdf):
    cfg = mock_cfg
    cfg["industrial_category_numbers"] = ["1", "4"]

    result = find_unconnected_industrial_areas.filter_industrial_wwtps(cfg, tiny_points_gdf)

    assert set(result["WASTE_ID"]) == {1, 2, 5, 6}


def test_find_unconnected_areas_returns_only_polygons_outside_service_regions():
    industrial_gdf = gpd.GeoDataFrame(
        {
            "industrial_id": [1, 2],
            "geometry": [box(0.0, 0.0, 0.05, 0.05), box(2.0, 2.0, 2.1, 2.1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    voronoi_gdf = gpd.GeoDataFrame(
        {"geometry": [box(-0.1, -0.1, 0.1, 0.1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = find_unconnected_industrial_areas.find_unconnected_areas(industrial_gdf, voronoi_gdf)

    assert result["industrial_id"].tolist() == [2]


def test_load_industrial_areas_returns_none_when_file_missing(mock_cfg, monkeypatch):
    cfg = mock_cfg
    monkeypatch.setattr(find_unconnected_industrial_areas.os.path, "exists", lambda path: False)

    result = find_unconnected_industrial_areas.load_industrial_areas(cfg)

    assert result is None


def test_load_industrial_areas_reads_geopackage_when_present(mock_cfg, monkeypatch):
    cfg = mock_cfg
    expected = gpd.GeoDataFrame(
        {"industrial_id": [1], "geometry": [box(0.0, 0.0, 1.0, 1.0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    captured = {}

    monkeypatch.setattr(find_unconnected_industrial_areas.os.path, "exists", lambda path: True)

    def fake_read_file(path, driver=None):
        captured["path"] = path
        captured["driver"] = driver
        return expected

    monkeypatch.setattr(find_unconnected_industrial_areas.gpd, "read_file", fake_read_file)

    result = find_unconnected_industrial_areas.load_industrial_areas(cfg)

    assert result.equals(expected)
    assert captured == {
        "path": cfg["paths"]["industrial_merged_filepath"],
        "driver": "GPKG",
    }


def test_load_wwtps_adds_basin_info_when_missing_for_approach_one(mock_cfg, monkeypatch):
    cfg = mock_cfg
    basin_col = cfg["basin_column_name"]
    read_calls = []

    wwtps_gdf = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1, 2],
            basin_col: [None, None],
            "geometry": [Point(0.0, 0.0), Point(1.0, 1.0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    basin_gdf = gpd.GeoDataFrame(
        {
            basin_col: [101, 202],
            "geometry": [box(-1.0, -1.0, 0.5, 0.5), box(0.5, 0.5, 2.0, 2.0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    def fake_read_file(path, driver=None):
        read_calls.append((path, driver))
        if path == cfg["paths"]["corrected_all_filepath"]:
            return wwtps_gdf.copy()
        if path == cfg["paths"]["watershed"]:
            return basin_gdf.copy()
        raise AssertionError(f"Unexpected path: {path}")

    def fake_intersect(run_gdf, basin_slice, column_name, concurrency):
        assert column_name == basin_col
        assert basin_slice.columns.tolist() == [basin_col, "geometry"]
        assert concurrency == cfg["sindex_concurrency"]
        return run_gdf.assign(**{basin_col: [101, 202]})

    monkeypatch.setattr(find_unconnected_industrial_areas.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(find_unconnected_industrial_areas, "intersect_with_polygon_sindex", fake_intersect)

    result = find_unconnected_industrial_areas.load_wwtps(cfg, approach_id="1")

    assert result[basin_col].tolist() == [101, 202]
    assert read_calls == [
        (cfg["paths"]["corrected_all_filepath"], "GPKG"),
        (cfg["paths"]["watershed"], "GPKG"),
    ]


def test_load_wwtps_raises_when_basin_dataset_lacks_configured_column(mock_cfg, monkeypatch):
    cfg = mock_cfg
    basin_col = cfg["basin_column_name"]

    wwtps_gdf = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            basin_col: [None],
            "geometry": [Point(0.0, 0.0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    basin_gdf = gpd.GeoDataFrame(
        {"wrong_column": [1], "geometry": [box(-1.0, -1.0, 1.0, 1.0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    def fake_read_file(path, driver=None):
        if path == cfg["paths"]["corrected_all_filepath"]:
            return wwtps_gdf.copy()
        if path == cfg["paths"]["watershed"]:
            return basin_gdf.copy()
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(find_unconnected_industrial_areas.gpd, "read_file", fake_read_file)

    with pytest.raises(KeyError, match="Configured basin column"):
        find_unconnected_industrial_areas.load_wwtps(cfg, approach_id="1")


def test_run_voronoi_for_wwtps_approach_one_builds_basin_buffer_ids(
    mock_cfg,
    tiny_points_gdf,
    tiny_watershed_gdf,
    tiny_country_gdf,
    monkeypatch,
):
    cfg = mock_cfg
    cfg["weight_func"] = "mult"
    cfg["weight_method"] = "logarithmic"
    cfg["distance_fn"] = "distance-sentinel"
    captured = {}

    def fake_run_voronoi_approach(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        region_df = gpd.GeoDataFrame(
            {"geometry": [box(-0.08, -0.08, 0.08, 0.08)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        return region_df, None

    monkeypatch.setattr(find_unconnected_industrial_areas, "run_voronoi_approach", fake_run_voronoi_approach)

    result = find_unconnected_industrial_areas.run_voronoi_for_wwtps(
        cfg,
        "1",
        tiny_points_gdf.iloc[:3].copy(),
        tiny_watershed_gdf.copy(),
        tiny_country_gdf.copy(),
        {"buffers": {"WWTP": "unused"}},
        output_path="synthetic.gpkg",
        only_round=True,
    )

    run_gdf = captured["args"][1]
    clipping_gdf = captured["args"][2]

    assert result is not None
    assert run_gdf["buffer_id"].tolist() == run_gdf["HYBAS_ID"].tolist()
    assert clipping_gdf["buffer_id"].tolist() == clipping_gdf["HYBAS_ID"].tolist()
    assert captured["kwargs"]["scale_weights"] is True
    assert captured["kwargs"]["buffering"] is True
    assert captured["kwargs"]["only_round"] is True
    assert captured["kwargs"]["method"] == "logarithmic"


def test_run_voronoi_for_wwtps_approach_zero_uses_dissolved_buffers(
    mock_cfg,
    tiny_points_gdf,
    tiny_country_gdf,
    monkeypatch,
):
    cfg = mock_cfg
    cfg["weight_func"] = ""
    cfg["weight_method"] = "linear"
    cfg["distance_fn"] = "distance-sentinel"
    captured = {}

    dissolved_buffers = gpd.GeoDataFrame(
        {
            cfg["country_output_column"]: ["DE", "FR"],
            "geometry": [box(-0.1, -0.1, 0.1, 0.1), box(0.9, -0.1, 1.1, 0.1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(
        find_unconnected_industrial_areas,
        "orchestrate_overlaps",
        lambda gdf, max_workers, output_path, buffer, country_col: dissolved_buffers.copy(),
    )
    monkeypatch.setattr(find_unconnected_industrial_areas, "drop_duplicates", lambda gdf, col: gdf)
    monkeypatch.setattr(
        find_unconnected_industrial_areas,
        "intersect_with_polygon_sindex",
        lambda run_gdf, buffers, col, concurrency: run_gdf.assign(buffer_id=[0] * len(run_gdf)),
    )

    def fake_run_voronoi_approach(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        region_df = gpd.GeoDataFrame(
            {"geometry": [box(-0.1, -0.1, 0.1, 0.1)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        return region_df, None

    monkeypatch.setattr(find_unconnected_industrial_areas, "run_voronoi_approach", fake_run_voronoi_approach)

    result = find_unconnected_industrial_areas.run_voronoi_for_wwtps(
        cfg,
        "0",
        tiny_points_gdf.iloc[:2].copy(),
        gpd.GeoDataFrame({"HYBAS_ID": [101], "geometry": [box(-1, -1, 1, 1)]}, geometry="geometry", crs="EPSG:4326"),
        tiny_country_gdf.copy(),
        {"buffers": {"WWTP": "buffers.gpkg"}},
        output_path="synthetic.gpkg",
        only_round=False,
    )

    run_gdf = captured["args"][1]
    clipping_gdf = captured["args"][2]

    assert result is not None
    assert run_gdf["buffer_id"].tolist() == [0, 0]
    assert clipping_gdf["buffer_id"].tolist() == [0, 1]
    assert captured["kwargs"]["scale_weights"] is False
    assert captured["kwargs"]["buffering"] is False
    assert captured["kwargs"]["method"] == "linear"


def test_run_voronoi_for_wwtps_rejects_invalid_approach(mock_cfg, tiny_points_gdf, tiny_watershed_gdf, tiny_country_gdf):
    cfg = mock_cfg

    with pytest.raises(ValueError, match="Unsupported approach"):
        find_unconnected_industrial_areas.run_voronoi_for_wwtps(
            cfg,
            "2",
            tiny_points_gdf.copy(),
            tiny_watershed_gdf.copy(),
            tiny_country_gdf.copy(),
            {"buffers": {"WWTP": "unused"}},
            output_path="synthetic.gpkg",
            only_round=False,
        )


def test_main_skips_when_output_exists_and_overwrite_disabled(mock_cfg, monkeypatch, tmp_path):
    cfg = mock_cfg
    cfg["paths"]["industrial_unconnected_output"] = str(tmp_path / "industrial.parquet")
    cfg["industrial_unconnected_overwrite"] = False

    monkeypatch.setattr(sys, "argv", ["find_unconnected_industrial_areas.py"])
    monkeypatch.setattr(find_unconnected_industrial_areas, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_unconnected_industrial_areas, "create_output_paths", lambda config: {"voronoi": {"1": "unused.gpkg"}})
    monkeypatch.setattr(
        find_unconnected_industrial_areas.os.path,
        "exists",
        lambda path: path == cfg["paths"]["industrial_unconnected_output"],
    )
    monkeypatch.setattr(
        find_unconnected_industrial_areas,
        "load_industrial_areas",
        lambda config: (_ for _ in ()).throw(AssertionError("load_industrial_areas should not be called")),
    )

    assert find_unconnected_industrial_areas.main() is True


def test_main_writes_industrial_copy_when_no_industrial_wwtps(mock_cfg, monkeypatch, tmp_path, tiny_points_gdf):
    cfg = mock_cfg
    cfg["paths"]["industrial_unconnected_output"] = str(tmp_path / "industrial.parquet")
    cfg["industrial_unconnected_overwrite"] = True

    industrial_gdf = gpd.GeoDataFrame(
        {
            "industrial_id": [1, 2],
            "geometry": [box(0.0, 0.0, 0.1, 0.1), box(1.0, 1.0, 1.1, 1.1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    saved = {}

    monkeypatch.setattr(sys, "argv", ["find_unconnected_industrial_areas.py"])
    monkeypatch.setattr(find_unconnected_industrial_areas, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_unconnected_industrial_areas, "create_output_paths", lambda config: {"voronoi": {"1": "unused.gpkg"}})
    monkeypatch.setattr(find_unconnected_industrial_areas.os.path, "exists", lambda path: False)
    monkeypatch.setattr(find_unconnected_industrial_areas.os, "makedirs", lambda path, exist_ok=False: None)
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_industrial_areas", lambda config: industrial_gdf.copy())
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_wwtps", lambda config, approach_id: tiny_points_gdf.copy())
    monkeypatch.setattr(
        find_unconnected_industrial_areas,
        "filter_industrial_wwtps",
        lambda config, wwtps_gdf: wwtps_gdf.iloc[0:0].copy(),
    )

    def fake_to_parquet(self, path, index=False, **kwargs):
        saved["path"] = path
        saved["index"] = index
        saved["industrial_ids"] = self["industrial_id"].tolist()

    monkeypatch.setattr(gpd.GeoDataFrame, "to_parquet", fake_to_parquet)

    assert find_unconnected_industrial_areas.main() is True
    assert saved == {
        "path": cfg["paths"]["industrial_unconnected_output"],
        "index": False,
        "industrial_ids": [1, 2],
    }


def test_main_returns_false_when_voronoi_generation_fails(
    mock_cfg,
    monkeypatch,
    tmp_path,
    tiny_points_gdf,
    tiny_watershed_gdf,
    tiny_country_gdf,
):
    cfg = mock_cfg
    cfg["paths"]["industrial_unconnected_output"] = str(tmp_path / "industrial.parquet")
    cfg["industrial_unconnected_overwrite"] = True
    cfg["prepare_data_fn"] = "prepare_data"

    industrial_gdf = gpd.GeoDataFrame(
        {"industrial_id": [1], "geometry": [box(0.0, 0.0, 0.1, 0.1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(sys, "argv", ["find_unconnected_industrial_areas.py"])
    monkeypatch.setattr(find_unconnected_industrial_areas, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_unconnected_industrial_areas, "create_output_paths", lambda config: {"voronoi": {"1": "voronoi.gpkg"}})
    monkeypatch.setattr(find_unconnected_industrial_areas.os.path, "exists", lambda path: False)
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_industrial_areas", lambda config: industrial_gdf.copy())
    monkeypatch.setattr(find_unconnected_industrial_areas, "load_wwtps", lambda config, approach_id: tiny_points_gdf.copy())
    monkeypatch.setattr(find_unconnected_industrial_areas, "filter_industrial_wwtps", lambda config, wwtps_gdf: wwtps_gdf.copy())
    monkeypatch.setattr(
        find_unconnected_industrial_areas,
        "_resolve_configured_callable",
        lambda *args, **kwargs: (lambda config: {"country_df": tiny_country_gdf.copy(), "basin_gdf": tiny_watershed_gdf.copy()}),
    )
    monkeypatch.setattr(find_unconnected_industrial_areas, "run_voronoi_for_wwtps", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gpd.GeoDataFrame,
        "to_parquet",
        lambda self, path, index=False, **kwargs: (_ for _ in ()).throw(AssertionError("to_parquet should not be called")),
    )

    assert find_unconnected_industrial_areas.main() is False