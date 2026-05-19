from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely import from_wkt, to_wkt
from shapely.geometry import box

from src.pop_at_risk_river_calculations import find_diff_pop, find_unserved_pop


pytestmark = pytest.mark.unit


class _DuckResult:
    def __init__(self, dataframe):
        self._dataframe = dataframe

    def df(self):
        return self._dataframe


class _FakeDifferenceConnection:
    def __init__(self):
        self.tables = {}

    def execute(self, sql):
        self.sql = sql
        return self

    def register(self, name, dataframe):
        self.tables[name] = dataframe.copy()

    def df(self):
        watershed_df = self.tables["watershed_gdf"]
        pop_df = self.tables["pop_gdf"]
        records = []
        for _, watershed_row in watershed_df.iterrows():
            matches = pop_df[pop_df["HYBAS_ID"] == watershed_row["HYBAS_ID"]]
            if matches.empty:
                continue
            diff_geom = from_wkt(watershed_row["geometry"]).difference(from_wkt(matches.iloc[0]["geometry"]))
            records.append({"HYBAS_ID": watershed_row["HYBAS_ID"], "geometry": to_wkt(diff_geom)})
        return pd.DataFrame(records)

    def close(self):
        return None


class _ImmediateFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _ImmediateExecutor:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args, **kwargs):
        return _ImmediateFuture(fn(*args, **kwargs))


def test_find_difference_returns_watershed_minus_served_geometry(monkeypatch, tiny_watershed_gdf):
    served = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "geometry": [box(-0.08, -0.02, 0.00, 0.02)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(find_diff_pop.duckdb, "connect", lambda path: _FakeDifferenceConnection())
    monkeypatch.setattr(find_diff_pop.os.path, "exists", lambda path: False)

    result = find_diff_pop.find_difference(tiny_watershed_gdf.iloc[[0]].copy(), served, basin_col="HYBAS_ID")

    assert result is not None
    assert len(result) == 1
    assert result.iloc[0]["geometry"].intersection(served.iloc[0].geometry).area == pytest.approx(0.0)


def test_find_difference_returns_none_when_duckdb_connect_fails(monkeypatch, tiny_watershed_gdf):
    served = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "geometry": [box(-0.08, -0.02, 0.00, 0.02)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(
        find_diff_pop.duckdb,
        "connect",
        lambda path: (_ for _ in ()).throw(RuntimeError("duckdb unavailable")),
    )
    monkeypatch.setattr(find_diff_pop.os.path, "exists", lambda path: False)

    result = find_diff_pop.find_difference(tiny_watershed_gdf.iloc[[0]].copy(), served, basin_col="HYBAS_ID")

    assert result is None


def test_create_unserved_pop_writes_filtered_geometries(monkeypatch, tmp_path):
    input_path = tmp_path / "non_served.csv"
    output_path = tmp_path / "filtered.gpkg"
    captured = {}

    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: path == str(input_path))
    monkeypatch.setattr(
        find_unserved_pop.duckdb,
        "sql",
        lambda query: _DuckResult(
            pd.DataFrame(
                {
                    "pop_sum": ["12"],
                    "geometry": [box(0, 0, 1, 1).wkt],
                }
            )
        ),
    )
    monkeypatch.setattr(
        find_unserved_pop,
        "ensure_output_dir_for_file",
        lambda path: captured.setdefault("ensured", path),
    )

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["path"] = path
        captured["driver"] = driver
        captured["index"] = index
        captured["rows"] = len(self)

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        rows_written = find_unserved_pop.create_unserved_pop(str(input_path), 5, str(output_path))
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert rows_written == 1
    assert captured["ensured"] == str(output_path)
    assert captured["path"] == str(output_path)
    assert captured["driver"] == "GPKG"
    assert captured["index"] is False
    assert captured["rows"] == 1


def test_create_unserved_pop_raises_when_input_file_is_missing(monkeypatch, tmp_path):
    input_path = tmp_path / "missing.csv"
    output_path = tmp_path / "filtered.gpkg"

    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: False)

    with pytest.raises(FileNotFoundError, match="Input CSV not found"):
        find_unserved_pop.create_unserved_pop(str(input_path), 5, str(output_path))


def test_create_unserved_pop_writes_empty_output_when_query_returns_no_rows(monkeypatch, tmp_path):
    input_path = tmp_path / "non_served.csv"
    output_path = tmp_path / "filtered.gpkg"
    captured = {}

    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: path == str(input_path))
    monkeypatch.setattr(
        find_unserved_pop.duckdb,
        "sql",
        lambda query: _DuckResult(pd.DataFrame(columns=["pop_sum", "geometry"])),
    )
    monkeypatch.setattr(find_unserved_pop, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", path))

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["path"] = path
        captured["rows"] = len(self)
        captured["driver"] = driver
        captured["index"] = index

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        rows_written = find_unserved_pop.create_unserved_pop(str(input_path), 5, str(output_path))
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert rows_written == 0
    assert captured["ensured"] == str(output_path)
    assert captured["path"] == str(output_path)
    assert captured["rows"] == 0
    assert captured["driver"] == "GPKG"
    assert captured["index"] is False


def test_create_unserved_pop_raises_when_geometry_column_is_missing(monkeypatch, tmp_path):
    input_path = tmp_path / "non_served.csv"
    output_path = tmp_path / "filtered.gpkg"

    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: path == str(input_path))
    monkeypatch.setattr(
        find_unserved_pop.duckdb,
        "sql",
        lambda query: _DuckResult(pd.DataFrame({"pop_sum": ["12"]})),
    )

    with pytest.raises(ValueError, match="missing required 'geometry' column"):
        find_unserved_pop.create_unserved_pop(str(input_path), 5, str(output_path))


def test_create_unserved_pop_rejects_negative_threshold(monkeypatch, tmp_path):
    input_path = tmp_path / "non_served.csv"
    output_path = tmp_path / "filtered.gpkg"

    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: path == str(input_path))

    with pytest.raises(ValueError, match="threshold"):
        find_unserved_pop.create_unserved_pop(str(input_path), -1, str(output_path))


def test_find_unserved_pop_main_raises_when_pop_threshold_is_missing(mock_cfg, monkeypatch):
    cfg = mock_cfg
    cfg["figures"] = {}

    monkeypatch.setattr(find_unserved_pop.os, "chdir", lambda path: None)
    monkeypatch.setattr(find_unserved_pop, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(find_unserved_pop, "load_config", lambda **overrides: cfg)

    with pytest.raises(KeyError, match="pop_threshold"):
        find_unserved_pop.main()


def test_find_unserved_pop_main_passes_configured_paths_and_threshold(mock_cfg, monkeypatch):
    cfg = mock_cfg
    cfg["figures"] = {"pop_threshold": 37}
    cfg["paths"]["non_served_outpath"] = "./outputs/non_served.gpkg"
    cfg["paths"]["non_served_above_threshold_outpath"] = "./outputs/non_served_above_threshold.gpkg"
    captured = {}

    monkeypatch.setattr(find_unserved_pop.os, "chdir", lambda path: None)
    monkeypatch.setattr(find_unserved_pop, "parse_config_overrides", lambda start_index=1: {"config": "unused.yaml"})
    monkeypatch.setattr(find_unserved_pop, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(
        find_unserved_pop,
        "create_unserved_pop",
        lambda filepath, threshold, output_filepath: captured.update(
            {
                "filepath": filepath,
                "threshold": threshold,
                "output_filepath": output_filepath,
            }
        ) or 4,
    )

    find_unserved_pop.main()

    assert captured == {
        "filepath": os.path.abspath("./outputs/non_served.csv"),
        "threshold": 37,
        "output_filepath": os.path.abspath("./outputs/non_served_above_threshold.gpkg"),
    }


def test_find_differences_serial_mode_combines_multiple_epsg_results(monkeypatch, tiny_watershed_gdf):
    pop_gdf = gpd.GeoDataFrame(
        {
            "HYBAS_ID": [101, 202],
            "geometry": [box(-0.08, -0.08, -0.02, -0.02), box(0.98, -0.02, 1.04, 0.02)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    calls = []

    monkeypatch.setattr(
        find_diff_pop,
        "estimate_utm_epsg",
        lambda lon, lat: 32631 if lon < 0.5 else 32632,
    )

    def fake_process_epsg_group(epsg, watershed_gdf, pop_local, basin_col="HYBAS_ID"):
        calls.append((epsg, sorted(pop_local[pop_local["epsg"] == epsg]["HYBAS_ID"].tolist()), basin_col))
        return gpd.GeoDataFrame(
            {"HYBAS_ID": [epsg], "geometry": [box(0.0, 0.0, 1.0, 1.0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )

    monkeypatch.setattr(find_diff_pop, "process_epsg_group", fake_process_epsg_group)

    result = find_diff_pop.find_differences(
        tiny_watershed_gdf,
        pop_gdf,
        is_parallel=False,
        basin_col="HYBAS_ID",
    )

    assert sorted(result["HYBAS_ID"].tolist()) == [32631, 32632]
    assert calls == [
        (32631, [101], "HYBAS_ID"),
        (32632, [202], "HYBAS_ID"),
    ]


def test_find_differences_returns_empty_geodataframe_when_all_groups_skip(monkeypatch, tiny_watershed_gdf):
    pop_gdf = gpd.GeoDataFrame(
        {
            "HYBAS_ID": [101],
            "geometry": [box(-0.08, -0.08, -0.02, -0.02)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(find_diff_pop, "estimate_utm_epsg", lambda lon, lat: 32631)
    monkeypatch.setattr(find_diff_pop, "process_epsg_group", lambda *args, **kwargs: None)

    result = find_diff_pop.find_differences(
        tiny_watershed_gdf,
        pop_gdf,
        is_parallel=False,
        basin_col="HYBAS_ID",
    )

    assert result.empty
    assert result.crs.to_epsg() == 4326
    assert result.columns.tolist() == tiny_watershed_gdf.columns.tolist()


def test_find_differences_parallel_mode_collects_executor_results(monkeypatch, tiny_watershed_gdf):
    rng = np.random.default_rng(41)
    pop_gdf = gpd.GeoDataFrame(
        {
            "HYBAS_ID": [101, 202],
            "geometry": [
                box(*(rng.random(4) * np.array([0.01, 0.01, 0.02, 0.02]))),
                box(*(np.array([1.0, 0.0, 1.05, 0.05]) + rng.random(4) * 0.01)),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    captured = []

    monkeypatch.setattr(find_diff_pop, "ProcessPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(find_diff_pop, "as_completed", lambda futures: futures)
    monkeypatch.setattr(find_diff_pop, "estimate_utm_epsg", lambda lon, lat: 32631 if lon < 0.5 else 32632)

    def fake_process_epsg_group(epsg, watershed_gdf, pop_local, basin_col="HYBAS_ID"):
        captured.append((epsg, basin_col, sorted(pop_local[pop_local["epsg"] == epsg]["HYBAS_ID"].tolist())))
        return gpd.GeoDataFrame(
            {"HYBAS_ID": [epsg], "geometry": [box(0.0, 0.0, 1.0, 1.0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )

    monkeypatch.setattr(find_diff_pop, "process_epsg_group", fake_process_epsg_group)

    result = find_diff_pop.find_differences(
        tiny_watershed_gdf,
        pop_gdf,
        max_workers=2,
        is_parallel=True,
        basin_col="HYBAS_ID",
    )

    assert sorted(result["HYBAS_ID"].tolist()) == [32631, 32632]
    assert captured == [
        (32631, "HYBAS_ID", [101]),
        (32632, "HYBAS_ID", [202]),
    ]


def test_create_unserved_pop_rejects_non_integer_like_threshold(monkeypatch, tmp_path):
    input_path = tmp_path / "non_served.csv"
    output_path = tmp_path / "filtered.gpkg"

    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: path == str(input_path))

    with pytest.raises(ValueError, match="integer-like"):
        find_unserved_pop.create_unserved_pop(str(input_path), "not-a-number", str(output_path))


def test_find_unserved_pop_script_entrypoint_runs_via_fallback_imports(monkeypatch, tmp_path):
    import os
    import runpy
    from pathlib import Path

    import src.create_voronoi as create_voronoi_mod
    import src.starter as starter_mod

    cfg = {
        "figures": {"pop_threshold": 5},
        "paths": {
            "non_served_outpath": str(tmp_path / "non_served.gpkg"),
            "non_served_above_threshold_outpath": str(tmp_path / "filtered.gpkg"),
        },
    }
    captured = {}
    module_path = Path(__file__).resolve().parents[3] / "src" / "pop_at_risk_river_calculations" / "find_unserved_pop.py"

    monkeypatch.setattr(os, "chdir", lambda path: None)
    monkeypatch.setattr(find_unserved_pop.os.path, "exists", lambda path: path == os.path.abspath(str(tmp_path / "non_served.csv")))
    monkeypatch.setattr(starter_mod, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(starter_mod, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(create_voronoi_mod, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", path))
    monkeypatch.setattr(
        find_unserved_pop.duckdb,
        "sql",
        lambda query: _DuckResult(pd.DataFrame({"pop_sum": ["7"], "geometry": [box(0, 0, 1, 1).wkt]})),
    )

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["write"] = {"path": path, "driver": driver, "index": index, "rows": len(self)}

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        runpy.run_path(str(module_path), run_name="__main__")
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert captured["ensured"] == os.path.abspath(cfg["paths"]["non_served_above_threshold_outpath"])
    assert captured["write"] == {
        "path": os.path.abspath(cfg["paths"]["non_served_above_threshold_outpath"]),
        "driver": "GPKG",
        "index": False,
        "rows": 1,
    }


def test_process_epsg_group_returns_none_when_subset_is_empty(tiny_watershed_gdf):
    pop_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "epsg": [3857], "geometry": [box(-0.08, -0.08, -0.02, -0.02)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = find_diff_pop.process_epsg_group(
        32631,
        tiny_watershed_gdf,
        pop_gdf,
        basin_col="HYBAS_ID",
    )

    assert result is None


def test_process_epsg_group_reprojects_difference_output_back_to_wgs84(monkeypatch, tiny_watershed_gdf):
    pop_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "epsg": [3857], "geometry": [box(-0.08, -0.08, -0.02, -0.02)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    captured = {}

    def fake_find_difference(watershed_gdf, pop_subset, basin_col="HYBAS_ID"):
        captured["watershed_epsg"] = watershed_gdf.crs.to_epsg()
        captured["pop_epsg"] = pop_subset.crs.to_epsg()
        captured["basin_col"] = basin_col
        return pd.DataFrame(
            {"HYBAS_ID": [101], "geometry": [box(-5000.0, -5000.0, -1000.0, -1000.0)]}
        )

    monkeypatch.setattr(find_diff_pop, "find_difference", fake_find_difference)

    result = find_diff_pop.process_epsg_group(
        3857,
        tiny_watershed_gdf,
        pop_gdf,
        basin_col="HYBAS_ID",
    )

    assert result is not None
    assert result.crs.to_epsg() == 4326
    assert captured == {
        "watershed_epsg": 3857,
        "pop_epsg": 3857,
        "basin_col": "HYBAS_ID",
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [("true", True), ("0", False), ("Yes", True), (False, False)],
)
def test_parse_bool_handles_common_string_values(value, expected):
    assert find_diff_pop.parse_bool(value) is expected


def test_parse_bool_rejects_invalid_value():
    with pytest.raises(ValueError, match="Invalid boolean value"):
        find_diff_pop.parse_bool("not-bool")


def test_parse_args_parses_index_parallel_flag_and_optional_overrides(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "find_diff_pop.py",
            "2",
            "false",
            "lvl",
            "v2",
            "15000",
            "linear",
            "",
            "true",
            "0.75",
        ],
    )

    args = find_diff_pop.parse_args()

    assert args.index == 2
    assert args.is_parallel is False
    assert args.level == "lvl"
    assert args.version == "v2"
    assert args.buffer == "15000"
    assert args.weight_method == "linear"
    assert args.weight_func == ""
    assert args.dynamic_buffering == "true"
    assert args.dynamic_buffer_k == "0.75"


def test_find_diff_pop_main_selects_sorted_input_and_writes_output(
    mock_cfg,
    monkeypatch,
    tiny_watershed_gdf,
):
    cfg = mock_cfg
    cfg["paths"]["hydrowaste"] = "watersheds.gpkg"
    cfg["paths"]["pop_output_dir"] = "./pop_outputs"
    cfg["paths"]["pop_tif_dir"] = "./tifs"
    cfg["paths"]["pop_dif_output_dir"] = "./diff_outputs"
    cfg["max_workers"] = 3
    cfg["basin_column_name"] = "HYBAS_ID"
    cfg["country_output_column"] = "country"
    captured = {}

    pop_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "country": ["DE"], "geometry": [box(-0.08, -0.08, -0.02, -0.02)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    diff_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "country": ["DE"], "geometry": [box(-0.08, -0.08, -0.04, -0.04)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(
        find_diff_pop,
        "parse_args",
        lambda: SimpleNamespace(index=1, is_parallel=False),
    )
    monkeypatch.setattr(find_diff_pop.os, "chdir", lambda path: None)
    monkeypatch.setattr(find_diff_pop, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_diff_pop, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_diff_pop.os, "listdir", lambda path: ["z_input.gpkg", "a_input.gpkg"])
    monkeypatch.setattr(find_diff_pop.os.path, "exists", lambda path: False)
    monkeypatch.setattr(find_diff_pop.os, "makedirs", lambda path, exist_ok=False: captured.setdefault("makedirs", (path, exist_ok)))

    def fake_read_file(path):
        if path == os.path.join(cfg["paths"]["pop_output_dir"], "z_input.gpkg"):
            return pop_gdf.copy()
        if path == cfg["paths"]["hydrowaste"]:
            return tiny_watershed_gdf.copy()
        raise AssertionError(f"Unexpected read path: {path}")

    def fake_find_differences(watershed_gdf, pop_subset, max_workers=None, is_parallel=True, basin_col="HYBAS_ID"):
        captured["find_differences"] = {
            "max_workers": max_workers,
            "is_parallel": is_parallel,
            "basin_col": basin_col,
            "pop_rows": len(pop_subset),
            "watershed_rows": len(watershed_gdf),
        }
        return diff_gdf.copy()

    def fake_intersect_all_files(gdf, tif_dir, max_workers=None, country_col=None):
        captured["intersect_all_files"] = {
            "tif_dir": tif_dir,
            "max_workers": max_workers,
            "country_col": country_col,
            "rows": len(gdf),
        }
        return gdf.copy()

    def fake_ensure_output_dir_for_file(path):
        captured["ensured"] = path

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["to_file"] = {
            "path": path,
            "driver": driver,
            "index": index,
            "rows": len(self),
        }

    try:
        monkeypatch.setattr(find_diff_pop.gpd, "read_file", fake_read_file)
        monkeypatch.setattr(find_diff_pop, "find_differences", fake_find_differences)
        monkeypatch.setattr(find_diff_pop, "intersect_all_files", fake_intersect_all_files)
        monkeypatch.setattr(find_diff_pop, "ensure_output_dir_for_file", fake_ensure_output_dir_for_file)
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)

        find_diff_pop.main()
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    expected_output = os.path.join(cfg["paths"]["pop_dif_output_dir"], "diff_z_input.gpkg")
    assert captured["makedirs"] == (cfg["paths"]["pop_dif_output_dir"], True)
    assert captured["find_differences"] == {
        "max_workers": 3,
        "is_parallel": False,
        "basin_col": "HYBAS_ID",
        "pop_rows": 1,
        "watershed_rows": 2,
    }
    assert captured["intersect_all_files"] == {
        "tif_dir": cfg["paths"]["pop_tif_dir"],
        "max_workers": 3,
        "country_col": "country",
        "rows": 1,
    }
    assert captured["ensured"] == expected_output
    assert captured["to_file"] == {
        "path": expected_output,
        "driver": "GPKG",
        "index": False,
        "rows": 1,
    }


def test_find_diff_pop_main_raises_when_no_input_files_exist(mock_cfg, monkeypatch):
    cfg = mock_cfg
    cfg["paths"]["hydrowaste"] = "watersheds.gpkg"
    cfg["paths"]["pop_output_dir"] = "./pop_outputs"
    cfg["paths"]["pop_tif_dir"] = "./tifs"
    cfg["paths"]["pop_dif_output_dir"] = "./diff_outputs"

    monkeypatch.setattr(find_diff_pop, "parse_args", lambda: SimpleNamespace(index=0, is_parallel=True))
    monkeypatch.setattr(find_diff_pop.os, "chdir", lambda path: None)
    monkeypatch.setattr(find_diff_pop, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_diff_pop, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_diff_pop.os, "listdir", lambda path: [])

    with pytest.raises(FileNotFoundError, match="No matching input .gpkg files found"):
        find_diff_pop.main()


def test_find_diff_pop_main_ignores_non_gpkg_files(mock_cfg, monkeypatch):
    cfg = mock_cfg
    cfg["paths"]["hydrowaste"] = "watersheds.gpkg"
    cfg["paths"]["pop_output_dir"] = "./pop_outputs"
    cfg["paths"]["pop_tif_dir"] = "./tifs"
    cfg["paths"]["pop_dif_output_dir"] = "./diff_outputs"

    monkeypatch.setattr(find_diff_pop, "parse_args", lambda: SimpleNamespace(index=0, is_parallel=True))
    monkeypatch.setattr(find_diff_pop.os, "chdir", lambda path: None)
    monkeypatch.setattr(find_diff_pop, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_diff_pop, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_diff_pop.os, "listdir", lambda path: ["notes.txt", "result.parquet"])

    with pytest.raises(FileNotFoundError, match="No matching input .gpkg files found"):
        find_diff_pop.main()


def test_find_diff_pop_main_raises_when_index_is_out_of_range(mock_cfg, monkeypatch):
    cfg = mock_cfg
    cfg["paths"]["hydrowaste"] = "watersheds.gpkg"
    cfg["paths"]["pop_output_dir"] = "./pop_outputs"
    cfg["paths"]["pop_tif_dir"] = "./tifs"
    cfg["paths"]["pop_dif_output_dir"] = "./diff_outputs"

    monkeypatch.setattr(find_diff_pop, "parse_args", lambda: SimpleNamespace(index=3, is_parallel=True))
    monkeypatch.setattr(find_diff_pop.os, "chdir", lambda path: None)
    monkeypatch.setattr(find_diff_pop, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(find_diff_pop, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(find_diff_pop.os, "listdir", lambda path: ["only_input.gpkg"])

    with pytest.raises(IndexError, match=r"index must be in \[0, 0\], got 3"):
        find_diff_pop.main()