from __future__ import annotations

import concurrent.futures
import os
import runpy
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely import to_wkb
from shapely.geometry import Point, Polygon

import src.create_voronoi as create_voronoi
import src.starter as starter
from src.annotation_scripts import NEW_03_WASTEWATERJOIN_GEOJSON as ww3


pytestmark = pytest.mark.unit


class _Future:
    def __init__(self, fn=None, args=(), kwargs=None, value=None):
        self._fn = fn
        self._args = args
        self._kwargs = kwargs or {}
        self._value = value

    def result(self):
        if self._fn is None:
            return self._value
        return self._fn(*self._args, **self._kwargs)


class _Executor:
    def __init__(self, max_workers=None, eager_results=True):
        self.max_workers = max_workers
        self.eager_results = eager_results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args, **kwargs):
        if self.eager_results:
            return _Future(fn, args, kwargs)
        return _Future(value=None)


def test_new03_import_fallback_block_executes():
    module_path = Path(__file__).resolve().parents[3] / "src" / "annotation_scripts" / "NEW_03_WASTEWATERJOIN_GEOJSON.py"

    module_globals = runpy.run_path(str(module_path), run_name="not_main")

    assert "merge_bboxes_sql" in module_globals


def test_write_geodata_ensures_parent_and_writes(monkeypatch, tmp_path):
    gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")
    captured = {}

    monkeypatch.setattr(ww3, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", path))
    monkeypatch.setattr(gpd.GeoDataFrame, "to_file", lambda self, path, driver="GeoJSON": captured.setdefault("written", (path, driver)))

    ww3.write_geodata(gdf, str(tmp_path / "out.geojson"))

    assert captured["ensured"].endswith("out.geojson")
    assert captured["written"][1] == "GeoJSON"


def test_cluster_points_skips_empty_neighbor_candidates():
    points = [Point(0, 0), Point()]

    class _Tree:
        def query(self, buffered, predicate=None):
            return [1]

    clusters = ww3.cluster_points(points, _Tree(), distance_threshold=1.0)

    assert clusters == [{0}]


def test_convert_geojson_to_parquet_success_path(monkeypatch, tmp_path):
    captured = {"queries": []}

    class _Conn:
        def execute(self, query):
            captured["queries"].append(str(query))
            return self

        def close(self):
            captured["closed"] = True

    monkeypatch.setattr(ww3.os.path, "exists", lambda path: False)
    monkeypatch.setattr(ww3, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", path))
    monkeypatch.setattr(ww3.duckdb, "connect", lambda *a, **k: _Conn())

    result = ww3.convert_geojson_to_parquet("idx_1_polygons.geojson", str(tmp_path), overwrite=False)

    assert result.endswith("idx_1_polygons.parquet")
    assert captured["ensured"].endswith("idx_1_polygons.parquet")
    assert captured["closed"] is True
    assert any("ST_Read('idx_1_polygons.geojson')" in query for query in captured["queries"])


def test_convert_geojson_to_parquet_failure_and_existing_skip(monkeypatch, tmp_path):
    class _FailConn:
        def execute(self, query):
            raise RuntimeError("duckdb failed")

        def close(self):
            return None

    monkeypatch.setattr(ww3.os.path, "exists", lambda path: False)
    monkeypatch.setattr(ww3, "ensure_output_dir_for_file", lambda path: None)
    monkeypatch.setattr(ww3.duckdb, "connect", lambda *a, **k: _FailConn())

    assert ww3.convert_geojson_to_parquet("idx_2_polygons.geojson", str(tmp_path), overwrite=False) is None

    monkeypatch.setattr(ww3.os.path, "exists", lambda path: True)
    monkeypatch.setattr(ww3.duckdb, "connect", lambda *a, **k: pytest.fail("duckdb.connect should not be called"))

    skipped = ww3.convert_geojson_to_parquet("idx_3_polygons.geojson", str(tmp_path), overwrite=False)

    assert skipped.endswith("idx_3_polygons.parquet")


def test_parallel_convert_geojsons_handles_future_errors(monkeypatch, tmp_path):
    monkeypatch.setattr(ww3, "ProcessPoolExecutor", _Executor)
    monkeypatch.setattr(ww3, "as_completed", lambda futures: list(futures))

    def fake_convert(path, temp_parquet_dir, overwrite=False):
        if "bad" in path:
            raise RuntimeError("conversion failed")
        return str(Path(temp_parquet_dir) / (Path(path).stem + ".parquet"))

    monkeypatch.setattr(ww3, "convert_geojson_to_parquet", fake_convert)

    result = ww3.parallel_convert_geojsons(["good.geojson", "bad.geojson"], str(tmp_path), max_workers=1)

    assert result == [str(tmp_path / "good.parquet")]


def test_merge_parquets_sql_ignores_schema_discovery_failures(monkeypatch):
    class _Conn:
        def __init__(self):
            self.commands = []

        def execute(self, query):
            self.commands.append(str(query))
            return self

    def fake_discover(parquet_file):
        if parquet_file == "bad.parquet":
            raise RuntimeError("schema failure")
        return parquet_file, "1", ["Geom", "Name"], {"Geom": "BLOB", "Name": "TEXT"}

    monkeypatch.setattr(ww3, "ThreadPoolExecutor", _Executor)
    monkeypatch.setattr(ww3, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(ww3, "discover_parquet_schema", fake_discover)

    conn = _Conn()
    with pytest.raises(KeyError, match="bad.parquet"):
        ww3.merge_parquets_sql(conn, ["good.parquet", "bad.parquet"], max_workers=1, insert_batch_size=1)

    assert any("CREATE TABLE dataset" in command for command in conn.commands)
    assert any("INSERT INTO dataset" in command for command in conn.commands)


def test_new03_script_entrypoint_runs_merge_tasks_and_calls_main(monkeypatch, tmp_path):
    cfg = {
        "overwrite_existing": False,
        "paths": {
            "corrected_all_filepath": str(tmp_path / "points.gpkg"),
            "annotations_grid_dir": str(tmp_path / "grids"),
            "annotations_by_osm_dir": str(tmp_path / "osm"),
            "annotations_temp_parquet_dir": str(tmp_path / "tmp_parquets"),
        },
    }
    os.makedirs(cfg["paths"]["annotations_grid_dir"], exist_ok=True)
    os.makedirs(cfg["paths"]["annotations_by_osm_dir"], exist_ok=True)

    output_dir = os.path.join(os.path.abspath(os.path.join(cfg["paths"]["annotations_grid_dir"], "..")), "data")
    output_filepath = os.path.join(output_dir, "merged_polygons.parquet")
    lines_filepath = output_filepath.replace("polygons", "lines")
    output_geojson = os.path.join(output_dir, "wastewater_plant.geojson")
    original_exists = os.path.exists
    checks = {"polygons": 0}
    captured = {}

    def fake_exists(path):
        abs_path = os.path.abspath(str(path))
        if abs_path == os.path.abspath(output_filepath):
            checks["polygons"] += 1
            return checks["polygons"] >= 2
        if abs_path == os.path.abspath(lines_filepath):
            return False
        return original_exists(path)

    def fake_to_file(self, path, driver="GeoJSON"):
        captured["written"] = {"path": str(path), "driver": driver, "rows": len(self)}

    df = pd.DataFrame(
        {
            "geometry": [
                to_wkb(Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])),
                to_wkb(Polygon([(3, 3), (4, 3), (3, 4), (3, 3)])),
            ]
        }
    )

    monkeypatch.setattr(os, "chdir", lambda path: None)
    monkeypatch.setattr(os.path, "exists", fake_exists)
    monkeypatch.setattr(starter, "parse_config_overrides", lambda *a, **k: {})
    monkeypatch.setattr(starter, "load_config", lambda **kwargs: cfg)
    monkeypatch.setattr("src.utils.ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", path))
    monkeypatch.setattr(pd, "read_parquet", lambda path: df.copy())
    monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
    monkeypatch.setattr(concurrent.futures, "ProcessPoolExecutor", lambda max_workers=None: _Executor(max_workers=max_workers, eager_results=False))

    runpy.run_path(str(Path(ww3.__file__).resolve()), run_name="__main__")

    assert captured["ensured"] == output_geojson
    assert captured["written"]["path"] == output_geojson
    assert captured["written"]["rows"] == 2
