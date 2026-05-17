from __future__ import annotations

import io
import runpy
from unittest.mock import mock_open, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon, box

from research_code.pop_at_risk_river_calculations import create_rasters


pytestmark = pytest.mark.unit


def test_new04_exportgeotiff_script_executes_with_dummy_data(monkeypatch):
    import research_code.create_voronoi as cv

    grid = gpd.GeoDataFrame(
        {"idx": [1], "geometry": [box(0, 0, 10, 10)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    class _Layer:
        def plot(self, ax=None, color=None, edgecolor=None):
            return ax

    calls = {"n": 0}

    def fake_read_file(path):
        calls["n"] += 1
        return grid.copy() if calls["n"] == 1 else _Layer()

    class _Ax:
        def set_xlim(self, a, b):
            return None

        def set_ylim(self, a, b):
            return None

        def axis(self, value):
            return None

    class _Fig:
        def savefig(self, *args, **kwargs):
            return None

    monkeypatch.setattr(cv, "ensure_output_dir_for_file", lambda path: None)
    monkeypatch.setattr("geopandas.read_file", fake_read_file)
    monkeypatch.setattr("matplotlib.pyplot.subplots", lambda *args, **kwargs: (_Fig(), _Ax()))
    monkeypatch.setattr("matplotlib.pyplot.close", lambda fig: None)
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)

    with patch("builtins.open", mock_open()):
        runpy.run_module("research_code.annotation_scripts.NEW_04_EXPORTGEOTIFF", run_name="__main__")


def test_orchestrate_country_intersection_wraps_sign_and_extract(monkeypatch):
    tiny = gpd.GeoDataFrame(
        {"HYBAS_ID": [101], "geometry": [box(0, 0, 1, 1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(create_rasters, "polygon_raster_sign_from_gdf", lambda *args, **kwargs: ("out.tif", 10, -5))
    monkeypatch.setattr(create_rasters, "extract_worldpop_universal", lambda *args, **kwargs: tiny.copy())

    out = create_rasters.orchestrate_country_intersection("in.tif", tiny, tiny, "out.tif")

    assert out[0] == "out.tif"
    assert out[1] == 10 and out[2] == -5
    assert isinstance(out[3], gpd.GeoDataFrame)


def test_orchestrate_intersections_writes_stats_and_nonserved(monkeypatch, tmp_path):
    tif_dict = {"DE": "de.tif", "FR": "fr.tif"}
    gdf = gpd.GeoDataFrame(
        {
            "ISO_2": ["DE", "FR"],
            "HYBAS_ID": [101, 202],
            "geometry": [Point(0, 0), Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    watershed = gdf.copy()

    class _Future:
        def __init__(self, value):
            self._value = value

        def result(self):
            return self._value

    class _Exec:
        def __init__(self, max_workers=None):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            return _Future(fn(*args, **kwargs))

    def fake_orchestrate_country_intersection(tif_filepath, sub_gdf, sub_ws, out, **kwargs):
        island = gpd.GeoDataFrame(
            {"tile": ["1/1/1"], "geometry": [box(0, 0, 1, 1)]},
            geometry="geometry",
            crs="EPSG:3857",
        )
        return out, 100, -25, island

    writes = []

    def fake_to_csv(self, path, index=False, mode=None, header=None, **kwargs):
        writes.append({"path": path, "mode": mode, "header": header, "rows": len(self)})

    monkeypatch.setattr(create_rasters, "ProcessPoolExecutor", _Exec)
    monkeypatch.setattr(create_rasters, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(create_rasters, "tqdm", lambda iterable, **kwargs: iterable)
    monkeypatch.setattr(create_rasters.random, "shuffle", lambda seq: None)
    monkeypatch.setattr(create_rasters, "orchestrate_country_intersection", fake_orchestrate_country_intersection)
    monkeypatch.setattr(create_rasters, "ensure_output_dir_for_file", lambda path: None)
    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv)

    csv_path = str(tmp_path / "stats.csv")
    non_served = str(tmp_path / "non_served.gpkg")

    monkeypatch.setattr(create_rasters.os.path, "exists", lambda path: False)

    result = create_rasters.orchestrate_intersections(
        tif_dict,
        gdf,
        watershed,
        output_dir=str(tmp_path / "out"),
        csv_output_filepath=csv_path,
        non_served_outpath=non_served,
        max_workers=1,
        country_col="ISO_2",
        basin_col="HYBAS_ID",
    )

    assert result == {"DE": True, "FR": True}
    assert len(writes) >= 2


def test_orchestrate_intersections_rejects_non_positive_workers(monkeypatch, tmp_path):
    tif_dict = {"DE": "de.tif"}
    gdf = gpd.GeoDataFrame(
        {"ISO_2": ["DE"], "HYBAS_ID": [101], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="max_workers"):
        create_rasters.orchestrate_intersections(
            tif_dict,
            gdf,
            gdf.copy(),
            output_dir=str(tmp_path / "out"),
            csv_output_filepath=str(tmp_path / "stats.csv"),
            non_served_outpath=str(tmp_path / "non_served.gpkg"),
            max_workers=0,
            country_col="ISO_2",
            basin_col="HYBAS_ID",
        )


def test_create_rasters_main_watershed_requires_crs_and_read_file_no_crs_kwarg(monkeypatch, tmp_path):
    args = type("Args", (), {"job_index": 0, "total_jobs": 1})()
    monkeypatch.setattr(create_rasters, "parse_args", lambda: args)
    monkeypatch.setattr(create_rasters, "parse_config_overrides", lambda args=None: {})

    cfg = {
        "annotations": {"max_workers": 1, "random_seed": 7},
        "min_pixels": 3,
        "zoom_level": 8,
        "figures": {"approach": "1"},
        "paths": {
            "pop_tif_dir": str(tmp_path / "pop_tifs"),
            "WWTP_tif_dir": str(tmp_path / "out_tifs"),
            "non_served_outpath": str(tmp_path / "non_served.gpkg"),
            "csv_output_filepath": str(tmp_path / "stats.gpkg"),
            "watershed": str(tmp_path / "watershed.gpkg"),
            "overture": str(tmp_path / "overture.parquet"),
            "overture_s3_url": "s3://dummy/overture",
        },
        "country_output_column": "iso3",
        "country_boundary_column": "country_name",
        "basin_column_name": "HYBAS_ID",
    }
    monkeypatch.setattr(create_rasters, "load_config", lambda **kwargs: cfg)
    monkeypatch.setattr(
        create_rasters,
        "create_pop_output_paths",
        lambda cfg_: {"voronoi": {"1": str(tmp_path / "voronoi.gpkg")}},
    )

    voronoi = gpd.GeoDataFrame(
        {"iso3": ["DEU"], "geometry": [box(0, 0, 1, 1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    watershed_no_crs = gpd.GeoDataFrame(
        {"HYBAS_ID": [1], "country_name": ["Germany"], "geometry": [box(0, 0, 2, 2)]},
        geometry="geometry",
    )

    calls = {"watershed_kwargs": None}

    def _read_file(path, **kwargs):
        if str(path).endswith("voronoi.gpkg"):
            return voronoi.copy()
        calls["watershed_kwargs"] = dict(kwargs)
        return watershed_no_crs.copy()

    monkeypatch.setattr(create_rasters.gpd, "read_file", _read_file)
    monkeypatch.setattr(create_rasters.os.path, "exists", lambda _p: True)
    monkeypatch.setattr(create_rasters.os, "makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr(create_rasters.os, "chdir", lambda _p: None)
    monkeypatch.setattr(create_rasters, "find_newest_country_tif_files", lambda countries, tif_dir: {"DEU": str(tmp_path / "deu.tif")})

    with pytest.raises(ValueError, match="Watershed dataset must include CRS metadata"):
        create_rasters.main()

    assert calls["watershed_kwargs"] == {}
