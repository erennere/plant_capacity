from __future__ import annotations

import logging
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point

from research_code import add_pop


pytestmark = pytest.mark.unit


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


class _RasterStub:
    def __init__(self, crs="EPSG:4326"):
        self.crs = crs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_find_country_tif_files_maps_existing_country_directories(monkeypatch, tmp_path):
    pop_root = tmp_path / "population"
    deu_dir = pop_root / "deu"
    deu_dir.mkdir(parents=True)
    (deu_dir / "worldpop_2020_deu.tif").write_text("stub", encoding="utf-8")

    monkeypatch.setattr(
        add_pop,
        "get_iso_codes",
        lambda: ({"deu": "DE"}, {"DE": "DEU"}, {}, {}),
    )

    result = add_pop.find_country_tif_files(["DE", "FR", "ZZ"], str(pop_root))

    assert len(result["DE"]) == 1
    assert Path(result["DE"][0]).name == "worldpop_2020_deu.tif"
    assert result["FR"] is None
    assert result["ZZ"] is None


def test_intersect_single_file_clips_negative_stats_to_zero(monkeypatch, tiny_watershed_gdf):
    tif_path = "synthetic_2020_population.tif"
    original_exists = add_pop.os.path.exists

    monkeypatch.setattr(
        add_pop.os.path,
        "exists",
        lambda path: True if path == tif_path else original_exists(path),
    )
    monkeypatch.setattr(add_pop.rasterio, "open", lambda path: _RasterStub())
    monkeypatch.setattr(
        add_pop,
        "exact_extract",
        lambda rast, vec, ops, output: pd.DataFrame({"sum": [10.0, -5.0], "stdev": [1.0, -2.0]}),
    )

    result = add_pop.intersect_single_file(tiny_watershed_gdf.copy(), [tif_path], all_years=True)

    assert result["2020_zonal_sum"].tolist() == pytest.approx([10.0, 0.0])
    assert result["2020_zonal_std"].tolist() == pytest.approx([1.0, 0.0])


def test_find_newest_country_tif_files_selects_highest_year_from_seeded_random_order(monkeypatch, tmp_path):
    rng = np.random.default_rng(7)
    pop_root = tmp_path / "population"
    deu_dir = pop_root / "deu"
    deu_dir.mkdir(parents=True)
    years = rng.permutation([2018, 2020, 2023]).tolist()
    for year in years:
        (deu_dir / f"worldpop_{year}_deu.tif").write_text("stub", encoding="utf-8")

    monkeypatch.setattr(
        add_pop,
        "get_iso_codes",
        lambda: ({"deu": "DE"}, {"DE": "DEU"}, {}, {}),
    )

    result = add_pop.find_newest_country_tif_files(["DE"], str(pop_root))

    assert Path(result["DE"]).name == "worldpop_2023_deu.tif"


def test_intersect_single_file_returns_original_when_no_usable_year_can_be_parsed(monkeypatch, tiny_watershed_gdf):
    tif_path = "population_unknown_year.tif"
    original_exists = add_pop.os.path.exists

    monkeypatch.setattr(
        add_pop.os.path,
        "exists",
        lambda path: True if path == tif_path else original_exists(path),
    )
    monkeypatch.setattr(
        add_pop.rasterio,
        "open",
        lambda path: (_ for _ in ()).throw(AssertionError("raster should not be opened when no year is parseable")),
    )

    result = add_pop.intersect_single_file(tiny_watershed_gdf.copy(), [tif_path], all_years=True)

    assert result.crs == tiny_watershed_gdf.crs
    assert result.columns.tolist() == tiny_watershed_gdf.columns.tolist()


def test_intersect_all_files_preserves_input_crs_and_concatenates_seeded_random_country_slices(monkeypatch):
    rng = np.random.default_rng(19)
    point_a, point_b = rng.random(2), rng.random(2) + 5
    gdf = gpd.GeoDataFrame(
        {
            "ISO_2": ["DE", "FR"],
            "value": rng.integers(10, 100, size=2),
            "geometry": [Point(point_a[0], point_a[1]), Point(point_b[0], point_b[1])],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )

    monkeypatch.setattr(add_pop, "ProcessPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(add_pop, "as_completed", lambda futures: futures)
    monkeypatch.setattr(add_pop, "tqdm", lambda iterable, total=None, desc=None: iterable)
    monkeypatch.setattr(add_pop.random, "shuffle", lambda seq: seq.reverse())
    monkeypatch.setattr(
        add_pop,
        "find_country_tif_files",
        lambda countries, tif_dir: {"DE": ["de_2020.tif"], "FR": ["fr_2020.tif"]},
    )

    def fake_intersect_single_file(sub_gdf, tif_paths, all_years=True):
        return sub_gdf.assign(pop_sum_seed=int(rng.integers(1, 50)))

    monkeypatch.setattr(add_pop, "intersect_single_file", fake_intersect_single_file)

    result = add_pop.intersect_all_files(gdf, tif_dir="unused", max_workers=1, all_years=True, country_col="ISO_2")

    assert result.crs.to_epsg() == 3857
    assert set(result["ISO_2"]) == {"DE", "FR"}
    assert "pop_sum_seed" in result.columns


def test_intersect_all_files_returns_empty_geodataframe_with_geometry_when_no_country_has_rasters(monkeypatch):
    gdf = gpd.GeoDataFrame(
        {
            "ISO_2": ["ZZ"],
            "geometry": [Point(1, 2)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )

    monkeypatch.setattr(add_pop, "find_country_tif_files", lambda countries, tif_dir: {"ZZ": None})
    monkeypatch.setattr(add_pop, "ProcessPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(add_pop, "as_completed", lambda futures: futures)
    monkeypatch.setattr(add_pop, "tqdm", lambda iterable, total=None, desc=None: iterable)

    result = add_pop.intersect_all_files(gdf, tif_dir="unused", max_workers=1, all_years=True, country_col="ISO_2")

    assert result.empty
    assert result.crs.to_epsg() == 3857
    assert "geometry" in result.columns


def test_orchestrate_intersections_rejects_negative_index(tmp_path):
    data_dir = tmp_path / "voronoi"
    output_dir = tmp_path / "output"
    data_dir.mkdir()
    (data_dir / "sample.gpkg").write_text("stub", encoding="utf-8")

    with pytest.raises(IndexError, match="out of range"):
        add_pop.orchestrate_intersections(str(data_dir), "unused", str(output_dir), index=-1)


def test_orchestrate_intersections_reads_selected_file_and_writes_output(monkeypatch, tmp_path):
    data_dir = tmp_path / "voronoi"
    output_dir = tmp_path / "output"
    data_dir.mkdir()
    (data_dir / "b_file.gpkg").write_text("stub", encoding="utf-8")
    (data_dir / "a_file.gpkg").write_text("stub", encoding="utf-8")
    rng = np.random.default_rng(31)
    captured = {}

    source_gdf = gpd.GeoDataFrame(
        {
            "ISO_2": ["DE", "FR"],
            "geometry": [Point(*rng.random(2)), Point(*(rng.random(2) + 1))],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    def fake_read_file(path):
        captured["read_path"] = path
        return source_gdf.copy()

    def fake_intersect_all_files(gdf, tif_dir, max_workers, all_years=True, country_col="ISO_2"):
        captured["intersect"] = {
            "tif_dir": tif_dir,
            "max_workers": max_workers,
            "country_col": country_col,
            "rows": len(gdf),
        }
        return gdf.assign(seed_value=int(rng.integers(1, 1000)))

    monkeypatch.setattr(add_pop.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(add_pop, "intersect_all_files", fake_intersect_all_files)
    monkeypatch.setattr(add_pop, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", path))

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["write"] = {"path": path, "driver": driver, "index": index, "rows": len(self)}

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        add_pop.orchestrate_intersections(str(data_dir), "tifs", str(output_dir), index=1, max_workers=3, country_col="ISO_2")
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    expected_input = str(data_dir / "b_file.gpkg")
    expected_output = str(output_dir / "pop_added_b_file.gpkg")
    assert captured["read_path"] == expected_input
    assert captured["intersect"] == {"tif_dir": "tifs", "max_workers": 3, "country_col": "ISO_2", "rows": 2}
    assert captured["ensured"] == expected_output
    assert captured["write"] == {"path": expected_output, "driver": "GPKG", "index": False, "rows": 2}


def test_main_passes_configured_paths_and_avoids_false_existing_output_warning(monkeypatch, mock_cfg, caplog, tmp_path):
    cfg = mock_cfg
    cfg["paths"]["voronoi_dir"] = str(tmp_path / "voronoi")
    cfg["paths"]["pop_tif_dir"] = str(tmp_path / "tifs")
    cfg["paths"]["pop_output_dir"] = str(tmp_path / "new_output")
    cfg["country_output_column"] = "ISO_2"
    cfg["add_pop_max_workers"] = 5
    cfg["pop_voronoi_overwrite"] = False
    captured = {}
    state = {"output_exists": False}

    monkeypatch.setattr(sys, "argv", ["add_pop.py", "2"])
    monkeypatch.setattr(add_pop, "parse_config_overrides", lambda start_index=2: {})
    monkeypatch.setattr(add_pop, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(add_pop.os, "chdir", lambda path: None)

    def fake_makedirs(path, exist_ok=False):
        captured.setdefault("makedirs", (path, exist_ok))
        if path == cfg["paths"]["pop_output_dir"]:
            state["output_exists"] = True

    monkeypatch.setattr(add_pop.os, "makedirs", fake_makedirs)

    original_exists = add_pop.os.path.exists
    monkeypatch.setattr(
        add_pop.os.path,
        "exists",
        lambda path: state["output_exists"] if path == cfg["paths"]["pop_output_dir"] else original_exists(path),
    )
    monkeypatch.setattr(
        add_pop,
        "orchestrate_intersections",
        lambda data_dir, tif_dir, output_dir, index, max_workers, country_col=None: captured.update(
            {
                "call": {
                    "data_dir": data_dir,
                    "tif_dir": tif_dir,
                    "output_dir": output_dir,
                    "index": index,
                    "max_workers": max_workers,
                    "country_col": country_col,
                }
            }
        ),
    )

    with caplog.at_level(logging.WARNING):
        add_pop.main()

    assert captured["makedirs"] == (cfg["paths"]["pop_output_dir"], True)
    assert captured["call"] == {
        "data_dir": cfg["paths"]["voronoi_dir"],
        "tif_dir": cfg["paths"]["pop_tif_dir"],
        "output_dir": cfg["paths"]["pop_output_dir"],
        "index": 2,
        "max_workers": 5,
        "country_col": "ISO_2",
    }
    assert "already exists and pop_voronoi_overwrite is False" not in caplog.text


def test_main_exits_on_non_integer_index(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["add_pop.py", "not-an-int"])

    with pytest.raises(SystemExit, match="1"):
        add_pop.main()