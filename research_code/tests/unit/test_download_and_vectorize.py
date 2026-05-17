from __future__ import annotations

import os

import geopandas as gpd
import pytest
from shapely.geometry import Point

from research_code.industrial_analysis import download_and_vectorize


pytestmark = pytest.mark.unit


def test_find_raster_dirs_returns_only_directories_with_tif_files(tmp_path):
    has_tif = tmp_path / "a" / "nested"
    has_tif.mkdir(parents=True)
    (has_tif / "tile_1.tif").write_text("stub", encoding="utf-8")

    no_tif = tmp_path / "b"
    no_tif.mkdir()
    (no_tif / "notes.txt").write_text("stub", encoding="utf-8")

    result = download_and_vectorize._find_raster_dirs(str(tmp_path))

    assert str(has_tif) in result
    assert str(no_tif) not in result


def test_vectorize_rasters_parallel_returns_empty_when_no_rasters(tmp_path):
    result = download_and_vectorize.vectorize_rasters_parallel(str(tmp_path), max_workers=2)

    assert result == []


def test_vectorize_rasters_parallel_rejects_non_positive_workers(tmp_path):
    with pytest.raises(ValueError, match="max_workers"):
        download_and_vectorize.vectorize_rasters_parallel(str(tmp_path), max_workers=0)


def test_vectorize_and_merge_raises_when_no_raster_dirs():
    with pytest.raises(FileNotFoundError, match="No raster directories found"):
        download_and_vectorize._vectorize_and_merge([], max_workers=1, min_cells=10)


def test_vectorize_and_merge_raises_when_vectorization_produces_no_outputs(monkeypatch):
    monkeypatch.setattr(download_and_vectorize, "vectorize_rasters_parallel", lambda *args, **kwargs: [])

    with pytest.raises(ValueError, match="Failed to vectorize any raster files"):
        download_and_vectorize._vectorize_and_merge(["/tmp/rasters"], max_workers=1, min_cells=10)


def test_merge_geodataframes_rejects_invalid_max_workers():
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(0, 0).buffer(0.01)], "value": [1]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="max_workers"):
        download_and_vectorize.merge_geodataframes([gdf], max_workers="invalid")


def test_main_uses_cached_vectorized_polygons_and_writes_enriched_result(monkeypatch, tmp_path):
    vectorized_path = str(tmp_path / "industrial_merged.parquet")
    watershed_path = str(tmp_path / "watershed.gpkg")
    cfg = {
        "paths": {
            "industrial_merged_filepath": vectorized_path,
            "watershed": watershed_path,
            "overture": str(tmp_path / "overture.parquet"),
            "overture_s3_url": "s3://example/overture.parquet",
            "industrial_raster_persistent_dir": str(tmp_path / "rasters"),
        },
        "industrial_vectorize_overwrite": False,
        "industrial_min_cells": 20,
        "industrial_persist_rasters": False,
        "industrial_simplify_tolerance": 0.01,
        "max_workers": 2,
        "basin_column_name": "HYBAS_ID",
        "sindex_concurrency": False,
        "country_boundary_column": "country",
        "country_output_column": "ISO_2",
        "industrial_zenodo_url": "https://example.com/industrial.zip",
    }
    captured = {"removed": [], "writes": [], "replaced": []}
    state = {"vectorized_exists": True}

    merged_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")
    watershed_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [1], "geometry": [Point(0, 0).buffer(1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    enriched_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [1], "ISO_2": ["DE"], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    original_exists = download_and_vectorize.os.path.exists

    def fake_exists(path):
        if path == vectorized_path:
            return state["vectorized_exists"]
        return original_exists(path)

    monkeypatch.setattr(download_and_vectorize, "parse_config_overrides", lambda args=None, argv=None, start_index=1: {})
    monkeypatch.setattr(download_and_vectorize, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(download_and_vectorize.os.path, "exists", fake_exists)
    monkeypatch.setattr(download_and_vectorize.gpd, "read_parquet", lambda path: merged_gdf.copy())
    monkeypatch.setattr(download_and_vectorize.gpd, "read_file", lambda path, driver=None: watershed_gdf.copy())
    monkeypatch.setattr(download_and_vectorize, "add_boundary_info", lambda *args, **kwargs: enriched_gdf.copy())

    def fake_remove(path):
        captured["removed"].append(path)
        if path == vectorized_path:
            state["vectorized_exists"] = False

    monkeypatch.setattr(download_and_vectorize.os, "remove", fake_remove)
    monkeypatch.setattr(download_and_vectorize.os, "replace", lambda src, dst: captured["replaced"].append((src, dst)))

    original_to_parquet = gpd.GeoDataFrame.to_parquet

    def fake_to_parquet(self, path, index=False, **kwargs):
        captured["writes"].append({"path": path, "index": index, "rows": len(self), "columns": self.columns.tolist()})

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_parquet", fake_to_parquet)
        result = download_and_vectorize.main()
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_parquet", original_to_parquet)

    assert result is True
    assert captured["removed"] == []
    assert captured["writes"] == [
        {
            "path": f"{vectorized_path}.tmp",
            "index": False,
            "rows": 1,
            "columns": ["HYBAS_ID", "ISO_2", "geometry"],
        }
    ]
    assert captured["replaced"] == [(f"{vectorized_path}.tmp", vectorized_path)]


def test_add_boundary_info_empty_input_short_circuits(monkeypatch):
    industrial = gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4326")
    watershed = gpd.GeoDataFrame(
        {"HYBAS_ID": [1], "geometry": [Point(0, 0).buffer(1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(download_and_vectorize, "intersects_with_country_db", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("country helper should not run")))
    monkeypatch.setattr(download_and_vectorize, "intersect_with_polygon_sindex", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("basin helper should not run")))

    out = download_and_vectorize.add_boundary_info(
        industrial,
        watershed,
        overture_path="unused.parquet",
        overture_s3_url="s3://unused",
        basin_col="HYBAS_ID",
        sindex_concurrency=False,
        country_boundary_col="country",
        country_output_col="ISO_2",
    )

    assert out.empty
    assert "ISO_2" in out.columns
    assert "HYBAS_ID" in out.columns


def test_main_returns_false_when_boundary_enrichment_fails(monkeypatch, tmp_path):
    vectorized_path = str(tmp_path / "industrial_merged.parquet")
    watershed_path = str(tmp_path / "watershed.gpkg")
    cfg = {
        "paths": {
            "industrial_merged_filepath": vectorized_path,
            "watershed": watershed_path,
            "overture": str(tmp_path / "overture.parquet"),
            "overture_s3_url": "s3://example/overture.parquet",
            "industrial_raster_persistent_dir": str(tmp_path / "rasters"),
        },
        "industrial_vectorize_overwrite": False,
        "industrial_min_cells": 20,
        "industrial_persist_rasters": False,
        "industrial_simplify_tolerance": 0.01,
        "max_workers": 2,
        "basin_column_name": "HYBAS_ID",
        "sindex_concurrency": False,
        "country_boundary_column": "country",
        "country_output_column": "ISO_2",
        "industrial_zenodo_url": "https://example.com/industrial.zip",
    }

    merged_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")
    watershed_gdf = gpd.GeoDataFrame(
        {"HYBAS_ID": [1], "geometry": [Point(0, 0).buffer(1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(download_and_vectorize, "parse_config_overrides", lambda args=None, argv=None, start_index=1: {})
    monkeypatch.setattr(download_and_vectorize, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(download_and_vectorize.os.path, "exists", lambda path: path == vectorized_path)
    monkeypatch.setattr(download_and_vectorize.gpd, "read_parquet", lambda path: merged_gdf.copy())
    monkeypatch.setattr(download_and_vectorize.gpd, "read_file", lambda path, driver=None: watershed_gdf.copy())
    monkeypatch.setattr(
        download_and_vectorize,
        "add_boundary_info",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("enrichment failed")),
    )

    result = download_and_vectorize.main()

    assert result is False
