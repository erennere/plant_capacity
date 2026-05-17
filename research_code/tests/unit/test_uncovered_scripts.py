from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import LineString, Point, Polygon

from research_code import combine_watersheds
from research_code.figures_scripts import convert_voronoi_to_geojson_for_map
from research_code.pop_at_risk_river_calculations import assign_rivers_to_basin


pytestmark = pytest.mark.unit


def test_extract_and_merge_geodata_merges_first_readable_layer_per_zip(monkeypatch, tmp_path):
    zip_dir = tmp_path / "zips"
    out_dir = tmp_path / "out"
    zip_dir.mkdir()

    # Two readable archives and one malformed archive.
    (zip_dir / "a.zip").write_bytes(b"PK\x03\x04fake")
    (zip_dir / "b.zip").write_bytes(b"PK\x03\x04fake")
    (zip_dir / "bad.zip").write_text("not-a-zip", encoding="utf-8")

    captured = {}

    def fake_read_file(path):
        name = Path(path).name
        if name in {"candidate_a.geojson", "candidate_b.geojson"}:
            return gpd.GeoDataFrame(
                {"source": [name], "geometry": [Point(0, 0)]},
                geometry="geometry",
                crs="EPSG:4326",
            )
        raise RuntimeError("not geospatial")

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["rows"] = len(self)
        captured["path"] = path
        captured["driver"] = driver

    tmp_roots = []

    class _FakeTmpDir:
        def __enter__(self):
            root = tmp_path / f"tmp_{len(tmp_roots)}"
            root.mkdir(exist_ok=True)
            tmp_roots.append(root)
            return str(root)

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeZip:
        def __init__(self, zip_path, mode="r"):
            self.zip_path = Path(zip_path)

        def __enter__(self):
            if self.zip_path.name == "bad.zip":
                raise combine_watersheds.zipfile.BadZipFile("bad")
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extractall(self, target):
            root = Path(target)
            if self.zip_path.name == "a.zip":
                (root / "candidate_a.geojson").write_text("{}", encoding="utf-8")
                (root / "skip.txt").write_text("x", encoding="utf-8")
            elif self.zip_path.name == "b.zip":
                (root / "candidate_b.geojson").write_text("{}", encoding="utf-8")

    def fake_walk(root):
        root = Path(root)
        files = sorted(p.name for p in root.iterdir() if p.is_file())
        yield str(root), [], files

    monkeypatch.setattr(combine_watersheds.tempfile, "TemporaryDirectory", _FakeTmpDir)
    monkeypatch.setattr(combine_watersheds.zipfile, "ZipFile", _FakeZip)
    monkeypatch.setattr(combine_watersheds.os, "walk", fake_walk)
    monkeypatch.setattr(combine_watersheds.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(combine_watersheds, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", str(path)))

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        combine_watersheds.extract_and_merge_geodata(zip_dir, out_dir, output_filename="merged.gpkg")
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert captured["ensured"].endswith("merged.gpkg")
    assert captured["rows"] == 2
    assert captured["driver"] == "GPKG"


def test_extract_and_merge_geodata_handles_no_readable_layers(monkeypatch, tmp_path):
    zip_dir = tmp_path / "zips"
    out_dir = tmp_path / "out"
    zip_dir.mkdir()
    (zip_dir / "a.zip").write_bytes(b"PK\x03\x04fake")

    class _FakeTmpDir:
        def __enter__(self):
            root = tmp_path / "tmp"
            root.mkdir(exist_ok=True)
            return str(root)

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeZip:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extractall(self, target):
            (Path(target) / "notes.txt").write_text("x", encoding="utf-8")

    monkeypatch.setattr(combine_watersheds.tempfile, "TemporaryDirectory", _FakeTmpDir)
    monkeypatch.setattr(combine_watersheds.zipfile, "ZipFile", _FakeZip)
    monkeypatch.setattr(combine_watersheds.os, "walk", lambda root: [(str(root), [], ["notes.txt"])])
    monkeypatch.setattr(combine_watersheds.gpd, "read_file", lambda path: (_ for _ in ()).throw(RuntimeError("bad")))

    called = {"to_file": False}
    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, *args, **kwargs):
        called["to_file"] = True

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        combine_watersheds.extract_and_merge_geodata(zip_dir, out_dir)
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert called["to_file"] is False


def test_extract_and_merge_geodata_reads_only_one_layer_per_zip_across_subdirs(monkeypatch, tmp_path):
    zip_dir = tmp_path / "zips"
    out_dir = tmp_path / "out"
    zip_dir.mkdir()
    (zip_dir / "a.zip").write_bytes(b"PK\x03\x04fake")

    captured = {}

    class _FakeTmpDir:
        def __enter__(self):
            root = tmp_path / "tmp_nested"
            (root / "d1").mkdir(parents=True, exist_ok=True)
            (root / "d2").mkdir(parents=True, exist_ok=True)
            return str(root)

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeZip:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extractall(self, target):
            root = Path(target)
            (root / "d1" / "candidate_1.geojson").write_text("{}", encoding="utf-8")
            (root / "d2" / "candidate_2.geojson").write_text("{}", encoding="utf-8")

    def fake_walk(root):
        root = Path(root)
        yield str(root / "d1"), [], ["candidate_1.geojson"]
        yield str(root / "d2"), [], ["candidate_2.geojson"]

    def fake_read_file(path):
        return gpd.GeoDataFrame(
            {"source": [Path(path).name], "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["rows"] = len(self)

    monkeypatch.setattr(combine_watersheds.tempfile, "TemporaryDirectory", _FakeTmpDir)
    monkeypatch.setattr(combine_watersheds.zipfile, "ZipFile", _FakeZip)
    monkeypatch.setattr(combine_watersheds.os, "walk", fake_walk)
    monkeypatch.setattr(combine_watersheds.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(combine_watersheds, "ensure_output_dir_for_file", lambda path: None)

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        combine_watersheds.extract_and_merge_geodata(zip_dir, out_dir, output_filename="merged.gpkg")
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert captured["rows"] == 1


def test_combine_watersheds_main_handles_missing_optional_overrides(monkeypatch, tmp_path):
    base = tmp_path / "levels"
    (base / "lvl1").mkdir(parents=True)
    (base / "misc").mkdir(parents=True)

    cfg_initial = {
        "paths": {
            "watersheds_zip_dir": str(base / "lvl1"),
            "watershed": str(tmp_path / "out" / "lvl1.gpkg"),
        }
    }

    calls = []

    def fake_load_config(**kwargs):
        if kwargs:
            level = kwargs.get("level")
            return {
                "paths": {
                    "watersheds_zip_dir": str(base / f"lvl{level}"),
                    "watershed": str(tmp_path / "out" / f"lvl{level}.gpkg"),
                }
            }
        return cfg_initial

    monkeypatch.setattr(combine_watersheds.os, "chdir", lambda path: None)
    monkeypatch.setattr(combine_watersheds, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(combine_watersheds, "load_config", fake_load_config)
    monkeypatch.setattr(combine_watersheds.os, "listdir", lambda p: ["lvl1", "misc"])
    monkeypatch.setattr(combine_watersheds.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(
        combine_watersheds,
        "extract_and_merge_geodata",
        lambda zip_dir, out_dir, output_filename="merged.gpkg": calls.append((zip_dir, out_dir, output_filename)),
    )

    combine_watersheds.main()

    assert len(calls) == 1
    assert calls[0][0].endswith("lvl1")
    assert calls[0][2] == "lvl1.gpkg"


def test_convert_voronoi_main_converts_to_centroids_and_updates_html(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    figures_dir = data_dir / "figures"
    figures_dir.mkdir(parents=True)

    geojson_output = data_dir / "figures" / "points.geojson"
    html_path = figures_dir / "sizes_interactive_map.html"
    html_path.write_text('fetch("old/path.geojson")\n', encoding="utf-8")

    cfg = {
        "figures": {"approach": 1},
        "paths": {
            "leaflet_geojson_filepath": str(geojson_output),
            "data_dir": str(data_dir),
        },
    }

    src = gpd.GeoDataFrame(
        {
            "total_area": [10.0, 20.0],
            "round_area": [6.0, 12.0],
            "geometry": [Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]), None],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    captured = {}
    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["path"] = path
        captured["driver"] = driver
        captured["index"] = index
        captured["geoms"] = list(self.geometry)

    monkeypatch.setattr(convert_voronoi_to_geojson_for_map.os, "chdir", lambda path: None)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "create_pop_output_paths", lambda _: {"voronoi": {"1": "in.gpkg"}})
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map.gpd, "read_file", lambda *args, **kwargs: src.copy())
    monkeypatch.setattr(
        convert_voronoi_to_geojson_for_map,
        "ensure_output_dir_for_file",
        lambda path: captured.setdefault("ensured", path),
    )

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        convert_voronoi_to_geojson_for_map.main()
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert captured["ensured"] == str(geojson_output)
    assert captured["path"] == str(geojson_output)
    assert captured["driver"] == "GeoJSON"
    assert captured["index"] is False
    assert captured["geoms"][0].geom_type == "Point"
    assert captured["geoms"][1] is None
    assert 'fetch("./points.geojson")' in html_path.read_text(encoding="utf-8")


def test_convert_voronoi_main_skips_html_rewrite_when_file_missing(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    cfg = {
        "figures": {"approach": 1},
        "paths": {
            "leaflet_geojson_filepath": str(data_dir / "figures" / "points.geojson"),
            "data_dir": str(data_dir),
        },
    }

    src = gpd.GeoDataFrame(
        {"total_area": [1.0], "round_area": [1.0], "geometry": [Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(convert_voronoi_to_geojson_for_map.os, "chdir", lambda path: None)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "create_pop_output_paths", lambda _: {"voronoi": {"1": "in.gpkg"}})
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map.gpd, "read_file", lambda *args, **kwargs: src.copy())
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "ensure_output_dir_for_file", lambda path: None)
    monkeypatch.setattr(gpd.GeoDataFrame, "to_file", lambda self, *args, **kwargs: None)

    convert_voronoi_to_geojson_for_map.main()


def test_convert_voronoi_main_requires_figures_approach(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    cfg = {
        "paths": {
            "leaflet_geojson_filepath": str(data_dir / "figures" / "points.geojson"),
            "data_dir": str(data_dir),
        },
    }

    monkeypatch.setattr(convert_voronoi_to_geojson_for_map.os, "chdir", lambda path: None)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "load_config", lambda **overrides: cfg)

    with pytest.raises(KeyError, match="figures.approach"):
        convert_voronoi_to_geojson_for_map.main()


def test_convert_voronoi_main_requires_voronoi_mapping_for_selected_approach(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    cfg = {
        "figures": {"approach": 2},
        "paths": {
            "leaflet_geojson_filepath": str(data_dir / "figures" / "points.geojson"),
            "data_dir": str(data_dir),
        },
    }

    monkeypatch.setattr(convert_voronoi_to_geojson_for_map.os, "chdir", lambda path: None)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(convert_voronoi_to_geojson_for_map, "create_pop_output_paths", lambda _: {"voronoi": {"1": "in.gpkg"}})

    with pytest.raises(KeyError, match="approach '2'"):
        convert_voronoi_to_geojson_for_map.main()


def test_extract_first_digit_handles_strings_and_missing_values():
    frame = pd.DataFrame({"HYRIV_ID": [" 123", "9", None]})

    result = assign_rivers_to_basin.extract_first_digit(frame.copy(), "HYRIV_ID", "continent")

    assert result["continent"].iloc[0] == "1"
    assert result["continent"].iloc[1] == "9"
    assert pd.isna(result["continent"].iloc[2])


def test_assign_hybas_id_by_length_handles_empty_inputs(tiny_watershed_gdf):
    lines = gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4326")

    out = assign_rivers_to_basin.assign_hybas_id_by_length(lines, tiny_watershed_gdf)

    assert out.empty


def test_assign_hybas_id_by_length_picks_longest_intersection_for_multi_match():
    polygons = gpd.GeoDataFrame(
        {
            "HYBAS_ID": [1, 2],
            "geometry": [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(1.5, 0), (4, 0), (4, 2), (1.5, 2)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    lines = gpd.GeoDataFrame(
        {
            "river": ["single", "multi"],
            "geometry": [
                LineString([(0.1, 1), (1.0, 1)]),
                LineString([(0.2, 1), (2.2, 1)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = assign_rivers_to_basin.assign_hybas_id_by_length(lines.copy(), polygons, id_col="HYBAS_ID")

    assert result["HYBAS_ID"].tolist() == [1, 1]


def test_orchestrate_intersections_keeps_unmatched_regions_and_combines_results(monkeypatch):
    hybas = gpd.GeoDataFrame(
        {
            "HYBAS_ID": ["1A"],
            "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    rivers = gpd.GeoDataFrame(
        {
            "HYRIV_ID": ["1x", "2y"],
            "geometry": [
                LineString([(0, 0.5), (1, 0.5)]),
                LineString([(2, 0.5), (3, 0.5)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

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

    monkeypatch.setattr(assign_rivers_to_basin, "ProcessPoolExecutor", _Exec)
    monkeypatch.setattr(assign_rivers_to_basin, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(assign_rivers_to_basin, "tqdm", lambda iterable, **kwargs: iterable)
    monkeypatch.setattr(
        assign_rivers_to_basin,
        "assign_hybas_id_by_length",
        lambda r_chunk, h_chunk, col: r_chunk.assign(**{col: [h_chunk.iloc[0][col]] * len(r_chunk)}),
    )

    out = assign_rivers_to_basin.orchestrate_intersections(
        hybas,
        rivers,
        hybas_col="HYBAS_ID",
        hyshed_col="HYRIV_ID",
        new_col="continent",
        max_workers=1,
    )

    assert sorted(out["HYRIV_ID"].tolist()) == ["1x", "2y"]
    matched = out[out["HYRIV_ID"] == "1x"]
    unmatched = out[out["HYRIV_ID"] == "2y"]
    assert matched["HYBAS_ID"].iloc[0] == "1A"
    assert "HYBAS_ID" not in unmatched.columns or pd.isna(unmatched.get("HYBAS_ID", pd.Series([pd.NA])).iloc[0])


def test_assign_rivers_orchestrate_intersections_accepts_leading_n_region(monkeypatch):
    hybas = gpd.GeoDataFrame(
        {
            "HYBAS_ID": ["nA"],
            "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    rivers = gpd.GeoDataFrame(
        {
            "HYRIV_ID": ["n1"],
            "geometry": [LineString([(0, 0.5), (1, 0.5)])],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

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

    monkeypatch.setattr(assign_rivers_to_basin, "ProcessPoolExecutor", _Exec)
    monkeypatch.setattr(assign_rivers_to_basin, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(assign_rivers_to_basin, "tqdm", lambda iterable, **kwargs: iterable)
    monkeypatch.setattr(
        assign_rivers_to_basin,
        "assign_hybas_id_by_length",
        lambda r_chunk, h_chunk, col: r_chunk.assign(**{col: [h_chunk.iloc[0][col]] * len(r_chunk)}),
    )

    out = assign_rivers_to_basin.orchestrate_intersections(
        hybas,
        rivers,
        hybas_col="HYBAS_ID",
        hyshed_col="HYRIV_ID",
        new_col="continent",
        max_workers=1,
    )

    assert out["HYBAS_ID"].iloc[0] == "nA"


def test_assign_rivers_orchestrate_intersections_rejects_non_positive_workers():
    hybas = gpd.GeoDataFrame(
        {"HYBAS_ID": ["1A"], "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    rivers = gpd.GeoDataFrame(
        {"HYRIV_ID": ["1x"], "geometry": [LineString([(0, 0.5), (1, 0.5)])]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="max_workers"):
        assign_rivers_to_basin.orchestrate_intersections(
            hybas,
            rivers,
            hybas_col="HYBAS_ID",
            hyshed_col="HYRIV_ID",
            new_col="continent",
            max_workers=0,
        )


def test_assign_hybas_id_by_length_requires_id_column():
    lines = gpd.GeoDataFrame(
        {"geometry": [LineString([(0, 0), (1, 1)])]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    polygons = gpd.GeoDataFrame(
        {"NOT_HYBAS": [1], "geometry": [Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(KeyError, match="HYBAS_ID"):
        assign_rivers_to_basin.assign_hybas_id_by_length(lines, polygons, id_col="HYBAS_ID")
