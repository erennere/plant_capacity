from __future__ import annotations

import os
from types import SimpleNamespace

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

from research_code.data_merge import correct_locations_w_OSM, final_data_merge, merge_seg_results


pytestmark = pytest.mark.unit


def test_cluster_point_indices_merges_transitive_neighbors():
    geoms = [
        Point(0, 0).wkt,
        Point(4, 0).wkt,
        Point(8, 0).wkt,
        Point(30, 0).wkt,
    ]

    clusters = final_data_merge.cluster_point_indices(geoms, threshold=5)
    normalized = {frozenset(cluster) for cluster in clusters}

    assert normalized == {frozenset({0, 1, 2}), frozenset({3})}


def test_cluster_points_sums_population_and_keeps_richest_geometry():
    df = gpd.GeoDataFrame(
        {
            "meter_geometry": [Point(0, 0).wkt, Point(3, 0).wkt, Point(25, 0).wkt],
            "geometry": [Point(10, 10), Point(20, 20), Point(30, 30)],
            "POP_SERVED": [100, 250, 50],
            "wwtp_area_square": ["[5]", "[7 3]", "[2]"],
            "diameters": ["[2]", "[4]", "[1]"],
            "name": [pd.NA, "richest", "solo"],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = final_data_merge.cluster_points(df, threshold=5)
    result = gpd.GeoDataFrame(result, geometry="geometry", crs="EPSG:4326")

    assert len(result) == 2

    merged_row = result[result["name"] == "richest"].iloc[0]
    assert merged_row["POP_SERVED"] == 350
    assert merged_row["wwtp_area_square"] == "[15.0]"
    assert merged_row["round_area"] == pytest.approx((1.0**2 * 3.141592653589793) + (2.0**2 * 3.141592653589793))
    assert merged_row["geometry"].equals(Point(20, 20))


def test_assign_to_nearest_merges_matching_target_attributes_and_preserves_unmatched_rows():
    gdf_source = gpd.GeoDataFrame(
        {
            "src_id": [1, 2, 3],
            "geometry": [Point(0, 0), Point(100, 0), None],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )
    gdf_target = gpd.GeoDataFrame(
        {
            "target_name": ["near-a", "near-b"],
            "geometry": [Point(1, 0), Point(102, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )

    result = merge_seg_results.assign_to_nearest(gdf_source, gdf_target, threshold=5)
    result = result.sort_values("src_id").reset_index(drop=True)

    assert result.crs == gdf_source.crs
    assert result.loc[0, "target_name"] == "near-a"
    assert result.loc[1, "target_name"] == "near-b"
    assert pd.isna(result.loc[2, "target_name"])
    assert "nearest_index" not in result.columns
    assert "geometry_nearest" not in result.columns


def test_assign_to_nearest_respects_distance_threshold():
    gdf_source = gpd.GeoDataFrame(
        {
            "src_id": [1],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )
    gdf_target = gpd.GeoDataFrame(
        {
            "target_name": ["far-away"],
            "geometry": [Point(1000, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )

    result = merge_seg_results.assign_to_nearest(gdf_source, gdf_target, threshold=100)

    assert pd.isna(result.loc[0, "target_name"])


def test_assign_to_nearest_requires_defined_crs():
    gdf_source = gpd.GeoDataFrame(
        {"src_id": [1], "geometry": [Point(0, 0)]},
        geometry="geometry",
    )
    gdf_target = gpd.GeoDataFrame(
        {"target_name": ["a"], "geometry": [Point(1, 1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="CRS"):
        merge_seg_results.assign_to_nearest(gdf_source, gdf_target)


def test_coordinate_corr_locations_wosm_matches_polygon_centroid_within_radius():
    pdf = gpd.GeoDataFrame(
        {
            "epsg": [3857],
            "geometry": [Polygon([(50, -20), (70, -20), (70, 20), (50, 20)])],
        },
        geometry="geometry",
        crs="EPSG:3857",
    ).to_crs(4326)

    df = gpd.GeoDataFrame(
        {
            "epsg": [3857],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    ).to_crs(4326)

    result = correct_locations_w_OSM.coordinate_corr_locations_wOSM(100, pdf, df)
    result = gpd.GeoDataFrame(result, geometry="geometry", crs="EPSG:4326")

    assert len(result) == 1
    assert result.loc[0, "matched_osm_geometry"].geom_type == "Point"

    reprojected = result.set_geometry("matched_osm_geometry").to_crs(3857)
    original = result.to_crs(3857)
    moved_distance = original.geometry.iloc[0].distance(reprojected.geometry.iloc[0])
    assert moved_distance <= 100
    assert reprojected.geometry.iloc[0].x == pytest.approx(60, abs=1e-6)
    assert reprojected.geometry.iloc[0].y == pytest.approx(0, abs=1e-6)


def test_coordinate_corr_locations_wosm_leaves_unmatched_rows_empty():
    pdf = gpd.GeoDataFrame(
        {
            "epsg": [3857],
            "geometry": [Point(1000, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    ).to_crs(4326)

    df = gpd.GeoDataFrame(
        {
            "epsg": [3857],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    ).to_crs(4326)

    result = correct_locations_w_OSM.coordinate_corr_locations_wOSM(100, pdf, df)

    assert pd.isna(result.loc[0, "matched_osm_geometry"])


def test_find_unmatched_targets_returns_only_targets_without_near_source():
    rng = np.random.default_rng(77)
    gdf_source = gpd.GeoDataFrame(
        {
            "src_id": [1, 2],
            "geometry": [Point(*rng.random(2) * 10), Point(*(rng.random(2) * 10 + 20))],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )
    near_geom = Point(gdf_source.geometry.iloc[0].x + 1, gdf_source.geometry.iloc[0].y + 1)
    far_geom = Point(gdf_source.geometry.iloc[1].x + 1000, gdf_source.geometry.iloc[1].y + 1000)
    gdf_target = gpd.GeoDataFrame(
        {
            "target_id": [10, 20],
            "geometry": [near_geom, far_geom],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )

    result = final_data_merge.find_unmatched_targets(gdf_source, gdf_target, threshold=50)

    assert result["target_id"].tolist() == [20]


def test_get_best_points_overrides_geometry_and_splits_high_and_low_confidence():
    rng = np.random.default_rng(88)
    gdf = gpd.GeoDataFrame(
        {
            "detection_flag": [True, False, False],
            "best_file2_lon": [rng.random(), rng.random() + 10, np.nan],
            "best_file2_lat": [rng.random(), rng.random() + 10, np.nan],
            "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    high_conf, low_conf = final_data_merge.get_best_points(gdf)

    assert len(high_conf) == 1
    assert len(low_conf) == 2
    assert high_conf.geometry.iloc[0].x == pytest.approx(gdf.loc[0, "best_file2_lon"])
    assert low_conf.geometry.iloc[0].x == pytest.approx(gdf.loc[1, "best_file2_lon"])
    assert low_conf.geometry.iloc[1].equals(Point(2, 2))


def test_find_safe_epsg_uses_polygon_centroid(monkeypatch):
    captured = {}
    polygon = Polygon([(10, 20), (12, 20), (12, 24), (10, 24)])

    monkeypatch.setattr(
        final_data_merge,
        "estimate_utm_epsg",
        lambda lon, lat: captured.setdefault("coords", (lon, lat)) or 32632,
    )

    epsg = final_data_merge.find_safe_epsg(pd.Series({"geometry": polygon}))

    assert epsg == captured["coords"]
    assert captured["coords"][0] == pytest.approx(polygon.centroid.x)
    assert captured["coords"][1] == pytest.approx(polygon.centroid.y)


def test_find_meter_coordinates_returns_empty_geodataframe_for_empty_input():
    empty = gpd.GeoDataFrame({"epsg": [], "geometry": []}, geometry="geometry", crs="EPSG:4326")

    result = final_data_merge.find_meter_coordinates(empty)

    assert result.empty
    assert result.crs.to_epsg() == 4326
    assert "meter_geometry" in result.columns


def test_find_meter_coordinates_adds_projected_wkt_by_epsg_group():
    gdf = gpd.GeoDataFrame(
        {
            "epsg": [3857, 3857],
            "geometry": [Point(0, 0), Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = final_data_merge.find_meter_coordinates(gdf)

    assert "meter_geometry" in result.columns
    assert result["meter_geometry"].notna().all()
    assert result.crs.to_epsg() == 4326


def test_merge_new_drops_old_segmentation_columns_and_writes(monkeypatch, tmp_path):
    corrected_path = str(tmp_path / "corrected_all.gpkg")
    seg_csv_path = str(tmp_path / "seg_results.csv")
    cfg = {
        "paths": {
            "corrected_all_filepath": corrected_path,
            "seg_results_filepath": seg_csv_path,
        }
    }
    captured = {}

    points_df = gpd.GeoDataFrame(
        {
            "idx": [1, 2],
            "num_detection_circle": [9, 8],
            "wwtp_area_square": ["[1]", "[2]"],
            "geometry": [Point(0, 0), Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    seg_results = pd.DataFrame(
        {
            "img_name": ["1.png", "2.png"],
            "num_detection_circle": [1, 2],
            "diameters": ["[3]", "[4]"],
        }
    )

    monkeypatch.setattr(merge_seg_results.gpd, "read_file", lambda path: points_df.copy())
    monkeypatch.setattr(merge_seg_results.pd, "read_csv", lambda path: seg_results.copy())
    monkeypatch.setattr(
        merge_seg_results,
        "ensure_output_dir_for_file",
        lambda path: captured.setdefault("ensured", path),
    )

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, filename=None, driver=None, index=None, **kwargs):
        captured["write"] = {
            "filename": filename,
            "driver": driver,
            "index": index,
            "columns": self.columns.tolist(),
            "rows": len(self),
        }

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        merge_seg_results.merge_new(cfg)
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert captured["ensured"] == corrected_path
    assert captured["write"]["filename"] == corrected_path
    assert captured["write"]["driver"] == "GPKG"
    assert captured["write"]["index"] is False
    assert captured["write"]["rows"] == 2
    assert "num_detection_square" not in captured["write"]["columns"]


def test_merge_new_requires_img_name_column(monkeypatch, tmp_path):
    corrected_path = str(tmp_path / "corrected_all.gpkg")
    seg_csv_path = str(tmp_path / "seg_results.csv")
    cfg = {
        "paths": {
            "corrected_all_filepath": corrected_path,
            "seg_results_filepath": seg_csv_path,
        }
    }

    points_df = gpd.GeoDataFrame(
        {"idx": [1], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    seg_results = pd.DataFrame({"wrong_col": ["1.png"]})

    monkeypatch.setattr(merge_seg_results.gpd, "read_file", lambda path: points_df.copy())
    monkeypatch.setattr(merge_seg_results.pd, "read_csv", lambda path: seg_results.copy())

    with pytest.raises(KeyError, match="img_name"):
        merge_seg_results.merge_new(cfg)


def test_merge_new_rejects_non_numeric_img_name_prefix(monkeypatch, tmp_path):
    corrected_path = str(tmp_path / "corrected_all.gpkg")
    seg_csv_path = str(tmp_path / "seg_results.csv")
    cfg = {
        "paths": {
            "corrected_all_filepath": corrected_path,
            "seg_results_filepath": seg_csv_path,
        }
    }

    points_df = gpd.GeoDataFrame(
        {"idx": [1], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    seg_results = pd.DataFrame({"img_name": ["abc.png"]})

    monkeypatch.setattr(merge_seg_results.gpd, "read_file", lambda path: points_df.copy())
    monkeypatch.setattr(merge_seg_results.pd, "read_csv", lambda path: seg_results.copy())

    with pytest.raises(ValueError, match="integer id"):
        merge_seg_results.merge_new(cfg)


def test_merge_seg_main_dispatches_variants_and_legacy_flag(monkeypatch):
    captured = {"old": 0, "new": 0}

    monkeypatch.setattr(merge_seg_results.os, "chdir", lambda path: None)
    monkeypatch.setattr(merge_seg_results, "parse_config_overrides", lambda args=None: {})
    monkeypatch.setattr(merge_seg_results, "merge_old", lambda cfg: captured.__setitem__("old", captured["old"] + 1))
    monkeypatch.setattr(merge_seg_results, "merge_new", lambda cfg: captured.__setitem__("new", captured["new"] + 1))

    monkeypatch.setattr(merge_seg_results, "parse_args", lambda: SimpleNamespace(variant="old"))
    monkeypatch.setattr(merge_seg_results, "load_config", lambda **overrides: {"legacy_merge": True})
    merge_seg_results.main()

    monkeypatch.setattr(merge_seg_results, "parse_args", lambda: SimpleNamespace(variant="old"))
    monkeypatch.setattr(merge_seg_results, "load_config", lambda **overrides: {"legacy_merge": False})
    merge_seg_results.main()

    monkeypatch.setattr(merge_seg_results, "parse_args", lambda: SimpleNamespace(variant="new"))
    monkeypatch.setattr(merge_seg_results, "load_config", lambda **overrides: {"legacy_merge": True})
    merge_seg_results.main()

    assert captured == {"old": 1, "new": 1}


def test_final_data_merge_main_writes_new_and_corrected_outputs(monkeypatch, tmp_path):
    corrected_south = str(tmp_path / "corrected_south.gpkg")
    seg_corrected_south = str(tmp_path / "seg_corrected_south.gpkg")
    thailand_path = str(tmp_path / "thailand.gpkg")
    us_path = str(tmp_path / "us.gpkg")
    eu_path = str(tmp_path / "eu.gpkg")
    germany_path = str(tmp_path / "germany.gpkg")
    osm_path = str(tmp_path / "osm.gpkg")
    new_points_path = str(tmp_path / "new_points.gpkg")
    corrected_all_path = str(tmp_path / "corrected_all.gpkg")
    canada_path = str(tmp_path / "canada.csv")

    cfg = {
        "legacy_merge": False,
        "osm_threshold": 100,
        "threshold": 50,
        "paths": {
            "corrected_south": corrected_south,
            "seg_corrected_south": seg_corrected_south,
            "thailand_filepath": thailand_path,
            "us_new_filepath": us_path,
            "eu_new_filepath": eu_path,
            "germany_filepath": germany_path,
            "osmgeo_filepath": osm_path,
            "new_points_filepath": new_points_path,
            "corrected_all_filepath": corrected_all_path,
            "canada_filepath": canada_path,
            "data_dir": str(tmp_path),
        },
    }
    captured = {"writes": []}

    base_old = gpd.GeoDataFrame(
        {"geometry": [Point(0, 0)], "WASTE_ID": [1], "detection_flag": [True]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    thailand = gpd.GeoDataFrame({"geometry": [Point(2, 2)]}, geometry="geometry", crs="EPSG:4326")
    us_df = gpd.GeoDataFrame({"geometry": [Point(3, 3)]}, geometry="geometry", crs="EPSG:4326")
    eu_df = gpd.GeoDataFrame({"geometry": [Point(4, 4)]}, geometry="geometry", crs="EPSG:4326")
    germany_df = gpd.GeoDataFrame(
        {"geometry": [Point(10, 10)], "neigh_lon": [10.0], "neigh_lat": [10.0]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    osm_df = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")
    old_reference = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")

    def fake_read_file(path):
        if path == corrected_south:
            return base_old.copy()
        if path == thailand_path:
            return thailand.copy()
        if path == us_path:
            return us_df.copy()
        if path == eu_path:
            return eu_df.copy()
        if path == germany_path:
            return germany_df.copy()
        if path == osm_path:
            return osm_df.copy()
        if path.endswith("corrected_WWTP_enhanced.geojson"):
            return old_reference.copy()
        raise AssertionError(f"Unexpected read path: {path}")

    canada_csv = pd.DataFrame(
        {
            "Longitude/ Longitude": [5.0],
            "Latitude/ Latitude": [5.0],
        }
    )

    high = gpd.GeoDataFrame(
        {"geometry": [Point(6, 6)], "detection_flag": [True], "WASTE_ID": [2]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    low = gpd.GeoDataFrame(
        {"geometry": [Point(7, 7)], "detection_flag": [False], "WASTE_ID": [3]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    corrected_low = low.copy()
    corrected_low["matched_osm_geometry"] = [Point(7.1, 7.1)]

    monkeypatch.setattr(final_data_merge.os, "chdir", lambda path: None)
    monkeypatch.setattr(final_data_merge, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(final_data_merge, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(final_data_merge.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(final_data_merge.pd, "read_csv", lambda path, encoding=None: canada_csv.copy())
    monkeypatch.setattr(final_data_merge, "get_best_points", lambda gdf: (high.copy(), low.copy()))
    monkeypatch.setattr(final_data_merge, "find_unmatched_targets", lambda src, target, threshold: target.iloc[[0]].copy())
    monkeypatch.setattr(final_data_merge, "coordinate_corr_locations_wOSM", lambda rad, pdf, df: corrected_low.copy())
    monkeypatch.setattr(final_data_merge, "find_safe_epsg", lambda row: 3857)
    monkeypatch.setattr(
        final_data_merge,
        "find_meter_coordinates",
        lambda df: gpd.GeoDataFrame(df.assign(meter_geometry=df["geometry"].apply(lambda g: g.wkt)), geometry="geometry", crs=4326),
    )
    monkeypatch.setattr(final_data_merge, "cluster_points", lambda df, threshold: df)
    monkeypatch.setattr(final_data_merge, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", []).append(path))

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["writes"].append(
            {
                "path": path,
                "driver": driver,
                "index": index,
                "rows": len(self),
                "has_idx": "idx" in self.columns,
            }
        )

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        final_data_merge.main()
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    write_paths = [w["path"] for w in captured["writes"]]
    assert new_points_path in write_paths
    assert corrected_all_path in write_paths
    corrected_write = next(w for w in captured["writes"] if w["path"] == corrected_all_path)
    assert corrected_write["driver"] == "GPKG"
    assert corrected_write["index"] is False
    assert corrected_write["has_idx"] is True


def test_correct_locations_main_writes_corrected_and_missing_outputs(monkeypatch, tmp_path):
    corrected_path = str(tmp_path / "corrected_south.geojson")
    paul_path = str(tmp_path / "paul_corrected.gpkg")
    osm_path = str(tmp_path / "osm.gpkg")
    overture_path = str(tmp_path / "overture.parquet")
    data_dir = str(tmp_path)
    cfg = {
        "rad": 100,
        "paths": {
            "data_dir": data_dir,
            "paul_corrected_filepath": paul_path,
            "osmgeo_filepath": osm_path,
            "corrected_south": corrected_path,
            "overture": overture_path,
            "overture_s3_url": "s3://bucket/overture.parquet",
        },
    }
    captured = {"writes": []}

    corrected_input = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1, 2],
            "neigh_lon": [np.nan, 1.0],
            "neigh_lat": [np.nan, 2.0],
            "lon": [0.0, 10.0],
            "lat": [0.0, 10.0],
            "CNTRY_ISO": ["DEU", "FRA"],
            "ISO_2": ["DE", "FR"],
            "geometry": [Point(0, 0), Point(10, 10)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    osm_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")
    corrected_match = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            "neigh_lon": [np.nan],
            "neigh_lat": [np.nan],
            "lon": [0.0],
            "lat": [0.0],
            "CNTRY_ISO": ["DEU"],
            "ISO_2": ["DE"],
            "combined_geometry": [Point(0, 0)],
            "corrected_geometry": [None],
            "HW_geometry": [Point(0, 0)],
            "geometry": [Point(0, 0)],
            "matched_osm_geometry": [Point(0.5, 0.5)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    old_reference = gpd.GeoDataFrame(
        {"geometry": [Point(0.5, 0.5)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    old_filename = os.path.abspath(os.path.join(data_dir, "corrected_WWTP_enhanced.geojson"))

    def fake_read_file(path):
        if path == paul_path:
            return corrected_input.copy()
        if path == osm_path:
            return osm_gdf.copy()
        if path == old_filename:
            return old_reference.copy()
        raise AssertionError(f"Unexpected read path: {path}")

    monkeypatch.setattr(correct_locations_w_OSM.os, "chdir", lambda path: None)
    monkeypatch.setattr(correct_locations_w_OSM, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(correct_locations_w_OSM, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(correct_locations_w_OSM.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(correct_locations_w_OSM, "estimate_utm_epsg", lambda lon, lat: 3857)
    monkeypatch.setattr(
        correct_locations_w_OSM,
        "coordinate_corr_locations_wOSM",
        lambda rad, pdf, df: corrected_match.copy(),
    )
    monkeypatch.setattr(correct_locations_w_OSM, "ensure_output_dir_for_file", lambda path: captured.setdefault("ensured", []).append(path))

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["writes"].append({"path": path, "driver": driver, "index": index, "rows": len(self)})

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        correct_locations_w_OSM.main()
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    missing_output = os.path.abspath(os.path.join(data_dir, "missing_WWTPs.geojson"))
    write_paths = [w["path"] for w in captured["writes"]]
    assert corrected_path in write_paths
    assert missing_output in write_paths
    assert all(w["index"] is False for w in captured["writes"])


def test_correct_locations_main_downloads_overture_and_enriches_when_iso2_missing(monkeypatch, tmp_path):
    corrected_path = str(tmp_path / "corrected_south.geojson")
    paul_path = str(tmp_path / "paul_corrected.gpkg")
    osm_path = str(tmp_path / "osm.gpkg")
    overture_path = str(tmp_path / "overture.parquet")
    data_dir = str(tmp_path)
    cfg = {
        "rad": 100,
        "paths": {
            "data_dir": data_dir,
            "paul_corrected_filepath": paul_path,
            "osmgeo_filepath": osm_path,
            "corrected_south": corrected_path,
            "overture": overture_path,
            "overture_s3_url": "s3://bucket/overture.parquet",
        },
    }
    captured = {}

    corrected_input = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            "neigh_lon": [np.nan],
            "neigh_lat": [np.nan],
            "lon": [0.0],
            "lat": [0.0],
            "CNTRY_ISO": ["DEU"],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    osm_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, geometry="geometry", crs="EPSG:4326")
    corrected_match = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            "neigh_lon": [np.nan],
            "neigh_lat": [np.nan],
            "lon": [0.0],
            "lat": [0.0],
            "CNTRY_ISO": ["DEU"],
            "combined_geometry": [Point(0, 0)],
            "corrected_geometry": [None],
            "HW_geometry": [Point(0, 0)],
            "geometry": [Point(0, 0)],
            "matched_osm_geometry": [Point(0.5, 0.5)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    old_reference = gpd.GeoDataFrame(
        {"geometry": [Point(0.5, 0.5)]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    old_filename = os.path.abspath(os.path.join(data_dir, "corrected_WWTP_enhanced.geojson"))

    def fake_read_file(path):
        if path == paul_path:
            return corrected_input.copy()
        if path == osm_path:
            return osm_gdf.copy()
        if path == old_filename:
            return old_reference.copy()
        raise AssertionError(f"Unexpected read path: {path}")

    monkeypatch.setattr(correct_locations_w_OSM.os, "chdir", lambda path: None)
    monkeypatch.setattr(correct_locations_w_OSM, "parse_config_overrides", lambda start_index=1: {})
    monkeypatch.setattr(correct_locations_w_OSM, "load_config", lambda **overrides: cfg)
    monkeypatch.setattr(correct_locations_w_OSM.gpd, "read_file", fake_read_file)
    monkeypatch.setattr(correct_locations_w_OSM, "estimate_utm_epsg", lambda lon, lat: 3857)
    monkeypatch.setattr(
        correct_locations_w_OSM,
        "coordinate_corr_locations_wOSM",
        lambda rad, pdf, df: corrected_match.copy(),
    )

    enriched = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            "CNTRY_ISO": ["DEU"],
            "ISO_2": [None],
            "geometry": [Point(0.5, 0.5)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    monkeypatch.setattr(correct_locations_w_OSM, "enrich_country_with_duckdb", lambda df, filepath: enriched.copy())
    monkeypatch.setattr(
        correct_locations_w_OSM,
        "get_iso_codes",
        lambda: ({"DEU": "DE"}, {"DE": "DEU"}, {}, {}),
    )
    monkeypatch.setattr(
        correct_locations_w_OSM,
        "download_overture_maps",
        lambda s3, path: captured.setdefault("downloaded", (s3, path)),
    )
    monkeypatch.setattr(correct_locations_w_OSM, "ensure_output_dir_for_file", lambda path: None)

    original_exists = correct_locations_w_OSM.os.path.exists
    monkeypatch.setattr(
        correct_locations_w_OSM.os.path,
        "exists",
        lambda path: False if path == overture_path else original_exists(path),
    )

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        if path == corrected_path:
            captured["iso2"] = self["ISO_2"].tolist()

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        correct_locations_w_OSM.main()
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    assert captured["downloaded"] == (cfg["paths"]["overture_s3_url"], overture_path)
    assert captured["iso2"] == ["DE"]


def test_merge_old_reads_zip_csvs_and_writes_output(monkeypatch, tmp_path):
    cfg = {
        "paths": {
            "dl_mapfile": str(tmp_path / "mapping.gpkg"),
            "dl_zipfile": str(tmp_path / "tiles.zip"),
            "data_dir": str(tmp_path),
            "dl_dir": "DL_results",
            "corrected_south": str(tmp_path / "corrected_south.gpkg"),
            "seg_corrected_south": "seg_corrected_south.gpkg",
        }
    }
    captured = {}

    gdf = gpd.GeoDataFrame(
        {
            "src_id": [1, 2],
            "geometry": [Point(0, 0), None],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    mapping = gpd.GeoDataFrame(
        {
            "idx": [1],
            "geometry": [Point(0.1, 0.1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    zip_output = os.path.join(cfg["paths"]["data_dir"], cfg["paths"]["dl_dir"], "tiles")
    zip_inner = os.path.join(zip_output, "tiles")

    class _ZipStub:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extractall(self, path):
            captured["extractall"] = path

    monkeypatch.setattr(
        merge_seg_results.gpd,
        "read_file",
        lambda path: gdf.copy() if path == cfg["paths"]["corrected_south"] else mapping.copy(),
    )
    monkeypatch.setattr(merge_seg_results.pd, "read_csv", lambda path: pd.DataFrame({"File Name": ["1.png"]}))
    monkeypatch.setattr(merge_seg_results.os.path, "exists", lambda path: False)
    monkeypatch.setattr(merge_seg_results.os, "makedirs", lambda path, exist_ok=False: captured.setdefault("makedirs", (path, exist_ok)))
    monkeypatch.setattr(merge_seg_results.zipfile, "ZipFile", lambda path, mode: _ZipStub())
    monkeypatch.setattr(merge_seg_results.os, "listdir", lambda path: ["1.csv"] if path == zip_inner else [])
    monkeypatch.setattr(
        merge_seg_results,
        "assign_to_nearest",
        lambda source, target: source.assign(merged=True),
    )
    monkeypatch.setattr(
        merge_seg_results,
        "ensure_output_dir_for_file",
        lambda path: captured.setdefault("ensured", path),
    )

    original_to_file = gpd.GeoDataFrame.to_file

    def fake_to_file(self, path, driver=None, index=None, **kwargs):
        captured["write"] = {"path": path, "driver": driver, "index": index, "rows": len(self)}

    try:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
        merge_seg_results.merge_old(cfg)
    finally:
        monkeypatch.setattr(gpd.GeoDataFrame, "to_file", original_to_file)

    expected_output = os.path.join(cfg["paths"]["data_dir"], cfg["paths"]["seg_corrected_south"])
    assert captured["makedirs"] == (zip_output, True)
    assert captured["extractall"] == zip_output
    assert captured["ensured"] == expected_output
    assert captured["write"] == {"path": expected_output, "driver": "GPKG", "index": False, "rows": 2}