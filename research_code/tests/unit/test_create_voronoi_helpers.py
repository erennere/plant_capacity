import numpy as np
import geopandas as gpd
import pytest
from shapely.geometry import Point, box

from research_code import create_voronoi


pytestmark = pytest.mark.unit


def test_calculate_area_combines_round_and_rect_components(sample_sites_gdf):
    result = create_voronoi.calculate_area(sample_sites_gdf.copy())

    expected_round_area = np.pi * (1.0**2 + 2.0**2)
    expected_total_area = 15.0 + expected_round_area

    assert result.loc[0, "round_area"] == pytest.approx(expected_round_area)
    assert result.loc[0, "total_area"] == pytest.approx(expected_total_area)
    assert result.loc[0, "num_detections"] == 4
    assert result.loc[0, "capacity_proxy"] == pytest.approx(expected_total_area * 2.0)

    fallback_mean = result["capacity_proxy"].mean()
    assert result.loc[1, "base_values"] == pytest.approx(fallback_mean)
    assert result.loc[2, "base_values"] == pytest.approx(fallback_mean)


def test_calculate_area_defaults_without_rectangles():
    df = gpd.GeoDataFrame(
        {"geometry": [Point(0, 0), Point(1, 1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = create_voronoi.calculate_area(df.copy())

    assert result["total_area"].tolist() == [1, 1]
    assert result["num_detections"].tolist() == [0, 0]
    assert result["capacity_proxy"].tolist() == [0.0, 0.0]
    assert result["base_values"].tolist() == [0.0, 0.0]


def test_default_distance_functions_match_normalized_plane_math():
    a = np.array([[0.0, 0.0], [3.0, 4.0]])
    b = (0.0, 0.0)

    additive = create_voronoi.default_distance_additive(a, b, weight=0.5, factor=1.0)
    multiplicative = create_voronoi.default_distance_multiplicative(a, b, weight=0.5, factor=1.0)

    assert additive == pytest.approx(np.array([0.1, np.sqrt(1.75)]))
    assert multiplicative == pytest.approx(np.array([0.0, np.sqrt(2.0) / 0.5]))


def test_cluster_point_indices_merges_transitive_neighbors():
    clusters = create_voronoi.cluster_point_indices(
        [Point(0, 0), Point(1, 0), Point(2, 0), Point(10, 0)],
        threshold=1.1,
    )

    assert {frozenset(cluster) for cluster in clusters} == {frozenset({0, 1, 2}), frozenset({3})}


def test_calculate_buffer_fixed_branch_clips_requested_buffer():
    df = gpd.GeoDataFrame(
        {"geometry": [Point(0, 0), Point(1, 1)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = create_voronoi.calculate_buffer(
        df,
        weights=np.array([0.5, 0.5]),
        buffer=9000,
        dynamic_buffering=False,
        min_buffer=2000,
        max_buffer=5000,
        k_min=0.32,
        k_max=0.92,
        detection_confidence_threshold=3,
    )

    assert result.tolist() == [5000.0, 5000.0]


def test_calculate_buffer_single_site_dynamic_uses_buffer_fallback(monkeypatch):
    df = gpd.GeoDataFrame(
        {
            "geometry": [Point(0, 0)],
            "num_detection_circle": [0],
            "num_detection_rect": [0],
            "basin_area": [1000.0],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(
        create_voronoi,
        "nearest_neighbor_distances_and_median",
        lambda input_df: (np.array([np.nan]), np.nan),
    )

    result = create_voronoi.calculate_buffer(
        df,
        weights=np.array([1.0]),
        buffer=10000,
        dynamic_buffering=True,
        min_buffer=2000,
        max_buffer=20000,
        k_min=0.32,
        k_max=0.92,
        detection_confidence_threshold=3,
        k_value=0.5,
    )

    expected_k_density = np.log1p(10000) / np.log1p(60000)
    expected_k = 0.32 + expected_k_density * (0.92 - 0.32)
    expected_buffer = 10000 * expected_k

    assert result == pytest.approx(np.array([expected_buffer]))


def test_calculate_buffer_uses_mean_2_nnd_when_nnd_length_mismatches(monkeypatch):
    df = gpd.GeoDataFrame(
        {
            "geometry": [Point(0, 0), Point(1, 1)],
            "mean_2_nnd": [4000.0, 9000.0],
            "num_detection_circle": [0, 0],
            "num_detection_rect": [0, 0],
            "basin_area": [1000.0, 1000.0],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    monkeypatch.setattr(
        create_voronoi,
        "nearest_neighbor_distances_and_median",
        lambda input_df: (np.array([1.0]), 1.0),
    )

    result = create_voronoi.calculate_buffer(
        df,
        weights=np.array([0.5, 0.5]),
        buffer=7000,
        dynamic_buffering=True,
        min_buffer=1000,
        max_buffer=20000,
        k_min=0.32,
        k_max=0.92,
        detection_confidence_threshold=3,
        k_value=0.5,
    )

    expected = []
    for nnd in [4000.0, 9000.0]:
        k_density = np.log1p(nnd) / np.log1p(60000)
        k = 0.32 + k_density * (0.92 - 0.32)
        expected.append(nnd * k)

    assert result == pytest.approx(np.array(expected))


def test_filter_requested_approaches_skips_city_when_disabled(monkeypatch):
    monkeypatch.setattr(create_voronoi.os.path, "exists", lambda path: False)

    runnable, skipped_existing, skipped_disabled = create_voronoi._filter_requested_approaches(
        ["0", "2"],
        {"city_voronoi": False, "voronoi_overwrite": False},
        {
            "voronoi": {
                "0": "appr_0.gpkg",
                "1": "appr_1.gpkg",
                "2": "appr_2.gpkg",
                "0_only_round": "appr_0_only_round.gpkg",
                "1_only_round": "appr_1_only_round.gpkg",
            }
        },
        only_round=False,
    )

    assert runnable == ["0"]
    assert skipped_existing == []
    assert skipped_disabled == ["2"]


def test_filter_requested_approaches_keeps_city_when_enabled_and_skips_existing(monkeypatch):
    monkeypatch.setattr(
        create_voronoi.os.path,
        "exists",
        lambda path: path == "appr_1_only_round.gpkg",
    )

    runnable, skipped_existing, skipped_disabled = create_voronoi._filter_requested_approaches(
        ["1", "2"],
        {"city_voronoi": True, "voronoi_overwrite": False},
        {
            "voronoi": {
                "0": "appr_0.gpkg",
                "1": "appr_1.gpkg",
                "2": "appr_2.gpkg",
                "0_only_round": "appr_0_only_round.gpkg",
                "1_only_round": "appr_1_only_round.gpkg",
            }
        },
        only_round=True,
    )

    assert runnable == ["2"]
    assert skipped_existing == ["1"]
    assert skipped_disabled == []


def test_weighted_voronoi_returns_none_for_empty_input(tiny_country_gdf):
    empty = gpd.GeoDataFrame({"WASTE_ID": [], "HYBAS_ID": [], "weights": []}, geometry=[], crs="EPSG:4326")

    result = create_voronoi.weighted_voronoi(
        empty,
        "HYBAS_ID",
        tiny_country_gdf,
        n_points=20,
        calculate_buffer_fn=lambda df, weights, **kwargs: np.array([]),
        site_id_col="WASTE_ID",
    )

    assert result is None


def test_weighted_voronoi_raises_when_site_id_column_is_missing(tiny_country_gdf):
    df = gpd.GeoDataFrame(
        {
            "HYBAS_ID": [101],
            "weights": [1.0],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(KeyError, match="Site identifier column 'WASTE_ID'"):
        create_voronoi.weighted_voronoi(
            df,
            "HYBAS_ID",
            tiny_country_gdf,
            n_points=20,
            calculate_buffer_fn=lambda sub_df, weights, **kwargs: np.array([1000.0]),
            site_id_col="WASTE_ID",
        )


def test_weighted_voronoi_returns_none_when_utm_estimation_fails(monkeypatch, tiny_country_gdf):
    df = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            "HYBAS_ID": [101],
            "weights": [1.0],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs=None,
    )

    monkeypatch.setattr(create_voronoi, "estimate_utm_crs", lambda gdf: None)

    result = create_voronoi.weighted_voronoi(
        df,
        "HYBAS_ID",
        tiny_country_gdf,
        n_points=20,
        calculate_buffer_fn=lambda sub_df, weights, **kwargs: np.array([1000.0]),
        site_id_col="WASTE_ID",
    )

    assert result is None


def test_weighted_voronoi_rejects_calculate_buffer_length_mismatch(tiny_country_gdf):
    df = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1, 2],
            "HYBAS_ID": [101, 101],
            "weights": [0.5, 0.5],
            "geometry": [Point(0, 0), Point(1, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )

    with pytest.raises(ValueError, match="buffer lengths"):
        create_voronoi.weighted_voronoi(
            df,
            "HYBAS_ID",
            tiny_country_gdf,
            threshold=0.1,
            n_points=20,
            calculate_buffer_fn=lambda sub_df, weights, **kwargs: np.array([1000.0]),
            site_id_col="WASTE_ID",
        )


def test_weighted_voronoi_single_site_clips_to_country_boundary():
    site = gpd.GeoDataFrame(
        {
            "WASTE_ID": [1],
            "HYBAS_ID": [101],
            "weights": [1.0],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:3857",
    )
    country_clip = gpd.GeoDataFrame(
        {"country": ["DE"], "geometry": [box(-100, -100, 100, 100)]},
        geometry="geometry",
        crs="EPSG:3857",
    )

    region_df, point_df = create_voronoi.weighted_voronoi(
        site,
        "HYBAS_ID",
        country_clip,
        clipping=None,
        n_points=30,
        buffering=False,
        calculate_buffer_fn=lambda sub_df, weights, **kwargs: np.array([500.0]),
        site_id_col="WASTE_ID",
    )

    assert len(region_df) == 1
    assert len(point_df) == 1
    assert region_df.crs.to_epsg() == 4326
    assert point_df.crs.to_epsg() == 4326

    country_clip_wgs84 = country_clip.to_crs(4326)
    assert region_df.geometry.iloc[0].difference(country_clip_wgs84.geometry.iloc[0]).area == pytest.approx(0.0, abs=1e-9)