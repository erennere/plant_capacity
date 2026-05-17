#!/usr/bin/env python
"""
Download, vectorize, and merge industrial land-use raster data.

This script downloads industrial land classification rasters from Zenodo,
vectorizes them (converts rasters to polygons), merges all geometries, clips
to watershed and country boundaries, and saves the result as a GeoPackage.

Usage:
    python -m research_code.industrial_analysis.download_and_vectorize [level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
"""

import sys
import os
import logging
import tempfile
import zipfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import List

import requests
import geopandas as gpd
import rasterio
from rasterio.features import shapes
from shapely.geometry import Point, shape
from shapely.ops import unary_union
import pandas as pd

try:
    from shapely import make_valid
except ImportError:
    make_valid = None

try:
    from ..starter import load_config, parse_config_overrides
    from ..create_voronoi import (
        dissolve_overlapping_geometries_fast,
        download_overture_maps,
        estimate_utm_epsg,
        intersects_with_country_db,
        intersect_with_polygon_sindex,
    )
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.create_voronoi import (
        dissolve_overlapping_geometries_fast,
        download_overture_maps,
        estimate_utm_epsg,
        intersects_with_country_db,
        intersect_with_polygon_sindex,
    )

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)


def download_file(url: str, dest_path: str, chunk_size: int = 8192) -> None:
    """Download a file from URL with progress tracking."""
    logger.info(f"Downloading from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    if int(percent) % 10 == 0:
                        logger.debug(f"Downloaded {percent:.1f}%")
    
    logger.info(f"Downloaded to {dest_path}")


def vectorize_raster_file(raster_path: str, crs: str = "EPSG:4326", min_cells: int = 100) -> gpd.GeoDataFrame:
    """
    Vectorize a single raster file to polygons.
    
    Parameters
    ----------
    raster_path : str
        Path to the raster file.
    crs : str
        Coordinate reference system for output.
    min_cells : int
        Minimum number of connected pixels a polygon must cover to be retained.
        At 10 m resolution, 100 cells ≈ 1 ha.
    
    Returns
    -------
    geopandas.GeoDataFrame
        Vectorized polygons with values.
    """
    logger.info(f"Vectorizing {Path(raster_path).name}...")
    
    polygons = []
    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        data = src.read(1)
        transform = src.transform
        
        # Minimum area filter: one pixel = |dx| * |dy| in raster native units
        pixel_area = abs(transform.a) * abs(transform.e)
        min_area = min_cells * pixel_area

        # Extract shapes from raster (convert pixels to polygons)
        for geom, value in shapes(data, transform=transform):
            if value == 1:  # Keep only class-1 industrial cells
                geom_shape = shape(geom)
                if geom_shape.area < min_area:
                    continue
                polygons.append({
                    'geometry': geom_shape,
                    'value': value
                })
    
    if not polygons:
        logger.warning(f"No valid polygons found in {raster_path}")
        return gpd.GeoDataFrame({'geometry': [], 'value': []}, crs=raster_crs or crs)
    
    gdf = gpd.GeoDataFrame(polygons, crs=raster_crs or crs)
    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)
    
    logger.info(f"Vectorized {len(gdf)} polygons from {Path(raster_path).name}")
    return gdf


def vectorize_rasters_parallel(
    raster_dir: str,
    max_workers: int = 8,
    crs: str = "EPSG:4326",
    min_cells: int = 100,
) -> List[gpd.GeoDataFrame]:
    """
    Vectorize multiple raster files in parallel.
    
    Parameters
    ----------
    raster_dir : str
        Directory containing raster files.
    max_workers : int
        Number of parallel workers.
    crs : str
        Coordinate reference system.
    
    Returns
    -------
    list of geopandas.GeoDataFrame
        Vectorized geodataframes.
    """
    if int(max_workers) < 1:
        raise ValueError("max_workers must be >= 1")

    raster_files = list(Path(raster_dir).glob("*.tif")) + list(Path(raster_dir).glob("*.tiff"))
    
    if not raster_files:
        logger.warning(f"No raster files found in {raster_dir}")
        return []
    
    logger.info(f"Vectorizing {len(raster_files)} raster files with {max_workers} workers...")
    
    gdfs = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(vectorize_raster_file, str(raster_file), crs, min_cells)
            for raster_file in raster_files
        ]
        
        for idx, future in enumerate(futures, 1):
            try:
                gdf = future.result()
                gdfs.append(gdf)
                logger.info(f"Completed {idx}/{len(futures)} rasters")
            except Exception as e:
                logger.error(f"Error vectorizing raster: {e}")
    
    return gdfs


def _repair_geometry(geom):
    """Repair invalid geometries before overlay operations."""
    if geom is None or geom.is_empty:
        return None
    if geom.is_valid:
        return geom
    if make_valid is not None:
        repaired = make_valid(geom)
    else:
        repaired = geom.buffer(0)
    if repaired is None or repaired.is_empty:
        return None
    if not repaired.is_valid:
        repaired = repaired.buffer(0)
    if repaired.is_empty:
        return None
    return repaired


def _dissolve_by_overlap_groups(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Dissolve original geometries using fast overlap groups from create_voronoi."""
    if gdf is None or gdf.empty:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=getattr(gdf, "crs", None))

    working = gdf.reset_index(drop=True).copy()
    working["some_id"] = working.index.astype(int)

    overlap_groups, _ = dissolve_overlapping_geometries_fast(
        working[["some_id", "geometry"]].copy(),
        radius=0,
        convex=True,
    )

    group_map = {}
    for group_index, group in enumerate(overlap_groups):
        for some_id in group:
            group_map[int(some_id)] = group_index

    working["group_id"] = working["some_id"].map(group_map)
    missing_mask = working["group_id"].isna()
    if missing_mask.any():
        start = len(overlap_groups)
        working.loc[missing_mask, "group_id"] = list(range(start, start + int(missing_mask.sum())))
    working["group_id"] = working["group_id"].astype(int)

    dissolved = working[["group_id", "geometry"]].dissolve(by="group_id").reset_index(drop=True)
    dissolved["geometry"] = dissolved.geometry.map(_repair_geometry)
    dissolved = dissolved[dissolved.geometry.notna() & ~dissolved.geometry.is_empty].copy()
    return gpd.GeoDataFrame(dissolved, geometry="geometry", crs=gdf.crs)


def merge_geodataframes(
    gdfs: List[gpd.GeoDataFrame],
    simplify_tolerance: float = 0.01,
    max_workers: int = 8,
) -> gpd.GeoDataFrame:
    """Merge multiple GeoDataFrames, dissolve overlaps, and explode to individual features.
    
    Parameters
    ----------
    gdfs : List[gpd.GeoDataFrame]
        List of geodataframes to merge.
    simplify_tolerance : float
        Tolerance (in degrees) for geometry simplification. Default 0.01 (~1.1km at equator).
        Increase if GPKG blob size errors persist. Set to None to skip simplification.
    max_workers : int
        Number of worker processes used for per-UTM dissolve tasks.
    
    Returns
    -------
    gpd.GeoDataFrame
        Merged and exploded geodataframe with individual polygons.
    """
    if not gdfs:
        raise ValueError("No geodataframes to merge")
    
    logger.info(f"Merging {len(gdfs)} geodataframes...")
    target_crs = gdfs[0].crs or "EPSG:4326"
    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), geometry="geometry", crs=target_crs)

    logger.info(f"Concatenated {len(merged)} total polygons")

    logger.info("Repairing invalid geometries before dissolve...")
    merged["geometry"] = merged.geometry.map(_repair_geometry)
    merged = merged[merged.geometry.notna() & ~merged.geometry.is_empty].copy()
    logger.info(f"Retained {len(merged)} valid polygons after repair")
    if merged.empty:
        raise ValueError("No valid industrial polygons remain after geometry repair")

    # Simplify early to reduce complexity before dissolve
    if simplify_tolerance is not None:
        logger.info(f"Simplifying geometries (early pass) with tolerance {simplify_tolerance}...")
        merged["geometry"] = merged.geometry.simplify(tolerance=simplify_tolerance, preserve_topology=True)
        logger.info("Repairing geometries after simplification...")
        merged["geometry"] = merged.geometry.map(_repair_geometry)
        merged = merged[merged.geometry.notna() & ~merged.geometry.is_empty].copy()
        logger.info(f"Retained {len(merged)} valid polygons after simplification")
        if merged.empty:
            raise ValueError("No valid industrial polygons remain after simplification")

    if merged.crs is None:
        merged = merged.set_crs("EPSG:4326")

    merged_wgs84 = merged if merged.crs is not None and merged.crs.to_epsg() == 4326 else merged.to_crs(4326)
    try:
        fallback_utm = merged_wgs84.estimate_utm_crs()
        fallback_epsg = fallback_utm.to_epsg() if fallback_utm is not None else None
    except Exception as err:
        logger.warning("Failed to estimate fallback UTM CRS: %s", err)
        fallback_epsg = None
    fallback_epsg = fallback_epsg or 3857

    logger.info("Dissolving overlapping geometries by UTM group with spatial indexing...")
    try:
        merged_wgs84["utm_group"] = merged_wgs84.apply(
            lambda row: estimate_utm_epsg(row["geometry"].x, row["geometry"].y)
            if isinstance(row["geometry"], Point)
            else estimate_utm_epsg(row["geometry"].centroid.x, row["geometry"].centroid.y),
            axis=1,
        )
    except Exception as err:
        logger.warning("Failed to assign per-geometry UTM groups: %s. Using fallback EPSG %s.", err, fallback_epsg)
        merged_wgs84["utm_group"] = fallback_epsg

    utm_group_count = merged_wgs84["utm_group"].nunique(dropna=False)
    grouped_frames = (
        gpd.GeoDataFrame(subdf.copy(), geometry="geometry", crs=merged_wgs84.crs)
        for _, subdf in merged_wgs84.groupby("utm_group", sort=False)
    )
    if max_workers is None:
        pool_size = os.cpu_count() or 1
    else:
        try:
            pool_size = int(max_workers)
        except (TypeError, ValueError) as err:
            raise ValueError("max_workers must be a positive integer or None") from err
    pool_size = max(1, pool_size)
    logger.info("Dispatching %d UTM dissolve task(s) with %d worker(s)", utm_group_count, pool_size)
    with ProcessPoolExecutor(max_workers=pool_size) as executor:
        dissolved_groups = tuple(
            filter(
                lambda frame: frame is not None and not frame.empty,
                executor.map(_dissolve_by_overlap_groups, grouped_frames),
            )
        )

    if not dissolved_groups:
        raise ValueError("No valid industrial polygons remain after overlap dissolve")

    merged_reduced = gpd.GeoDataFrame(
        pd.concat(dissolved_groups, ignore_index=True),
        geometry="geometry",
        crs=merged_wgs84.crs,
    )

    logger.info("Running final union on reduced geometry set...")
    merged_geom = unary_union(merged_reduced.geometry)
    
    # Create a temporary GeoDataFrame with the merged geometry
    temp_gdf = gpd.GeoDataFrame(
        {'geometry': [merged_geom], 'category': ['industrial_land']},
        crs=merged_reduced.crs
    )

    if target_crs is not None and temp_gdf.crs != target_crs:
        temp_gdf = temp_gdf.to_crs(target_crs)
    
    # Explode MultiPolygon/MultiPart geometries into individual parts
    logger.info("Exploding multipart geometries into individual features...")
    result = temp_gdf.explode(index_parts=False, ignore_index=True)
    
    logger.info(f"Merged and exploded GeoDataFrame with {len(result)} feature(s)")
    return result


def add_boundary_info(
    industrial_gdf: gpd.GeoDataFrame,
    watershed_gdf: gpd.GeoDataFrame,
    overture_path: str,
    overture_s3_url: str,
    basin_col: str,
    sindex_concurrency: bool,
    country_boundary_col: str = 'country',
    country_output_col: str = 'ISO_2',
) -> gpd.GeoDataFrame:
    """Add country and basin attributes without clipping industrial geometry."""
    if industrial_gdf is None or industrial_gdf.empty:
        empty = industrial_gdf.copy() if industrial_gdf is not None else gpd.GeoDataFrame(geometry=[], crs=getattr(watershed_gdf, "crs", None))
        if country_output_col not in empty.columns:
            empty[country_output_col] = pd.Series(dtype="object")
        if basin_col not in empty.columns:
            empty[basin_col] = pd.Series(dtype="object")
        return gpd.GeoDataFrame(empty, geometry="geometry", crs=getattr(empty, "crs", None))

    if basin_col not in watershed_gdf.columns:
        raise KeyError(f"Watershed column '{basin_col}' not found in watershed dataset.")

    if not os.path.exists(overture_path):
        logger.info("Overture boundary parquet not found. Downloading...")
        download_overture_maps(overture_s3_url, overture_path)

    # Work on a copy and preserve original geometry so helper side-effects
    # do not alter output shapes.
    enriched = industrial_gdf.reset_index(drop=True).copy()
    enriched["__row_id"] = enriched.index.astype(int)
    original_geometry = enriched[["__row_id", "geometry"]].rename(columns={"geometry": "_original_geometry"})

    logger.info("Adding %s via create_voronoi.intersects_with_country_db...", country_output_col)
    enriched = intersects_with_country_db(
        enriched,
        overture_path,
        polygon_country_col=country_boundary_col,
        output_country_col=country_output_col,
    )

    logger.info(f"Adding basin info via create_voronoi.intersect_with_polygon_sindex ({basin_col})...")
    if enriched.crs is not None and enriched.crs != watershed_gdf.crs:
        watershed_gdf = watershed_gdf.to_crs(enriched.crs)
    enriched = intersect_with_polygon_sindex(
        enriched,
        watershed_gdf,
        basin_col,
        concurrency=sindex_concurrency,
    )

    # Restore original shapes and row order.
    enriched = enriched.drop(columns=["geometry"]).merge(original_geometry, on="__row_id", how="left")
    enriched = enriched.sort_values("__row_id").reset_index(drop=True)
    enriched = enriched.rename(columns={"_original_geometry": "geometry"})
    enriched = enriched.drop(columns=["__row_id"], errors="ignore")
    return gpd.GeoDataFrame(enriched, geometry="geometry", crs=industrial_gdf.crs)


def _find_raster_dirs(base_dir: str) -> List[str]:
    """Walk *base_dir* and return every subdirectory that contains .tif/.tiff files."""
    raster_dirs = []
    for root, _dirs, files in os.walk(base_dir):
        if any(f.lower().endswith((".tif", ".tiff")) for f in files):
            raster_dirs.append(root)
    return raster_dirs


def _vectorize_and_merge(raster_dirs: List[str], max_workers: int, min_cells: int, simplify_tolerance: float = 0.01) -> gpd.GeoDataFrame:
    """Vectorize all rasters in *raster_dirs* and dissolve into a single GeoDataFrame."""
    if not raster_dirs:
        raise FileNotFoundError("No raster directories found")
    gdfs = []
    for raster_dir in raster_dirs:
        logger.info(f"Vectorizing rasters in directory: {raster_dir}")
        gdfs.extend(
            vectorize_rasters_parallel(raster_dir, max_workers=max_workers, crs="EPSG:4326", min_cells=min_cells)
        )
    if not gdfs:
        raise ValueError("Failed to vectorize any raster files")
    return merge_geodataframes(gdfs, simplify_tolerance=simplify_tolerance, max_workers=max_workers)


def main():
    """Download, vectorize, and merge industrial land data."""
    overrides = parse_config_overrides(args=None, argv=None, start_index=1)
    cfg = load_config(**overrides)

    vectorized_path = cfg['paths']['industrial_merged_filepath']
    overwrite = cfg['industrial_vectorize_overwrite']
    min_cells = cfg['industrial_min_cells']
    persist_rasters = cfg['industrial_persist_rasters']
    simplify_tolerance = cfg['industrial_simplify_tolerance']

    # Intermediate file: merged vectorized polygons (pre-enrichment).
    # Named after min_cells so different thresholds don't clobber each other.
    industrial_analysis_dir = os.path.dirname(vectorized_path)
    merged_gdf = None

    try:
        # ------------------------------------------------------------------ #
        # Step 1 – obtain merged_gdf (from cache or fresh vectorization)      #
        # ------------------------------------------------------------------ #
        if os.path.exists(vectorized_path) and not overwrite:
            logger.info(f"Loading cached vectorized polygons from {vectorized_path}")
            merged_gdf = gpd.read_parquet(vectorized_path)
        else:
            if persist_rasters:
                # Rasters are kept on disk so future runs can skip download.
                raster_base_dir = cfg['paths']['industrial_raster_persistent_dir']
                os.makedirs(raster_base_dir, exist_ok=True)
                existing = (
                    list(Path(raster_base_dir).rglob("*.tif"))
                    + list(Path(raster_base_dir).rglob("*.tiff"))
                )
                if existing and not overwrite:
                    logger.info(f"Reusing {len(existing)} existing raster(s) in {raster_base_dir}")
                else:
                    zip_path = os.path.join(raster_base_dir, "industrial_land.zip")
                    download_file(cfg['industrial_zenodo_url'], zip_path)
                    logger.info(f"Extracting to {raster_base_dir}...")
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(raster_base_dir)
                raster_dirs = _find_raster_dirs(raster_base_dir)
            else:
                # Use a temporary directory; rasters are discarded after vectorization.
                with tempfile.TemporaryDirectory() as temp_dir:
                    zip_path = os.path.join(temp_dir, "industrial_land.zip")
                    extract_dir = os.path.join(temp_dir, "extracted")
                    os.makedirs(extract_dir, exist_ok=True)
                    download_file(cfg['industrial_zenodo_url'], zip_path)
                    logger.info(f"Extracting to {extract_dir}...")
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    raster_dirs = _find_raster_dirs(extract_dir)
                    merged_gdf = _vectorize_and_merge(raster_dirs, cfg['max_workers'], min_cells, simplify_tolerance)

            if persist_rasters:
                # _vectorize_and_merge is called outside the with-block for the
                # persistent-dir branch so the rasters remain accessible.
                merged_gdf = _vectorize_and_merge(raster_dirs, cfg['max_workers'], min_cells, simplify_tolerance)

            # Save intermediate result so boundary enrichment can be re-run
            # independently without repeating download + vectorization.
            os.makedirs(industrial_analysis_dir, exist_ok=True)
            logger.info(f"Saving vectorized polygons to {vectorized_path}...")
            if merged_gdf is None:
                raise ValueError("Vectorization did not produce any industrial polygons")
            vectorized_tmp_path = f"{vectorized_path}.tmp"
            if os.path.exists(vectorized_tmp_path):
                os.remove(vectorized_tmp_path)
            merged_gdf.to_parquet(vectorized_tmp_path, index=False)
            os.replace(vectorized_tmp_path, vectorized_path)
            logger.info(f"Saved {len(merged_gdf)} feature(s) to {vectorized_path}")

        # ------------------------------------------------------------------ #
        # Step 2 – enrich with country / basin boundaries                     #
        # ------------------------------------------------------------------ #
        logger.info("Loading watershed data...")
        watershed_gdf = gpd.read_file(cfg['paths']['watershed'], driver='GPKG')

        enriched_gdf = add_boundary_info(
            merged_gdf,
            watershed_gdf,
            cfg['paths']['overture'],
            cfg['paths']['overture_s3_url'],
            cfg['basin_column_name'],
            cfg['sindex_concurrency'],
            cfg['country_boundary_column'],
            cfg['country_output_column'],
        )

        logger.info(f"Writing to {vectorized_path}...")
        enriched_tmp_path = f"{vectorized_path}.tmp"
        if os.path.exists(enriched_tmp_path):
            os.remove(enriched_tmp_path)
        enriched_gdf.to_parquet(enriched_tmp_path, index=False)
        os.replace(enriched_tmp_path, vectorized_path)
        logger.info(f"Successfully created {vectorized_path}")
        return True

    except Exception as e:
        logger.error(f"Error during vectorization: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
