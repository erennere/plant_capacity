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
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests
import geopandas as gpd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape
from shapely.ops import unary_union
import pandas as pd

try:
    from shapely import make_valid
except ImportError:
    make_valid = None

try:
    from ..starter import load_config, parse_config_overrides
    from ..create_voronoi import intersects_with_country_db, intersect_with_polygon_sindex, download_overture_maps
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.create_voronoi import intersects_with_country_db, intersect_with_polygon_sindex, download_overture_maps

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
            if value > 0:  # Only keep non-zero pixels
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


def merge_geodataframes(gdfs: List[gpd.GeoDataFrame], simplify_tolerance: float = 0.01) -> gpd.GeoDataFrame:
    """Merge multiple GeoDataFrames, dissolve overlaps, and explode to individual features.
    
    Parameters
    ----------
    gdfs : List[gpd.GeoDataFrame]
        List of geodataframes to merge.
    simplify_tolerance : float
        Tolerance (in degrees) for geometry simplification. Default 0.01 (~1.1km at equator).
        Increase if GPKG blob size errors persist. Set to None to skip simplification.
    
    Returns
    -------
    gpd.GeoDataFrame
        Merged and exploded geodataframe with individual polygons.
    """
    if not gdfs:
        raise ValueError("No geodataframes to merge")
    
    logger.info(f"Merging {len(gdfs)} geodataframes...")
    merged = pd.concat(gdfs, ignore_index=True)

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

    logger.info("Dissolving overlapping geometries with unary_union...")

    # Dissolve all geometries into a single union (may be MultiPolygon)
    merged_geom = unary_union(merged.geometry)
    
    # Create a temporary GeoDataFrame with the merged geometry
    temp_gdf = gpd.GeoDataFrame(
        {'geometry': [merged_geom], 'category': ['industrial_land']},
        crs=merged.crs
    )
    
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
    return merge_geodataframes(gdfs, simplify_tolerance=simplify_tolerance)


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
            if os.path.exists(vectorized_path):
                os.remove(vectorized_path)
            logger.info(f"Saving vectorized polygons to {vectorized_path}...")
            if merged_gdf is None:
                raise ValueError("Vectorization did not produce any industrial polygons")
            merged_gdf.to_parquet(vectorized_path, index=False)
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

        if os.path.exists(vectorized_path):
            os.remove(vectorized_path)
        logger.info(f"Writing to {vectorized_path}...")
        enriched_gdf.to_parquet(vectorized_path, index=False)
        logger.info(f"Successfully created {vectorized_path}")
        return True

    except Exception as e:
        logger.error(f"Error during vectorization: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
