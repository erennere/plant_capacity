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
    from ..starter import load_config, parse_config_overrides
    from ..create_voronoi import duckdb_intersect, intersect_watershed_sindex, download_overture_maps
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.create_voronoi import duckdb_intersect, intersect_watershed_sindex, download_overture_maps

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


def vectorize_raster_file(raster_path: str, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """
    Vectorize a single raster file to polygons.
    
    Parameters
    ----------
    raster_path : str
        Path to the raster file.
    crs : str
        Coordinate reference system for output.
    
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
        
        # Extract shapes from raster (convert pixels to polygons)
        for geom, value in shapes(data, transform=transform):
            if value > 0:  # Only keep non-zero pixels
                polygons.append({
                    'geometry': shape(geom),
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
    crs: str = "EPSG:4326"
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
            executor.submit(vectorize_raster_file, str(raster_file), crs)
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


def merge_geodataframes(gdfs: List[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """Merge multiple GeoDataFrames and dissolve overlaps."""
    if not gdfs:
        raise ValueError("No geodataframes to merge")
    
    logger.info(f"Merging {len(gdfs)} geodataframes...")
    merged = pd.concat(gdfs, ignore_index=True)
    
    logger.info("Dissolving overlapping geometries...")
    # Dissolve all geometries into a single multi-part geometry
    merged_geom = unary_union(merged.geometry)
    
    result = gpd.GeoDataFrame(
        {'geometry': [merged_geom], 'category': ['industrial_land']},
        crs=merged.crs
    )
    
    logger.info(f"Merged geometry with {len(result)} feature(s)")
    return result


def add_boundary_info(
    industrial_gdf: gpd.GeoDataFrame,
    watershed_gdf: gpd.GeoDataFrame,
    overture_path: str,
    overture_s3_url: str,
    basin_col: str,
    sindex_concurrency: bool,
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

    logger.info("Adding ISO_2 via create_voronoi.duckdb_intersect...")
    enriched = duckdb_intersect(enriched, overture_path)

    logger.info(f"Adding basin info via create_voronoi.intersect_watershed_sindex ({basin_col})...")
    if enriched.crs != watershed_gdf.crs:
        watershed_gdf = watershed_gdf.to_crs(enriched.crs)
    enriched = intersect_watershed_sindex(
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


def main():
    """Download, vectorize, and merge industrial land data."""
    overrides = parse_config_overrides(args=None, argv=None, start_index=1)
    cfg = load_config(**overrides)
    
    output_path = cfg['paths']['industrial_merged_gpkg']
    overwrite = cfg.get('industrial_vectorize_overwrite', False)
    
    # Check if output exists and overwrite is disabled
    if os.path.exists(output_path) and not overwrite:
        logger.info(f"Output file exists and overwrite=false. Skipping vectorization.")
        return True
    
    # Create temp directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = os.path.join(temp_dir, "industrial_land.zip")
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Download
            download_file(cfg['industrial_zenodo_url'], zip_path)
            
            # Unzip
            logger.info(f"Extracting to {extract_dir}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Find raster directories (can be multiple nested dirs)
            raster_dirs = []
            for root, dirs, files in os.walk(extract_dir):
                if any(f.lower().endswith((".tif", ".tiff")) for f in files):
                    raster_dirs.append(root)

            if not raster_dirs:
                raise FileNotFoundError("No raster files found in extracted archive")

            logger.info(f"Found {len(raster_dirs)} raster directories")

            # Vectorize all raster directories and concatenate results
            gdfs = []
            for raster_dir in raster_dirs:
                logger.info(f"Vectorizing rasters in directory: {raster_dir}")
                gdfs.extend(
                    vectorize_rasters_parallel(
                        raster_dir,
                        max_workers=cfg['max_workers'],
                        crs="EPSG:4326"
                    )
                )
            
            if not gdfs:
                raise ValueError("Failed to vectorize any raster files")
            
            # Merge
            merged_gdf = merge_geodataframes(gdfs)
            
            # Load watershed geometry for basin attribution
            logger.info("Loading watershed data...")
            watershed_gdf = gpd.read_file(
                cfg['paths']['watershed'],
                driver='GPKG'
            )

            # Add country and watershed attributes without modifying geometry
            enriched_gdf = add_boundary_info(
                merged_gdf,
                watershed_gdf,
                cfg['paths']['overture'],
                cfg['paths']['overture_s3_url'],
                cfg.get('basin_column_name', 'HYBAS_ID'),
                cfg.get('sindex_concurrency', False),
            )
            
            # Save to GeoPackage
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            if os.path.exists(output_path):
                os.remove(output_path)
            
            logger.info(f"Writing to {output_path}...")
            enriched_gdf.to_file(output_path, driver='GPKG', index=False)
            logger.info(f"Successfully created {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during vectorization: {e}", exc_info=True)
            return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
