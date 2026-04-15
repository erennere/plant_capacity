import os, sys
import argparse
import math
import random
import requests
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from concurrent.futures import ThreadPoolExecutor, as_completed
from shapely import from_wkt
from pyproj import Transformer
import duckdb
import logging
import shapely.wkt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------- CONFIG ---------------- #
def safe_wkt_load(wkt_wtr):
    """Cleans hex string and converts to Shapely geometry."""
    try:
        if not wkt_wtr or not isinstance(wkt_wtr, str):
            return None
        return shapely.wkt.loads(wkt_wtr)
    except Exception as e:
        # If a specific row is broken, we skip it rather than crashing the script
        return None
    
BING_API_KEY = "de60014913a0464f83eec298da249356"
RESOLUTIONS = { 
    1 :	78271.52,   
    2 :	39135.76,	
    3 :	19567.88,	
    4 :	9783.94,	
    5 : 4891.97,	
    6 :	2445.98,	
    7 :	1222.99,	
    8 :	611.50,	
    9 :	305.75,	
    10 : 152.87,	
    11 : 76.44,	
    12 : 38.22,
    13 : 19.11,
    14 : 9.55,
    15 : 4.78,
    16 : 2.39,
    17 : 1.19,
    18 : 0.60,
    19 : 0.30
}
ZOOM_LEVEL = 17
RES_X = RESOLUTIONS[ZOOM_LEVEL]
RES_Y = RESOLUTIONS[ZOOM_LEVEL]
CELL_SIZE = 3072
FACTOR = 1.194
#IMAGE_SIZE = [int(CELL_SIZE*FACTOR/RES_X), int(CELL_SIZE*FACTOR/RES_Y)]
IMAGE_SIZE = [3072, 3072]
MAX_WORKERS = 64
GEOREFERENCED = False
FONTSIZE = 24
DPI = 72 
EARTH_RADIUS = 6378137
WORLD_WIDTH = 2 * math.pi * EARTH_RADIUS  # ~40075016.685
TARGET_SIZE = [1024, 1024]

# -------------------------------------- #
transformer = Transformer.from_crs(
    "EPSG:4326", "EPSG:3857", always_xy=True
)

# ---------- BING IMAGE DOWNLOAD ---------- #

def download_bing_image(center_lon, center_lat):
    url = (
        "https://dev.virtualearth.net/REST/v1/Imagery/Map/Aerial"
        f"/{center_lat},{center_lon}"
        f"/{ZOOM_LEVEL}"
        f"?mapSize={IMAGE_SIZE[0]},{IMAGE_SIZE[1]}"
        f"&key={BING_API_KEY}"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return Image.open(BytesIO(r.content)).convert("RGB")

def download_random_image(center_lon, center_lat):
    """
    Dummy image generator for testing.
    Returns a solid black image with the same dimensions
    as the Bing imagery would have.
    """
    return Image.new("RGB", IMAGE_SIZE, (0, 0, 0))

def get_image(idx, images_dir):
    filepath = os.path.join(images_dir, f'{idx}.png')
    if os.path.exists(filepath):
        return Image.open(filepath)
    else:
        return None

# ---------- COORD TRANSFORM ---------- #

def mercator_to_pixel(x, y, cx, cy, IMAGE_SIZE, wrap=True):
    """
    Specifically tuned for Maxar Zoom 17 with a 3072px buffer.
    x, y: Target Web Mercator coordinates (meters)
    cx, cy: Center Web Mercator coordinates of the 3072px image
    """
    # 1. Standard Zoom 17 resolution (meters/pixel for a 256px tile)
    # At Z17, a 256px tile covers ~305.75 meters.
    # At Z17, a 512px tile covers ~611.5 meters.
    # Your 3072px image covers ~3669 meters.
    
    BASE_Z17_RES = 1.1943285669555664 
    
    # 2. Adjust resolution for your specific image size.
    # Since Maxar Zoom 17 typically refers to the 512px tile scale, 
    # we use 512 as the divisor if your imagery was pulled as 512px tiles.
    # However, standard Web Mercator math uses 256 as the base unit:
    res = BASE_Z17_RES * (256 / 512) # This gives ~0.597 m/px (Native Maxar Z17)
    
    # 3. Calculate distance from center
    dx = x - cx
    dy = cy - y  # Invert Y: Mercator North is (+), Pixel Down is (+)

    # 4. Handle World Wrap (The "International Date Line" logic)
    if wrap:
        WORLD_CIRCUMFERENCE = 40075016.68557849
        half_world = WORLD_CIRCUMFERENCE / 2
        if dx > half_world: dx -= WORLD_CIRCUMFERENCE
        elif dx < -half_world: dx += WORLD_CIRCUMFERENCE

    # 5. Map to Pixel Space
    # (Distance / Resolution) + (Center of the 3072px frame)
    px = (dx / res) + (IMAGE_SIZE[0] / 2)
    py = (dy / res) + (IMAGE_SIZE[1] / 2)

    return int(round(px)), int(round(py))

def image_bounds_mercator(center_lon, center_lat):
    cx, cy = transformer.transform(center_lon, center_lat)
    cx, cy = center_lon, center_lat

    initial_res = 2 * math.pi * 6378137 / 256
    res = initial_res / (2 ** ZOOM_LEVEL)

    half_w = IMAGE_SIZE[0] * res / 2
    half_h = IMAGE_SIZE[1] * res / 2

    xmin = cx - half_w
    xmax = cx + half_w
    ymin = cy - half_h
    ymax = cy + half_h

    return xmin, ymin, xmax, ymax, res

# -------------------- TEXT HELPERS --------------------
def draw_text_with_padding(draw, xy, text, font, fill, pad_fill, pad=2):
    """Draws centered text with a stroke/halo outline."""
    draw.text(
        xy, text, font=font, fill=fill,
        anchor="mm", stroke_width=pad, stroke_fill=pad_fill
    )

def draw_rotated_text_with_padding(image, xy, text, angle, font, fill, pad_fill, pad=2):
    """Draws rotated text by compositing a small scratchpad onto the main image."""
    # Create a small scratchpad based on text size
    bbox = font.getbbox(text)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    
    # Square buffer to allow rotation without clipping
    pad_dim = int(max(w, h) * 2.0) 
    txt_img = Image.new("RGBA", (pad_dim, pad_dim), (0, 0, 0, 0))
    txt_draw = ImageDraw.Draw(txt_img)

    center_pt = (pad_dim // 2, pad_dim // 2)
    txt_draw.text(
        center_pt, text, font=font, fill=fill,
        anchor="mm", stroke_width=pad, stroke_fill=pad_fill
    )

    # Note: If labels look 'mirrored', change angle to -angle
    rotated_txt = txt_img.rotate(angle, resample=Image.BICUBIC, expand=False)

    paste_x = int(xy[0] - pad_dim // 2)
    paste_y = int(xy[1] - pad_dim // 2)
    
    image.alpha_composite(rotated_txt, (paste_x, paste_y))

def linestring_angle(line):
    x1, y1 = line.coords[0]
    x2, y2 = line.coords[-1]
    return math.degrees(math.atan2(y2 - y1, x2 - x1))

def log_gdf_preview(name, gdf, columns, n=5):
    available_cols = [c for c in columns if c in gdf.columns]
    if not available_cols:
        logging.info("%s columns not found. available=%s", name, list(gdf.columns))
        return

    logging.info("%s columns: %s", name, available_cols)
    if gdf.empty:
        logging.info("%s is empty", name)
        return

    preview = gdf[available_cols].head(n).to_string(index=False)
    logging.info("%s sample rows:\n%s", name, preview)

def split_grids_for_instance(grids, instance_id, num_instances=10, split_seed=42):
    """Deterministically shuffle and split grids into disjoint worker chunks."""
    if num_instances <= 0:
        raise ValueError("num_instances must be > 0")
    if not (0 <= instance_id < num_instances):
        raise ValueError(
            f"instance_id must be between 0 and {num_instances - 1}, got {instance_id}"
        )

    shuffled = list(grids)
    random.Random(split_seed).shuffle(shuffled)
    return shuffled[instance_id::num_instances]

# ---------- DRAWING ---------- #

""" def draw_annotations(image, annotations):
    draw = ImageDraw.Draw(image)

    try:
        font = ImageFont.truetype("DejaVuSans.ttf", FONTSIZE)
    except:
        font = ImageFont.load_default()

    for x, y, label in annotations:
        #draw.ellipse((x-4, y-4, x+4, y+4), fill="red")
        draw.text((x+6, y-6), label, fill="yellow", font=font)

    return image """
def draw_annotations(image, annotations, fontsize=12):
    """Orchestrates drawing, ensuring lines are drawn before polygon labels."""
    image = image.convert("RGBA")
    
    font = ImageFont.truetype("dejavu-sans.book.ttf", fontsize)

    # 1. Draw Rotated Lines First
    #  (Alpha Compositing)
    for ann in [a for a in annotations if a["style"] == "line"]:
        draw_rotated_text_with_padding(
            image, (ann["x"], ann["y"]), ann["text"], 
            ann["angle"], font, "white", "blue", pad=3
        )

    # 2. Draw Static Labels (Polygons) on top
    draw = ImageDraw.Draw(image)
    for ann in [a for a in annotations if a["style"] != "line"]:
        if ann["style"] == "man_made":
            draw_text_with_padding(draw, (ann["x"], ann["y"]), ann["text"], font, "yellow", "black", pad=3)
        else:
            draw_text_with_padding(draw, (ann["x"], ann["y"]), ann["text"], font, "black", "white", pad=3)
    return image

# ---------- PROCESS SINGLE BBOX ---------- #

def georef_write(image, center_lon, center_lat, out_path):
    xmin, ymin, xmax, ymax, res = image_bounds_mercator(
        center_lon, center_lat
    )

    transform = from_origin(xmin, ymax, res, res)
    img_arr = np.array(image)

    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        height=img_arr.shape[0],
        width=img_arr.shape[1],
        count=3,
        dtype="uint8",
        crs="EPSG:3857",
        transform=transform,
    ) as dst:
        for i in range(3):
            dst.write(img_arr[:, :, i], i + 1)

# -------------------- MAIN PROCESS --------------------
def process_bbox(idx, bbox_geom, img_idx, poly_gdf, cols, line_gdf, line_cols, output_dir, images_dir):
    try:
        # 1. Tile Center & Bounds
        center = bbox_geom.centroid
        cx, cy = center.x, center.y

        image = get_image(img_idx, images_dir)
        if image is None:
            return idx, 0, f"Image not found for img_idx={img_idx}"
        annotations = []

        # -------- POLYGONS --------
        # Filter first using spatial index (sjoin or intersects) for performance
        possible_polys = poly_gdf[poly_gdf.intersects(bbox_geom)]
        for _, row in possible_polys.iterrows():
            # Filter and collect all valid strings from the specified columns
            valid_tags = [
                str(row[col]) for col in cols 
                if col in row and pd.notna(row[col]) and str(row[col]).lower() != "nan"
            ]
            # Combine them with a comma
            tag = ", ".join(valid_tags) if valid_tags else None
            # Skip if nothing was found
            if not tag: continue

            clipped_geom = row.geometry.intersection(bbox_geom)
            if clipped_geom.is_empty or not clipped_geom.is_valid:
                continue
            
            pt = clipped_geom.representative_point()
            px, py = mercator_to_pixel(pt.x, pt.y, cx, cy, IMAGE_SIZE)
            
            if 0 <= px < IMAGE_SIZE[0] and 0 <= py < IMAGE_SIZE[1]:
                annotations.append({
                    "x": px, "y": py, "text": tag, "type": "poly",
                    "style": "man_made" if "man_made" in valid_tags else "poly_other",
                    "angle": None,
                })
        
        # -------- LINES --------
        possible_lines = line_gdf[line_gdf.intersects(bbox_geom)]
        for _, row in possible_lines.iterrows():
            # Filter and collect all valid strings from the specified columns
            valid_tags = [
                str(row[col]) for col in cols 
                if col in row and pd.notna(row[col]) and str(row[col]).lower() != "nan"
            ]
            # Combine them with a comma
            tag = ", ".join(valid_tags) if valid_tags else None
            # Skip if nothing was found
            if not tag: continue

            # CLIP: Line intersection can return MultiLineStrings if it enters/leaves the tile
            clipped_line = row.geometry.intersection(bbox_geom)
            if clipped_line.is_empty:
                continue

            # For lines, interpolate(0.5) on the CLIPPED portion 
            # so the label is in the center of the VISIBLE segment
            pt = clipped_line.interpolate(0.5, normalized=True)
            px, py = mercator_to_pixel(pt.x, pt.y, cx, cy, IMAGE_SIZE)
            
            # Use the original geometry for angle to maintain road directionality, 
            # or clipped_line if you want the angle of the visible portion only.
            angle = linestring_angle(row.geometry) 
            if 0 <= px < IMAGE_SIZE[0] and 0 <= py < IMAGE_SIZE[1]:
                annotations.append({
                    "x": px, "y": py, "text": tag, "type": "line",
                    "style": "man_made" if "man_made" in valid_tags else "line", "angle": angle,
                })

        # -------- DRAW & OUTPUT --------
        image = draw_annotations(image, annotations, fontsize=FONTSIZE)
        if GEOREFERENCED:
            out_path = os.path.join(output_dir, f"bbox_{idx}.tif")
            georef_write(image, cx, cy, out_path)
        else:
            out_path = os.path.join(output_dir, f"bbox_{idx}.png")
            image = image.resize(TARGET_SIZE, resample=Image.LANCZOS)
            image.save(out_path, dpi=(DPI, DPI))
        return idx, len(annotations), None
    except Exception as e:
        logging.exception("bbox %s failed", idx)
        return idx, 0, str(e)
    
# ---------- MAIN PARALLEL PIPELINE ---------- #

def annotate_bboxes_parallel(bbox_gdf, poly_gdf, cols, line_gdf, line_cols, output_dir, images_dir, files):
    #mask = [not f"bbox_{idx}.png" in files
    #    for idx in bbox_gdf.data_idx
    #]
    #bbox_gdf = bbox_gdf[mask]
    logging.info("Queued %s bboxes for annotation", len(bbox_gdf))
    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _, row in bbox_gdf.iterrows():
            futures.append(
                executor.submit(
                    process_bbox,
                    row['idx'],
                    row.geometry,
                    row['img_idx'],
                    poly_gdf[poly_gdf['grid'] == row['idx']].copy(),
                    cols,
                    line_gdf[line_gdf['grid'] == row['idx']].copy(),
                    line_cols,
                    output_dir,
                    images_dir
                )
            )

        for future in as_completed(futures):
            idx, n, err = future.result()
            if err:
                logging.error("bbox %s failed: %s", idx, err)
            else:
                logging.info("bbox %s done (%s tags)", idx, n)

# ---------- USAGE ---------- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Annotate Bing images for a deterministic subset of grids."
    )
    parser.add_argument(
        "instance_id",
        type=int,
        help="Worker index in [0, num_instances-1]. For 10 workers use 0..9.",
    )
    parser.add_argument(
        "--num-instances",
        type=int,
        default=10,
        help="Total parallel script instances (default: 10).",
    )
    parser.add_argument(
        "--split-seed",
        type=int,
        default=42,
        help="Seed used for deterministic random grid split (default: 42).",
    )
    args = parser.parse_args()

    logging.info("Starting Bing annotation pipeline")
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from first_3 import load_config
    cfg = load_config('../config.yaml')
    os.chdir(parent_dir)
    logging.info("Configuration loaded")

    #images_dir = "./annotation_scripts/images"
    images_dir = cfg["paths"]["annotations_images_dir"]
    grid_filedir = cfg["paths"]["annotations_grid_dir"]
    temp_parquet_dir = cfg["paths"]["annotations_temp_parquet_dir"]
    geojson_file_dir = cfg['paths']['annotations_by_osm_dir']
    file_location_dir = os.path.join(os.path.abspath(os.path.join(grid_filedir, '..')), 'data')
    
    poly_filepath = os.path.join(file_location_dir, 'merged_polygons.parquet')
    line_filepath = os.path.join(file_location_dir, 'merged_lines.parquet')

    #points_path = cfg["paths"]["corrected_all_filepath"]
    points_path = f'{images_dir}/ref.geojson'
    grids_filepath = os.path.join(grid_filedir, f'grids_{os.path.basename(cfg["paths"]["corrected_all_filepath"])}')

    # 1. Load the grid and ensure the merge key is an integer
    bbox_gdf = gpd.read_file(grids_filepath).to_crs(3857)
    bbox_gdf['idx'] = bbox_gdf['idx'].astype(int)

    # 2. Load the points and prepare the img_idx
    points_gdf = gpd.read_file(points_path).to_crs(3857)
    points_gdf['idx'] = points_gdf['idx'].astype(int)
    points_gdf['img_idx'] = points_gdf.index.astype(int)

    bbox_gdf = bbox_gdf.merge(
        points_gdf[['idx', 'img_idx']], 
        on='idx', 
        how='inner'
    )
    bbox_gdf = bbox_gdf[bbox_gdf.geometry.is_valid & ~bbox_gdf.geometry.is_empty]
    logging.info("Prepared %s valid bounding boxes", len(bbox_gdf))
    #log_gdf_preview("grid_bbox (bbox_gdf)", bbox_gdf, ["data_idx_grid", "geometry", "img_idx"])

    #output_dir = './annotation_scripts/annotated_images'
    output_dir = cfg["paths"]["annotations_output_dir"]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    files = set(os.listdir(output_dir))
    geojson_files = set(os.listdir(geojson_file_dir))

    poly_cols = ["id", "man_made", "landuse", "industrial", "power", "resource"]
    line_cols = ["waterway", "man_made", "landuse", "industrial", "power", "resource", "water", "id"]
    cols_string = ','.join(poly_cols)
    line_cols_string = ','.join(line_cols)

    conn = duckdb.connect(f'temp_{int(random.randint(0, int(1e6)))}.db')
    conn.execute('INSTALL SPATIAL; LOAD SPATIAL;')
    logging.info("DuckDB initialized with SPATIAL extension")

    grids = bbox_gdf['idx'].unique().tolist()
    logging.info("Found %s total grids", len(grids))

    grids = split_grids_for_instance(
        grids,
        instance_id=args.instance_id,
        num_instances=args.num_instances,
        split_seed=args.split_seed,
    )
    logging.info(
        "Instance %s/%s will process %s grids (seed=%s)",
        args.instance_id,
        args.num_instances,
        len(grids),
        args.split_seed,
    )
    if not grids:
        logging.info("No grids assigned to this instance. Exiting.")
        sys.exit(0)

    for i in range(0, len(grids), 2 * MAX_WORKERS):
        sub_grids = grids[i : i + 2 * MAX_WORKERS]
        if not sub_grids:
            continue

        logging.info(
            "Processing grid batch %s-%s (%s grids)",
            i,
            i + len(sub_grids) - 1,
            len(sub_grids),
        )

        # --- Polygons ---
        poly_file_columns = {}
        all_poly_columns = set()
        for grid in sub_grids:
            polygon_file = f"idx_{grid}_polygons.geojson"
            path = f"{geojson_file_dir}/{polygon_file}"
            if polygon_file not in geojson_files:
                continue
            cols = conn.execute(f"DESCRIBE SELECT * FROM ST_READ('{path}')").df()["column_name"].tolist()
            poly_file_columns[grid] = cols
            all_poly_columns.update(cols)

        all_poly_columns = sorted(all_poly_columns - {"geom"})  # remove geom from attributes

        poly_queries = []
        for grid, cols in poly_file_columns.items():
            polygon_file = f"{geojson_file_dir}/idx_{grid}_polygons.geojson"
            select_cols = []

            for c in all_poly_columns:
                if c not in poly_cols:
                    continue
                if c in cols:
                    select_cols.append(c)
                else:
                    select_cols.append(f"NULL AS {c}")
            
            if select_cols:
                select_clause = ", ".join(select_cols) + ", "
                poly_queries.append(f"""
                    SELECT
                        {select_clause}
                        ST_AsText(geom) AS geometry,
                        '{grid}' AS grid
                    FROM ST_READ('{polygon_file}')
                """)
            else:
                poly_queries.append(f"""
                    SELECT
                        ST_AsText(geom) AS geometry,
                        '{grid}' AS grid
                    FROM ST_READ('{polygon_file}')
                """)

        poly_query = " UNION ALL ".join(poly_queries)
        poly_df = conn.execute(poly_query).df()
        poly_df = poly_df.dropna(subset=["geometry"])
        poly_df["geometry"] = poly_df["geometry"].apply(safe_wkt_load)
        poly_gdf = gpd.GeoDataFrame(poly_df, geometry="geometry", crs=4326).to_crs(3857)
        poly_gdf["grid"] = poly_gdf["grid"].astype(int)

        logging.info("Loaded %s polygon records in current batch", len(poly_gdf))
        log_gdf_preview("poly_gdf", poly_gdf, ["grid", "geometry"])

        # --- Lines ---
        line_file_columns = {}
        all_line_columns = set()
        for grid in sub_grids:
            line_file = f"idx_{grid}_lines.geojson"
            path = f"{geojson_file_dir}/{line_file}"
            if line_file not in geojson_files:
                continue
            cols = conn.execute(f"DESCRIBE SELECT * FROM ST_READ('{path}')").df()["column_name"].tolist()
            line_file_columns[grid] = cols
            all_line_columns.update(cols)

        all_line_columns = sorted(all_line_columns - {"geom"})

        line_queries = []
        for grid, cols in line_file_columns.items():
            line_file = f"{geojson_file_dir}/idx_{grid}_lines.geojson"
            select_cols = []

            for c in all_line_columns:
                if c not in line_cols:
                    continue
                if c in cols:
                    select_cols.append(c)
                else:
                    select_cols.append(f"NULL AS {c}")

            if select_cols:
                select_clause = ", ".join(select_cols) + ", "
                line_queries.append(f"""
                    SELECT
                        {select_clause}
                        ST_AsText(geom) AS geometry,
                        '{grid}' AS grid
                    FROM ST_READ('{line_file}')
                """)
            else:
                line_queries.append(f"""
                    SELECT
                        ST_AsText(geom) AS geometry,
                        '{grid}' AS grid
                    FROM ST_READ('{line_file}')
                """)

        line_query = " UNION ALL ".join(line_queries)
        line_df = conn.execute(line_query).df()
        line_df = line_df.dropna(subset=["geometry"])
        line_df["geometry"] = line_df["geometry"].apply(safe_wkt_load)
        line_gdf = gpd.GeoDataFrame(line_df, geometry="geometry", crs=4326).to_crs(3857)
        line_gdf["grid"] = line_gdf["grid"].astype(int)

        logging.info("Loaded %s line records in current batch", len(line_gdf))
        log_gdf_preview("lines_gdf", line_gdf, ["grid", "geometry"])
        batch_bbox_gdf = bbox_gdf[bbox_gdf['idx'].isin(sub_grids)].copy()

        # Run your parallel annotation
        annotate_bboxes_parallel(batch_bbox_gdf, poly_gdf, ["man_made", "landuse", "industrial", "power", "resource", "water"], line_gdf, ["waterway", "man_made", "landuse", "industrial", "power", "resource", "water"], 
                        output_dir, images_dir, files)
    logging.info("Annotation pipeline finished")