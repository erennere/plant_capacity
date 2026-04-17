# ===============================================================
# Overpass Downloader — Multi-category OSM data extractor (by idx)
# Extracts only LINES and POLYGONS
# Each feature (by 'idx') saved as GeoJSON
# Fully Fiona-free, 100% safe for QGIS Python 3.12
# ===============================================================

import os, requests, json, time, re, sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Polygon
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from ..starter import load_config
except ImportError:
    from research_code.starter import load_config

# ------------------ USER INPUTS ------------------

start_idx = 0     # <-- change here
end_idx   = 100000     # <-- change here

queries = {
    "man_made": "wastewater_plant",
    "waterway": "stream",
    "waterway": "river",
    "landuse": "",
    "power": "plant",
    "industrial": ""
}

urls = ["https://overpass.kumi.systems/api/interpreter",
         "https://maps.mail.ru/osm/tools/overpass/api/interpreter"]
         #"https://overpass-api.de/api/interpreter"]
pause_seconds = 0.1
# -------------------------------------------------

# ------------------ SUPPORT FUNCTIONS ------------------

def clean_columns(gdf):
    """Clean column names and ensure valid types for GeoJSON export."""
    if gdf.empty:
        return gdf

    # Clean names
    new_cols = []
    for c in gdf.columns:
        clean = re.sub(r"[^0-9a-zA-Z_]", "_", c)
        new_cols.append(clean[:60])
    gdf.columns = new_cols

    # Convert non-geometry columns to strings (GeoJSON accepts any type)
    for col in gdf.columns:
        if col != "geometry":
            try:
                gdf[col] = gdf[col].astype(str)
            except Exception:
                gdf[col] = gdf[col].apply(lambda x: str(x) if x is not None else "")
    return gdf


def query_overpass(bbox, queries, url, retries=3):
    """Query Overpass API safely with retries."""
    q = f"""
    [out:json][timeout:180];
    ("""
    for key, value in queries.items():
        if value:
            filter_expr = f'["{key}"="{value}"]'
        else:
            filter_expr = f'["{key}"]'
        q += f"""
        way{filter_expr}({bbox});
        relation{filter_expr}({bbox});
        """
    q += f"""
    );
    (._;>;);
    out body;
    """

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params={"data": q}, timeout=300)
            
            if r.status_code == 200:
                try:
                    return r.json()
                except json.JSONDecodeError:
                    print("⚠️ JSON error → invalid response, skipping.")
                    return None
            elif r.status_code in [429, 504]:
                print(f"⚠️ Server limit ({r.status_code}) → waiting {pause_seconds * attempt}s...")
                time.sleep(pause_seconds * attempt)
            else:
                print(f"⚠️ Overpass error {r.status_code}")
                break
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            time.sleep(pause_seconds * attempt)
    return None


def elements_to_gdf(data):
    """Convert Overpass JSON to GeoDataFrames for lines and polygons only."""
    if not data or "elements" not in data:
        empty = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        return empty.copy(), empty.copy()

    elements = data["elements"]
    nodes = {el["id"]: (el["lon"], el["lat"]) for el in elements if el["type"] == "node"}
    ways  = {el["id"]: el for el in elements if el["type"] == "way"}

    lns, polys = [], []

    for el in elements:
        tags = el.get("tags", {})
        if el["type"] == "way":
            coords = [nodes[n] for n in el.get("nodes", []) if n in nodes]
            if not coords:
                continue
            if coords[0] == coords[-1]:
                polys.append({**tags, "id": el["id"], "geometry": Polygon(coords)})
            else:
                lns.append({**tags, "id": el["id"], "geometry": LineString(coords)})
        elif el["type"] == "relation":
            outers = []
            for m in el.get("members", []):
                if m.get("type") == "way" and m.get("role") == "outer":
                    way_el = ways.get(m["ref"])
                    if way_el:
                        coords = [nodes[n] for n in way_el.get("nodes", []) if n in nodes]
                        if coords and coords[0] == coords[-1]:
                            outers.append(coords)
            if outers:
                try:
                    polys.append({**tags, "id": el["id"], "geometry": Polygon(outers[0])})
                except Exception:
                    pass

    gdf_lns  = gpd.GeoDataFrame(lns, geometry="geometry", crs="EPSG:4326") if lns else gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdf_poly = gpd.GeoDataFrame(polys, geometry="geometry", crs="EPSG:4326") if polys else gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    return gdf_lns, gdf_poly


# ------------------ MAIN SCRIPT ------------------
def timer(label):
    def wrap(func):
        def inner(*args, **kwargs):
            t0 = time.time()
            result = func(*args, **kwargs)
            dt = time.time() - t0
            print(f"⏱️ {label}: {dt:.5f} sec")
            return result
        return inner
    return wrap

#query_overpass = timer("Overpass query")(query_overpass)
#elements_to_gdf = timer("JSON ➜ GeoDataFrame")(elements_to_gdf)
#clean_columns = timer("Clean columns")(clean_columns)

def find_bbox(geometry):
    if geometry is None or pd.isna(geometry) or geometry.is_empty:
        return None
    
    if not geometry.is_valid:
        geometry = geometry.buffer(0)
        # Re-check after buffer; if still invalid or became empty, bail
        if geometry is None or not geometry.is_valid or geometry.is_empty:
            return None
            
    minx, miny, maxx, maxy = geometry.bounds
    return f"{miny},{minx},{maxy},{maxx}"

def create_tasks(gdf, batch_size):
    rows = list(gdf.itertuples())
    url_count = len(urls)

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start+batch_size]
        yield [(r.bbox, r.idx, urls[i % url_count]) 
               for i, r in enumerate(batch)]

def row_operation(bbox, idx_val, url, output_folder):
    if bbox is None: return
    line_path = os.path.join(output_folder, f"idx_{idx_val}_lines.geojson")
    poly_path = os.path.join(output_folder, f"idx_{idx_val}_polygons.geojson")
    data = query_overpass(bbox, queries, url)
    all_lines, all_polys = elements_to_gdf(data)

    # Save each idx as separate GeoJSON files
    if not all_lines.empty:
        all_lines = clean_columns(all_lines)

        all_lines.to_file(line_path, driver="GeoJSON")
        print(f"✅ Saved {line_path}")

    if not all_polys.empty:
        all_polys = clean_columns(all_polys)
        poly_path = os.path.join(output_folder, f"idx_{idx_val}_polygons.geojson")
        all_polys.to_file(poly_path, driver="GeoJSON")
        print(f"✅ Saved {poly_path}")

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()
    overwrite = cfg["annotations"]["overwrite"]

    points_path = cfg["paths"]["corrected_all_filepath"]
    #points_path = './annotation_scripts/ref.geojson'

    grid_filedir = cfg["paths"]["annotations_grid_dir"]
    grid_filepath = os.path.join(grid_filedir, f'grids_{os.path.basename(points_path)}')
    output_folder = cfg["paths"]["annotations_by_osm_dir"]

    os.makedirs(output_folder, exist_ok=True)
    max_workers = int(cfg["annotations"]["max_workers"])
    batch_size  = max_workers*len(urls)

    poly = gpd.read_file(grid_filepath).to_crs(4326)
    poly['bbox'] = poly['geometry'].map(find_bbox)

    if not overwrite:
        output_list = set(os.listdir(output_folder))
        mask = []
        for idx in poly['idx'].values:
            line_file = f"idx_{idx}_lines.geojson"
            poly_file = f"idx_{idx}_polygons.geojson"
            if line_file in output_list or poly_file in output_list:
                mask.append(False)
            else:
                mask.append(True)
        poly = poly[mask]
    print(len(poly))
    print(poly.head())
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for tasks in create_tasks(poly, batch_size):
            futures = [executor.submit(row_operation, bbox, idx_val, url, output_folder) for bbox, idx_val, url in tasks]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"⚠️ Error processing task: {e}")
        print("\n🎯 All requested idx features processed successfully!")

if __name__ == "__main__":
    main()
