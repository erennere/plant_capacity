import os
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import box
from PIL import Image
import numpy as np

# ==========================================================
# PARAMETERS (adjust here)
# ==========================================================
grid_fp = r'H:/02_RESEARCH/2025/HEIDELBERG/grid.shp'  # path to grid shapefile
layers_fps = [r'H:/02_RESEARCH/2025/HEIDELBERG/layer1.shp']  # list of vector layers to plot
output_folder = r'H:/02_RESEARCH/2025/HEIDELBERG/PATCH'
id_field = 'idx'
output_width_px = 3072
output_height_px = 3072
dpi = 96
image_format = 'tif'  # 'tif' or 'png'
# ==========================================================

os.makedirs(output_folder, exist_ok=True)

# Load grid and layers
grid = gpd.read_file(grid_fp)
layers = [gpd.read_file(fp) for fp in layers_fps]

print(f"🗺 Loaded {len(layers)} layers")

# EXPORT LOOP
for i, feature in grid.iterrows():
    if id_field not in grid.columns:
        raise Exception(f"❌ The field '{id_field}' does not exist in the grid layer.")
    
    feature_id = feature[id_field]
    extent = feature.geometry.bounds  # minx, miny, maxx, maxy

    fig, ax = plt.subplots(figsize=(output_width_px / dpi, output_height_px / dpi), dpi=dpi)

    # Plot each layer
    for lyr in layers:
        lyr.plot(ax=ax, color='lightblue', edgecolor='k')

    # Set extent to the current grid cell
    ax.set_xlim(extent[0], extent[2])
    ax.set_ylim(extent[1], extent[3])
    ax.axis('off')  # no axes

    # Save image
    file_path = os.path.join(output_folder, f"{feature_id}.{image_format}")
    fig.savefig(file_path, dpi=dpi, bbox_inches='tight', pad_inches=0)
    plt.close(fig)

    # Create a world file (.wld)
    pixel_size_x = (extent[2] - extent[0]) / output_width_px
    pixel_size_y = -(extent[3] - extent[1]) / output_height_px

    world_file = file_path.replace(f".{image_format}", ".wld")
    with open(world_file, 'w') as wf:
        wf.write(f"{pixel_size_x}\n")
        wf.write("0.0\n")
        wf.write("0.0\n")
        wf.write(f"{pixel_size_y}\n")
        wf.write(f"{extent[0]}\n")
        wf.write(f"{extent[3]}\n")

    print(f"✅ Exported and georeferenced: {file_path}")

print("🎯 All tiles exported successfully!")
