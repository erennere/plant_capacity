import os
import numpy as np
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from matplotlib.colors import LogNorm
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable

from pipelines import create_pop_output_paths

try:
    from ..starter import load_config
    from ..pipelines import create_pop_output_paths
except ImportError:
    from research_code.starter import load_config
    from research_code.pipelines import create_pop_output_paths

def aggregate_by_country(gdf, country_column, agg_column, industrial_column=None, is_pop=False):
    gdf = gdf.copy()
    agg_dict = {
        f"{agg_column}_sum": "sum",
        f"{agg_column}_mean": "mean",
        f"{agg_column}_median": "median",
        f"{agg_column}_std": "std",
    }

    if is_pop:
        gdf = gdf.dropna(subset=[country_column, agg_column])
        aggregated = gdf.groupby(country_column)[agg_column].agg(**agg_dict).reset_index()

    else:
        if industrial_column is None:
            raise ValueError("industrial_column must be provided when is_pop=False")

        gdf = gdf.dropna(subset=[country_column, agg_column, industrial_column])
        grouped = gdf.groupby([country_column, industrial_column])[agg_column].agg(**agg_dict).reset_index()

        ind = grouped[grouped[industrial_column] == True].drop(columns=[industrial_column]).reset_index(drop=True)
        res = grouped[grouped[industrial_column] != True].drop(columns=[industrial_column]).reset_index(drop=True)
        ind = ind.rename(columns={c: f"IND_{c}" for c in ind.columns if c != country_column})
        res = res.rename(columns={c: f"RES_{c}" for c in res.columns if c != country_column})
        aggregated = res.merge(ind, on=country_column, how="left")
    return aggregated

def plot_splitted_piechart(dist_tag1, dist_tag2, ax,
                            size_tag1, size_tag2, min_size,
                            labels=False, labels_text = ['Paved', 'Unpaved', ''],
                            cmap="tab20c"):
    ax.grid(False)
    ax.set_axis_off()
    
    # Determine relative radii
    rad_tag1, rad_tag2 = (1, size_tag2/size_tag1) if size_tag1 > size_tag2 else (size_tag1/size_tag2, 1)
    # Colormap
    cmap = plt.get_cmap(cmap)
    colors = {
        "tag1": [cmap(i) for i in [2, 6]] + [(1,1,1,0)],
        "tag2": [cmap(i) for i in [1, 5]] + [(1,1,1,0)]
    }
    
    # Append totals for donut effect
    val_tag1 = np.array(dist_tag1 + [sum(dist_tag1)])
    val_tag2 = np.array(dist_tag2 + [sum(dist_tag2)])
    pie_labels =  labels_text if labels else None

    def plot_pie(values, radius, cols, min_size, startangle, counterclock):
        # min_size here is the min size of WWTPs and the halving is due to the sum of all sizes as the last element
        if radius <= 0 or sum(values)/2 < min_size:
            return
        wedgeprops = dict(width=0.7) if radius > 0.5 else None
        wedges, _ = ax.pie(values, radius=radius, colors=cols,
                            wedgeprops=wedgeprops, labels=pie_labels,
                            startangle=startangle, counterclock=counterclock,
                            textprops={"fontsize":12})
        for w in wedges[:-1]:
            w.set_edgecolor('white')
            w.set_linewidth(0.9)
    plot_pie(val_tag1, rad_tag1, colors["tag1"], min_size, startangle=90, counterclock=True)
    plot_pie(val_tag2, rad_tag2, colors["tag2"], min_size, startangle=90, counterclock=False)

    ax.set_axis_off()

def get_pos(geometry):
    if geometry.geom_type == 'Polygon':
        return geometry.centroid.x, geometry.centroid.y
    elif geometry.geom_type == 'MultiPolygon':
        return max(list(geometry.geoms), key=lambda x: x.area).centroid.x, (max(list(geometry.geoms), key=lambda x: x.area)).centroid.y
    raise ValueError("Invalid geometry type")

def calculate_size(value, min_value, max_value, min_size, max_size, scale='log'):
    if scale == 'log':
        return (np.log(value) - np.log(min_value)) / (np.log(max_value) - np.log(min_value)) * (max_size - min_size) + min_size
    elif scale == 'linear':
        return (value - min_value) / (max_value - min_value) * (max_size - min_size) + min_size
    else: 
        raise ValueError("Invalid scale")
    
def round_numbers(arr, breaks):
    arr = np.asarray(arr)
    arr = arr[np.isfinite(arr) & (arr > 0)]
    nums = np.linspace(arr.min(), arr.max(), len(breaks)).astype(int)
    rounded = []
    for n in nums:
        log_n  = np.log10(n)
        power = int(np.floor(log_n))
        coeff = round(10*(log_n - power))
        rounded.append((coeff+1)*10**power)
    return rounded
            
def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config()

    approach = cfg['figures']['approach']
    boundaries_filepath = cfg['paths']['country_boundaries_filepath']
    pop_filepath = os.path.abspath(create_pop_output_paths(cfg)['voronoi'][approach])
    stats_filepath = cfg['paths']['raster_country_stats_filepath']

    pop_column = 'population_served_index'
    filter_col = '2024_zonal_sum'
    industrial_col = 'IND/RES'
    tag1 = 'round_area'
    tag2 = 'wwtp_area_rect_2'
    min_total_size = 10000
    scale = 'linear'
    agg_type = 'sum'

    agg_columns = {
        True: [ '2024_zonal_sum', '2023_zonal_sum', '2022_zonal_sum', 
            '2021_zonal_sum', '2020_zonal_sum', '2019_zonal_sum',
            '2018_zonal_sum', '2017_zonal_sum', '2016_zonal_sum',
            '2015_zonal_sum', '2014_zonal_sum'],
        False: ['num_detection_circle', 'num_detection_square', 'total_area', tag1, tag2]}
    
    # Load boundaries
    boundaries = gpd.read_file(boundaries_filepath).to_crs("ESRI:54030")
    boundaries['country'] = boundaries['ISO_A2_EH']
    boundaries = boundaries.drop_duplicates(subset=['country'])

    # Load population / WWTP data
    pop_gdf = gpd.read_file(pop_filepath)
    pop_gdf['country'] = pop_gdf['ISO_2']
    pop_gdf = pop_gdf.drop('geometry', axis=1)
    pop_gdf[industrial_col] = np.random.randint(0, 2, len(pop_gdf)).astype(bool)

    stats_df = pd.read_csv(stats_filepath)
    
    agg_datasets = []
    for is_pop, col_list in agg_columns.items():
        for agg_column in col_list:
            agg_datasets.append(aggregate_by_country(pop_gdf, 'country', agg_column, industrial_col, is_pop=is_pop))
    for dataset in agg_datasets:
        boundaries = boundaries.merge(dataset, on='country', how='left')
    boundaries = boundaries.merge(stats_df, on='country', how='left')

    # Disable seaborn/default style
    plt.style.use('default')
    fig = plt.figure(figsize=(20, 10), dpi=600)
    ax = fig.add_axes([0.05, 0.15, 0.9, 0.8], projection=ccrs.Robinson())  # [left, bottom, width, height]
        
    # Plot boundaries colored by population / WWTP metric
    boundaries = boundaries.drop_duplicates(subset=['country'])
    #boundaries[pop_column] = boundaries[pop_column]/1000
    boundaries[pop_column] = boundaries[pop_column]*100
    """     boundaries[boundaries.geometry.notna()].plot(
        ax=ax,
        column=pop_column,
        cmap='viridis',
        edgecolor='white',
        linewidth=0.5,
        legend=True,
        norm=LogNorm(),  # <-- THIS
        legend_kwds={
            'label': "Number of People Served by a WWTP (in thousands)",
            'orientation': 'horizontal',
            'shrink': 0.25,
            'pad': 0.05,
            'aspect' : 20
        }
    ) """
    # Make a copy of your GeoDataFrame with valid geometries
    gdf = boundaries[boundaries.geometry.notna()].copy()
    data = gdf[pop_column].dropna()
    log_norm = LogNorm(vmin=data.min(),vmax=data.max())
    norm = Normalize(vmin=data.min(),vmax=data.max())
    
    # Plot choropleth WITHOUT automatic legend
    gdf.plot(
        ax=ax,
        column=pop_column,
        cmap='viridis',
        edgecolor='white',
        linewidth=0.5,
        legend=False##,
        ##norm=log_norm,  # <-- THIS
    )
    ax.set_global()

    # ScalarMappable for manual colorbar
    sm = ScalarMappable(cmap='viridis', norm=log_norm)
    sm = ScalarMappable(cmap='viridis', norm=norm)
    sm._A = []

    # Colorbar below the map
    cbar_ax = fig.add_axes([0.3, 0.1, 0.5, 0.02])
    cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')

    #cbar.set_label("Number of People Served by a WWTP (in thousands)", fontsize=20)
    cbar.set_label("Percentage of People Served by a WWTP", fontsize=20)

    # Add coastlines for context
    ax.coastlines(resolution='110m', color='black', linewidth=0.5)
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.left_labels = False
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER

    boundaries['total_size'] = boundaries[f'IND_{tag1}_{agg_type}'] + boundaries[f'IND_{tag2}_{agg_type}'] + boundaries[f'RES_{tag1}_{agg_type}'] + boundaries[f'RES_{tag2}_{agg_type}']
    max_size = np.nanpercentile(boundaries['total_size'], 99)
    min_size = np.nanpercentile(boundaries['total_size'], 1)
    min_pie_size = 0.1
    max_pie_size = 0.7
    breaks = [5, 15, 30, 45, 60, 75, 90]
    breaks = round_numbers(boundaries['total_size'], breaks)
    
    for index, row in boundaries.iterrows():
        xpos, ypos = get_pos(row.geometry)
        dist_ind = [float(row[f'IND_{tag1}_{agg_type}']), float(row[f'IND_{tag2}_{agg_type}'])]
        dist_res = [float(row[f'RES_{tag1}_{agg_type}']), float(row[f'RES_{tag2}_{agg_type}'])]
        """         
        if dist_ind == [0., 0.] or any(np.isnan(item) for item in dist_ind):
            dist_ind = [1., 1.]
            
        if dist_res == [0., 0.] or any(np.isnan(item) for item in dist_res):
            dist_res = [1., 1.] 
        """
        if any(np.isnan(item) for item in dist_ind):
            dist_ind = [0., 0.]
        if any(np.isnan(item) for item in dist_res):
            dist_res = [0., 0.]
        if max(sum(dist_ind), sum(dist_res)) < min_total_size:
            continue

        #size_ind = max(calculate_size(sum(dist_ind), min_size, max_size, min_pie_size, max_pie_size, scale), min_pie_size)
        #size_res = max(calculate_size(sum(dist_res), min_size, max_size, min_pie_size, max_pie_size, scale), min_pie_size)
        size_ind = calculate_size(sum(dist_ind), min_size, max_size, min_pie_size, max_pie_size, scale)
        size_res = calculate_size(sum(dist_res), min_size, max_size, min_pie_size, max_pie_size, scale)

        #size = max(size_ind, size_res)
        size = size_ind + size_res
        if size_res < min_pie_size and size_ind < min_pie_size:
            continue
        ax_pie = inset_axes(ax, width=size, height=size, loc='center', bbox_to_anchor=(xpos, ypos, 1, 1), bbox_transform=ax.transData)
        plot_splitted_piechart(dist_res, dist_ind, ax_pie, size_res, size_ind, min_pie_size)

    # Create size legend directly in the figure
    largest_size = calculate_size(breaks[-1], min_size, max_size, min_pie_size, max_pie_size, scale)
    legend_ax = inset_axes(ax, width=largest_size, height=largest_size, loc='lower left', bbox_to_anchor=(0.06, 0.02, 1, 1), bbox_transform=ax.transAxes)
    legend_ax.axis('off')
    for i, size in enumerate([5000000, 10000000, 20000000, 30000000, 40000000, 50000000]):
        relative_size = calculate_size(size, min_size, max_size, min_pie_size, max_pie_size, scale)/largest_size
        circle = plt.Circle((0.5, 0.25 + relative_size / 2), relative_size / 2, color='black', fill=False)
        legend_ax.add_patch(circle)
        legend_ax.annotate(str(round(size/10**6)) + r" $\text{km}^2$", xy=(0.5, 0.25 + relative_size), xytext=(1.05, 0.25 + relative_size), ha='left', va='center', arrowprops=dict(arrowstyle='-', color='black'),fontsize=8)
    legend_ax.set_title("Total WWTP Area", fontsize=14, weight="semibold")

    #create the piechart legend
    pie_legend_ax = inset_axes(ax, width=largest_size, height=largest_size, loc='lower left', bbox_to_anchor=(0.06, 0.32, 1, 1), bbox_transform=ax.transAxes)
    dist_res = [8000000, 8000000]
    dist_ind = [10000000, 10000000]
    size_ind = max(calculate_size(sum(dist_ind), min_size, max_size, min_pie_size, max_pie_size, scale), min_pie_size)
    size_res = max(calculate_size(sum(dist_res), min_size, max_size, min_pie_size, max_pie_size, scale), min_pie_size)
    plot_splitted_piechart(dist_res, dist_ind, pie_legend_ax, size_res, size_ind, min_pie_size, labels=True, labels_text=['circular', 'rectangular', ''])

    pie_legend_ax.set_title("WWTP Type",fontsize=14,weight="semibold",y=1.05)
    pie_legend_ax.annotate('Residential', xy=(-0.1, 1), ha='left', va='center', fontsize=14, weight="semibold", xycoords='axes fraction')
    pie_legend_ax.annotate('Industrial', xy=(1.1, 1), ha='right', va='center', fontsize=14, weight="semibold", xycoords='axes fraction')
    pie_legend_ax.set_axis_off()

    # create title 
    ax.set_title("Worldwide Overview of WWTPs by Size and Technology",
                fontsize=24, fontweight='bold')
    plt.tight_layout()
    plt.savefig(cfg['paths']['static_piechart_filepath'], dpi=200)

if __name__ == '__main__':
    main()