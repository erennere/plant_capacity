import os
import logging
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import geopandas as gpd
import numpy as np
import cartopy.crs as ccrs
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
from matplotlib.colors import LogNorm, Normalize, ListedColormap
from research_code.starter import load_config, parse_config_overrides
from research_code.pop_at_risk_river_calculations.find_pop_in_danger_pop import find_tiles_in_countries

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_single_plot(
    z8_stats,
    column,
    title,
    output_filename,
    output_dir="coverage_maps",
    cmap=plt.get_cmap('viridis'),
    scale_type='log',
    value_transform=None,
    value_label=None,
    min_count_col=None,
    min_count=None,
    projection=None,
    vmin=None,
    vmax=None,
    missing_color='gray',
    edgecolor='white',
    linewidth=0.5,
    legend=True,
    legend_kwds=None,
    suptitle_template="Overview of the {title}",
    author_note=None,
    save_dpi=1000,
    show=True,
):
    """Create a projected choropleth plot from a GeoDataFrame column.

    Parameters are intentionally generic so this helper can be reused for
    different geospatial completeness or risk metrics.
    """
    if column not in z8_stats.columns:
        raise KeyError(f"Column '{column}' not found in GeoDataFrame")

    gdf = z8_stats.copy()

    # Set CRS to WGS84 if missing.
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)

    projection = projection or ccrs.Robinson()
    fig, ax = plt.subplots(figsize=(20, 12), dpi=400, subplot_kw={'projection': projection})

    # Build plotting values from source column.
    values = pd.to_numeric(gdf[column], errors='coerce')
    if value_transform is not None:
        values = value_transform(values)
    gdf['plot_value'] = values

    # Remove invalid geometries and project.
    gdf = gdf[gdf['geometry'].is_valid]
    gdf = gdf.to_crs(projection.proj4_init)

    # Optional masking by supporting count column.
    if min_count_col is not None and min_count is not None:
        if min_count_col not in gdf.columns:
            raise KeyError(f"Column '{min_count_col}' not found in GeoDataFrame")
        gdf.loc[gdf[min_count_col] < min_count, 'plot_value'] = np.nan

    if scale_type == 'log':
        valid_values = gdf['plot_value'][gdf['plot_value'] > 0]
        if valid_values.empty:
            raise ValueError("No valid positive values in 'plot_value' for logarithmic normalization.")

        if vmin is None:
            vmin = max(10 ** (-3), 10 ** (np.floor(np.log10(valid_values.min()))))
        if vmax is None:
            vmax = 10 ** 2
        norm = LogNorm(vmin=vmin, vmax=vmax)

        gdf.loc[gdf['plot_value'] < vmin, 'plot_value'] = np.nan
        gdf.loc[gdf['plot_value'] > vmax, 'plot_value'] = np.nan
    elif scale_type == 'linear':
        if vmin is None:
            vmin = gdf['plot_value'].min()
        if vmax is None:
            vmax = gdf['plot_value'].max()
        if vmin == vmax:
            raise ValueError("Minimum and maximum values are the same. Cannot normalize.")
        norm = Normalize(vmin=vmin, vmax=vmax)
    else:
        raise ValueError("scale_type must be 'log' or 'linear'")

    cmap_values = cmap(np.arange(cmap.N))
    cmap = ListedColormap(cmap_values)
    cmap.set_bad(color=missing_color)

    default_legend_kwds = {
        'label': value_label or title,
        'anchor': (0.2, 0.75),
        'orientation': 'horizontal',
        'shrink': 0.25,
        'pad': 0.05,
    }
    if legend_kwds:
        default_legend_kwds.update(legend_kwds)

    gdf.plot(
        ax=ax,
        edgecolor=edgecolor,
        linewidth=linewidth,
        column='plot_value',
        cmap=cmap,
        legend=legend,
        legend_kwds=default_legend_kwds,
        norm=norm,
        missing_kwds={'color': missing_color},
    )

    if hasattr(ax, 'gridlines'):
        gl = ax.gridlines(linewidth=0.5, color='gray', alpha=0.5, linestyle='--', draw_labels=True)  # type: ignore[attr-defined]
        gl.xlabels_top = False
        gl.ylabels_left = False
        gl.xformatter = LongitudeFormatter()
        gl.yformatter = LatitudeFormatter()

    plt.suptitle(suptitle_template.format(title=title), fontsize=16, fontweight='bold', y=0.95)

    if author_note:
        ax.annotate(
            author_note,
            xy=(1, -0.2),
            xycoords='axes fraction',
            ha='right',
            va='center',
            fontsize=8,
            backgroundcolor='white',
        )

    plt.tight_layout(rect=(0, 0, 1, 0.9), pad=2)
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(fname=f'{output_dir}/{output_filename}', dpi=save_dpi)
    if show:
        plt.show()
    return fig, ax

def create_impact_polygon_plots(pop_at_risk_gdf, tiles_gdf, output_filepath):
    """Create one map per radius/year population zonal-sum column."""
    tiles_gdf = tiles_gdf.copy()
    pop_at_risk_gdf = pop_at_risk_gdf.copy()
    tiles_gdf = pd.merge(tiles_gdf, pop_at_risk_gdf.drop(columns='geometry'), on='tile', how='left')
    plot_columns = [col for col in tiles_gdf.columns if col.endswith('_zonal_sum')]

    if not plot_columns:
        logger.warning("No '*_zonal_sum' columns found for impact polygon plotting.")
        return

    colormap_cycle = [
        plt.get_cmap('viridis'),
        plt.get_cmap('plasma'),
        plt.get_cmap('cividis'),
        plt.get_cmap('magma'),
        plt.get_cmap('YlGnBu'),
        plt.get_cmap('YlOrRd'),
        plt.get_cmap('PuBuGn'),
        plt.get_cmap('cubehelix'),
    ]

    for idx, col in enumerate(plot_columns):
        parts = col.split('_')
        if len(parts) < 3:
            logger.warning("Skipping unrecognized zonal-sum column format: %s", col)
            continue

        try:
            radius = int(parts[0])
            year = int(parts[1])
        except ValueError:
            logger.warning("Skipping zonal-sum column with non-numeric radius/year: %s", col)
            continue

        title = f"Population at Risk (Radius {radius}m, Year {year})"
        output_filename = f"impact_pop_radius_{radius}_year_{year}.png"
        selected_cmap = colormap_cycle[idx % len(colormap_cycle)]

        create_single_plot(
            z8_stats=tiles_gdf,
            column=col,
            title=title,
            output_filename=output_filename,
            output_dir=output_filepath,
            cmap=selected_cmap,
            scale_type='linear',
            value_label="Population at risk",
            suptitle_template="{title}",
            show=False,
        )
        logger.info("Saved impact polygon map for column: %s (cmap=%s)", col, selected_cmap.name)

    
def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    
    cfg = load_config(**overrides)
    country_boundaries_gdf = gpd.read_file(cfg['paths']['country_boundaries_filepath'])
    country_id_col = 'ISO_A2'  # Change this to the appropriate column name in your GeoDataFrame
    max_workers = 8
    zoom_level = int(cfg['zoom_level'])

    #pop_at_risk_gdf = gpd.read_parquet(cfg['paths']['pop_at_risk_output_filepath'])
    pop_at_risk_gdf = gpd.read_parquet("/mnt/sds-hd/sd17f001/eren/plant-capacity/data/pop_at_risk_pop.parquet")
    tiles_gdf = find_tiles_in_countries(country_boundaries_gdf, zoom_level=zoom_level, country_id_col=country_id_col, max_workers=max_workers)
    create_impact_polygon_plots(pop_at_risk_gdf, tiles_gdf, cfg['paths']['figures_dir'])

    #unserved_df = pd.read_csv(cfg['paths']['raster_country_stats_filepath'], usecols=['tile', 'pop_sum'])
    unserved_df = pd.read_csv("/mnt/sds-hd/sd17f001/eren/plant-capacity/data/non_served_areas.csv", usecols=['tile', 'pop_sum'])
    tiles_gdf = pd.merge(tiles_gdf, unserved_df, on='tile', how='left') 
    create_single_plot(
        z8_stats=tiles_gdf,
        column='pop_sum',
        title='Unserved Population',
        output_filename='unserved_population_tiles.png',
        output_dir=cfg['paths']['figures_dir'],
        cmap=plt.get_cmap('inferno'),
        scale_type='linear',
        value_label='Unserved population',
        suptitle_template='{title}',
        show=False,
    )
    logger.info("Saved unserved population tile map.")

if __name__ == "__main__":
    main()
