"""Compare WWTP-derived served-population estimates against the EU reference layer.

The script joins project outputs to the UWWTD reference dataset, computes both
normalized difference and multiplicative comparison metrics, and exports yearly
histogram panels for verification subsets.
"""
import os
import logging
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

try:
    from .hw_comparison import ndvi, multiples, replace_inf, extract_voronoi_parameters
    from ..starter import load_config, parse_config_overrides
    from ..create_voronoi import ensure_output_dir_for_file
    from ..data_merge.merge_seg_results import assign_to_nearest
except ImportError:
    from src.pop_validation_scripts.hw_comparison import ndvi, multiples, replace_inf, extract_voronoi_parameters
    from src.starter import load_config, parse_config_overrides
    from src.create_voronoi import ensure_output_dir_for_file
    from src.data_merge.merge_seg_results import assign_to_nearest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def composite_histogram(data, my_dict, title, output_filepath=None, save=False, dpi=300,
                        ylabel='N_WWTPs', xlabel=None, bins=100, lower_quantile=0.01, upper_quantile=0.95,
                        fontsize=26, small_fontsize=18):
    """Plot a grid of histograms for EU-reference comparison metrics by year."""
    fig, axes = plt.subplots(2, 5, figsize=(15, 6))

    pastel_colors = sns.color_palette("pastel", n_colors=len(my_dict))

    for i, (year, col_name) in enumerate(my_dict.items()):
        row, col = divmod(i, 5)
        ax = axes[row, col]

        # CHECK IF COLUMN EXISTS AND HAS DATA
        if col_name not in data.columns or data[col_name].dropna().empty:
            logger.warning(f"Column '{col_name}' is missing or empty in the data. Skipping plot for year {year}.")
            ax.set_title(f'{year} (No Data)')
            #ax.axis('off') # Hide empty plots
            continue
            
        color = pastel_colors[i]

        vmin = data[col_name].quantile(lower_quantile)
        vmax = data[col_name].quantile(upper_quantile)

        # CHECK IF QUANTILES ARE FINITE
        if np.isnan(vmin) or np.isnan(vmax):
            logger.warning(f"Column '{col_name}' has invalid quantile range for year {year}. Skipping plot.")
            ax.set_title(f'{year} (Invalid Range)')
            continue
        
        subset = data[(data[col_name] >= vmin) & (data[col_name] <= vmax)][col_name]
        if subset.empty:
            logger.warning(f"Column '{col_name}' has no data within quantile range for year {year}. Skipping plot.")
            continue
        
        # Plot histogram
        ax.hist(subset, bins=bins, range=(vmin, vmax), color=color, edgecolor='black')

        mean_val = subset.mean()
        median_val = subset.median()
        ax.axvline(mean_val, color='black', linestyle='--', linewidth=1.5, label=f'Mean: {mean_val:.2f}')
        ax.axvline(median_val, color='gray', linestyle='--', linewidth=1.5, label=f'Median: {median_val:.2f}')

        N = len(subset)
        ax.set_title(f'{year}, N : {N}', fontsize=small_fontsize)

        if col != 0:
            ax.set_yticklabels([])
        else:
            ax.set_ylabel(ylabel, fontsize=small_fontsize)
        if row == 1:
            ax.set_xlabel(xlabel if xlabel else '', fontsize=small_fontsize)

        ax.grid(True)
        ax.legend(fontsize=8, loc='upper right', frameon=False)

    fig.suptitle(title, fontsize=fontsize)
    plt.tight_layout(rect=(0, 0, 1, 0.95))
    if save and output_filepath:
        ensure_output_dir_for_file(output_filepath)
        plt.savefig(output_filepath, dpi=dpi)

    plt.show()
    plt.close(fig)

def orchestrate_single(gdf, approach, plot_args, output_dir, filename, pop_col='POP_SERVED'):
    """Compute EU comparison metrics for one verification file and save plots.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Verification subset to analyse.
    approach : str | None
        Approach identifier parsed from the file name.
    plot_args : dict
        Keyword arguments forwarded to ``composite_histogram``.
    output_dir : str
        Directory where plot images are written.
    filename : str
        Source verification filename.
    pop_col : str, default='POP_SERVED'
        Population reference column used for comparisons.
    """
    if pop_col not in gdf.columns:
        raise KeyError(f"Missing required population column '{pop_col}'")

    years_and_cols = dict(sorted({int(col.split('_')[0]): col for col in gdf.columns if col.endswith('_zonal_sum')}.items()))
    ndi_dict = {}
    HW_comp_dict = {}
    gdf['indx'] = range(len(gdf))
    
    verified = 'single'
    if 'unver' in filename:
        verified = False
    elif 'ver' in filename:
        verified = True

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    for year, col in years_and_cols.items():
        if year == 2014:
            continue
        ndi_col = f'{year}_NDI'
        HW_comp_col = f'{year}_HW_comp'
        ndi_dict[year] = ndi_col
        HW_comp_dict[year] = HW_comp_col

        pop_file = gdf[(gdf[pop_col].notna()) & (gdf[col].notna())]
        pop_file = ndvi(pop_file, col, pop_col, ndi_col)
        pop_file = multiples(pop_file, col, pop_col, HW_comp_col)
        pop_file = replace_inf(pop_file, ndi_col)
        pop_file = replace_inf(pop_file, HW_comp_col)
        pop_file = pop_file[['indx', ndi_col, HW_comp_col]]
        gdf =  pd.merge(
            gdf,
            pop_file,
            on='indx',
            how='left'
        )
    
    ylabel = 'N_WWTPs' 
    xlabel_ndi = 'NDI'
    xlabel_hW_comp = r'$\alpha$'
    upper_quantile_ndi = 0.99
    upper_quantile_hw_comp = 0.95
    ndi_output_filepath = os.path.join(output_dir, f"ndi_{filename.replace('.gpkg', '.png')}")
    hw_comp_output_filepath = os.path.join(output_dir, f"eu_comp_{filename.replace('.gpkg', '.png')}")

    ndi_title = f'Normalized Difference Index (NDI) w.r.t. Reference EU, approach: {approach}\n ver: {verified}'
    hw_comp_title = fr'Population = $\alpha\cdot$Reference EU, approach: {approach}' + f'\n ver: {verified}'
    composite_histogram(gdf, ndi_dict, ndi_title, output_filepath=ndi_output_filepath, ylabel=ylabel, xlabel=xlabel_ndi,
                         upper_quantile=upper_quantile_ndi, **plot_args)
    composite_histogram(gdf, HW_comp_dict, hw_comp_title, output_filepath=hw_comp_output_filepath, ylabel=ylabel, xlabel=xlabel_hW_comp,
                        upper_quantile=upper_quantile_hw_comp, **plot_args)

def main():
    """Load verification files, align them to the EU reference layer, and plot comparisons.

    Returns
    -------
    None
        The function iterates over configured verification files and writes the
        generated comparison plots to disk.
    """
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    overrides = parse_config_overrides(start_index=1)
    cfg = load_config(script_name="eu_comparison", **overrides)
    ver_dir = cfg['paths']['verification_dir']
    plots_dir = cfg['paths']['eu_plots_dir']
    if not os.path.isdir(ver_dir):
        logger.warning("Verification directory not found: %s", ver_dir)
        return

    pop_filepaths = [os.path.join(ver_dir, f) for f in os.listdir(ver_dir) if f.endswith('.gpkg')]
    plot_args = {
    'dpi' : 300,
    'bins' : 100,
    'save' : True,
    'fontsize' : 26,
    'small_fontsize' : 18,
    'lower_quantile' : 0.01}

    factor = 1
    threshold = cfg['threshold']
    utm = 3857 
    pop_col = 'POP_SERVED_EU'
    ref_filepath = cfg['paths']['eu_ref_filepath']
    organic_m_column = 'uwwCapacity'

    ref_file = gpd.read_file(ref_filepath)
    if organic_m_column not in ref_file.columns:
        raise KeyError(f"Reference column '{organic_m_column}' not found in EU reference layer")
    ref_file = ref_file.to_crs(utm)
    ref_file[pop_col] = factor*ref_file[organic_m_column]

    for filepath in pop_filepaths:
        filename = os.path.basename(filepath)
        params = extract_voronoi_parameters(filepath)
        approach = params.get('approach') if isinstance(params, dict) else None
        gdf = gpd.read_file(filepath)
        gdf = assign_to_nearest(gdf, ref_file, threshold)
        gdf = gdf[gdf[organic_m_column].notna()].reset_index(drop=True)
        orchestrate_single(gdf, approach, plot_args, plots_dir, filename, pop_col)

if __name__ == '__main__':
    main()