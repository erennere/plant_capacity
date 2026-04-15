"""
This script performs a comparative analysis of wastewater
treatment plant (WWTP) data across different years using two approaches:
    - Normalized Difference Index (NDI) and 
    - a population-based comparison(HydroWaste QUAL_POP = 1 which comes from governmental agencies).
"""
import os
import re
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from starter import load_config

def ndvi(df, col1, col2, new_col):
    df = df.copy()
    df[new_col] = (df[col1] - df[col2]) / (df[col1] + df[col2])
    return df

def get_approach(filename):
    result = None
    match = re.search(r'_appr_([^_]+)_', filename)
    if match:
        result = match.group(1)
    return result

def multiples(df, col1, col2, new_col):
    df = df.copy()
    df[new_col] = (df[col1]-df[col2])/df[col2] + 1
    return df

def replace_inf(df, col):
    df = df.copy()
    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
    return df

def composite_histogram(data, my_dict, title, output_filepath=None, save=False, dpi=300,
                        ylabel='N_WWTPs', xlabel=None, bins=100, lower_quantile=0.01, upper_quantile=0.95,
                        fontsize=26, small_fontsize=18):
    fig, axes = plt.subplots(2, 5, figsize=(15, 6))

    pastel_colors = sns.color_palette("pastel", n_colors=len(my_dict))

    for i, (year, col_name) in enumerate(my_dict.items()):
        row, col = divmod(i, 5)
        ax = axes[row, col]

        # CHECK IF COLUMN EXISTS AND HAS DATA
        if col_name not in data.columns or data[col_name].dropna().empty:
            ax.set_title(f'{year} (No Data)')
            ax.axis('off') # Hide empty plots
            continue
        
        color = pastel_colors[i]

        vmin = data[col_name].quantile(lower_quantile)
        vmax = data[col_name].quantile(upper_quantile)

        # CHECK IF QUANTILES ARE FINITE
        if np.isnan(vmin) or np.isnan(vmax):
            ax.set_title(f'{year} (Invalid Range)')
            continue
        
        subset = data[(data[col_name] >= vmin) & (data[col_name] <= vmax)][col_name]
        if subset.empty:
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
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    if save and output_filepath:
        plt.savefig(output_filepath, dpi=dpi)

    plt.show()
    plt.close(fig)

def orchestrate_single(gdf, approach, plot_args, output_dir, filename, pop_col='POP_SERVED'):
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
        pop_file = gdf[(gdf['QUAL_POP'] == 1) & (gdf[col].notna())]
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
    upper_quantile_hw_comp = 0.9
    ndi_output_filepath = os.path.join(output_dir, f'ndi_{filename.replace('.gpkg', '.png')}')
    hw_comp_output_filepath = os.path.join(output_dir, f'hw_comp_{filename.replace('.gpkg', '.png')}')

    ndi_title = f'Normalized Difference Index (NDI) w.r.t. HydroWaste, approach: {approach}\n ver: {verified}'
    hw_comp_title = fr'Population = $\alpha\cdot$HydroWaste, approach: {approach}' + f'\n ver: {verified}'
    composite_histogram(gdf, ndi_dict, ndi_title, output_filepath=ndi_output_filepath, ylabel=ylabel, xlabel=xlabel_ndi,
                         upper_quantile=upper_quantile_ndi, **plot_args)
    composite_histogram(gdf, HW_comp_dict, hw_comp_title, output_filepath=hw_comp_output_filepath, ylabel=ylabel, xlabel=xlabel_hW_comp,
                        upper_quantile=upper_quantile_hw_comp, **plot_args)
    
def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    globals().update(cfg)
    ver_dir = paths['verification_dir']
    plots_dir = paths['hw_plots_dir']
    pop_filepaths = [os.path.join(ver_dir, f) for f in os.listdir(ver_dir) if f.endswith('.gpkg')]
    plot_args = {
    'dpi' : 300,
    'bins' : 100,
    'save' : True,
    'fontsize' : 26,
    'small_fontsize' : 18,
    'lower_quantile' : 0.01}
    pop_col = 'POP_SERVED'
    for filepath in pop_filepaths:
        filename = os.path.basename(filepath)
        approach = get_approach(filename)
        gdf = gpd.read_file(filepath)
        orchestrate_single(gdf, approach, plot_args, plots_dir, filename, pop_col)

if __name__ == '__main__':
    main()