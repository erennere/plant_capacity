"""
This script processes the generated annotations to create a validation dataset for multimodal models.
It also adds the category information to the original geospatial dataset for further analysis and visualization.
"""
import os
import shutil
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from starter import load_config

def decode_gen_text(text):
    parts = text.split(':')
    if len(parts) < 2:
        return None, None, None
    
    justification = parts[1].strip()
    context = parts[0].split('.')

    if len(context) < 2:
        return None, None, None
    
    category_number = context[0].strip()
    category_name = context[1].strip()
    
    justification = justification if not justification.startswith('[') else None
    category_name = category_name if not category_name.startswith('[') else None
    category_number = category_number if not category_number.startswith('[') else None  
    return category_number, category_name, justification

def plot_category_distribution(df, column='category_name', save_path=None):
    """
    Generates and displays a bar chart of the categories, including NaNs.
    """
    # 1. Prepare data
    counts = df[column].fillna('NaN').value_counts()

    # 2. Create the plot
    plt.figure(figsize=(12, 6))
    counts.plot(kind='bar', color='skyblue', edgecolor='black')

    # 3. Formatting
    plt.title(f'Distribution of {column}', fontsize=14)
    plt.xlabel(column, fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # 4. Save or Show
    if save_path:
        plt.savefig(save_path)
        print(f"Plot saved to {save_path}")
    plt.show()
    
def get_stratified_sample(df, target_column, total_n=1000, seed=42):
    """
    Returns a random, equally distributed sample of rows.
    Handles NaNs as a category and fills remainder if groups are too small.
    """
    # 1. Create a temporary column to handle NaNs during grouping
    temp_df = df.copy()
    temp_df['temp_grp'] = temp_df[target_column].fillna('NaN')
    
    unique_cats = temp_df['temp_grp'].unique()
    n_per_cat = total_n // len(unique_cats)

    # 2. Sample equally from each category
    sampled = temp_df.groupby('temp_grp', group_keys=False).apply(
        lambda x: x.sample(min(len(x), n_per_cat), random_state=seed)
    )

    # 3. If we are short (due to small categories), fill the gap randomly
    if len(sampled) < total_n:
        shortfall = total_n - len(sampled)
        remaining_pool = temp_df.drop(sampled.index)
        
        # Ensure we don't try to sample more than what's left in the pool
        extra = remaining_pool.sample(min(len(remaining_pool), shortfall), random_state=seed)
        sampled = pd.concat([sampled, extra])
    return sampled.drop(columns=['temp_grp']).reset_index(drop=True)

def organize_files_by_category(df, source_col, category_col, base_dir):
    """
    Creates folders for each category and copies files into them.
    
    Args:
        df: The dataframe containing file paths and categories.
        source_col: The column name containing the current file paths (Column 'X').
        category_col: The column name to use for folder names.
        base_dir: The root directory where folders will be created.
    """
    # Ensure the base directory exists
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"Created base directory: {base_dir}")

    copy_count = 0
    error_count = 0

    for _, row in df.iterrows():
        # Get category name and source path
        cat_name = str(row[category_col]).strip() if pd.notna(row[category_col]) else "Uncategorized"
        src_path = row[source_col]

        # Skip if source path is empty/NaN
        if pd.isna(src_path):
            continue

        # Create the category-specific folder
        target_folder = os.path.join(base_dir, cat_name)
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        # Build the destination path (keeping the original filename)
        filename = os.path.basename(src_path)
        dest_path = os.path.join(target_folder, filename)

        # Copy the file
        try:
            if os.path.exists(src_path):
                shutil.copy2(src_path, dest_path) # copy2 preserves metadata
                copy_count += 1
            else:
                print(f"File not found: {src_path}")
                error_count += 1
        except Exception as e:
            print(f"Error copying {filename}: {e}")
            error_count += 1
    print(f"Done! Files copied: {copy_count} | Errors: {error_count}")

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()

    image_input_dir = os.path.abspath(cfg['paths']['annotated_images_output_dir'])
    image_output_dir = os.path.abspath(cfg['paths']['annotations_verf_image_outpath_dir'])
    filepath = os.path.abspath(cfg['paths']['annotations_results_filepath'])

    df = pd.read_csv(filepath)
    df[['category_number', 'category_name', 'justification']] = df['gen_text'].apply(
        lambda x: pd.Series(decode_gen_text(x))
    )
    df['filepath'] = df['image'].apply(lambda x: os.path.join(image_input_dir, x))

    #plot_category_distribution(df, 'category_name')
    df_sampled = get_stratified_sample(df, 'category_name', total_n=1000)
    print(f"New shape: {df_sampled.shape}")
    print(df_sampled['category_name'].value_counts(dropna=False))

    organize_files_by_category(df_sampled, 'filepath', 'category_name', image_output_dir)


    points_df = gpd.read_file(os.path.abspath(cfg['paths']['corrected_all_filepath']))
    points_df['idx'] = points_df['idx'].astype(int)

    df['idx'] = df['image'].apply(lambda x: int(x.split('.')[0].split('_')[-1]))
    df.drop(axis=1, inplace=True, columns=['filepath', 'image', 'gen_text'])

    # Merge the points_df with df on 'idx'
    merged_df = gpd.GeoDataFrame(pd.merge(points_df, df, on='idx', how='left'), geometry='geometry', crs=points_df.crs)
    merged_df.to_file(index=False, driver='GPKG', filename=os.path.abspath(cfg['paths']['corrected_all_filepath']))

if __name__ == "__main__":
    main()  


