"""Create composite histogram and scatter diagnostics for area/population ratios.

Plot A: two histograms
- total_area / zonal_sum
- round_area / total_area

Plot B: two scatter plots with 1:1 reference lines
- country aggregate vs median plant ratio for total_area / zonal_sum
- country aggregate vs median plant ratio for round_area / total_area
"""

import argparse
import os

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    from ..starter import load_config, parse_config_overrides
    from ..pipelines import create_pop_output_paths
    from ..create_voronoi import ensure_output_dir_for_file
except ImportError:
    from research_code.starter import load_config, parse_config_overrides
    from research_code.pipelines import create_pop_output_paths
    from research_code.create_voronoi import ensure_output_dir_for_file


def resolve_zonal_sum_column(df, preferred):
    """Return preferred zonal-sum column or fallback to latest available year."""
    if preferred in df.columns:
        return preferred

    candidates = []
    for col in df.columns:
        if not col.endswith("_zonal_sum"):
            continue
        try:
            year = int(col.split("_")[0])
        except (ValueError, IndexError):
            year = -1
        candidates.append((year, col))

    if not candidates:
        raise KeyError("No '*_zonal_sum' column found in Voronoi population file.")

    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


def clip_outliers(series, lower_q, upper_q):
    """Drop outliers by quantile clipping for cleaner histograms."""
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return s
    q_lo = float(s.quantile(lower_q))
    q_hi = float(s.quantile(upper_q))
    return s[(s >= q_lo) & (s <= q_hi)]


def _bleach_color(color, amount=0.35):
    """Blend a color toward white for a bleached pastel appearance."""
    rgb = np.array(color[:3], dtype=float)
    white = np.ones(3, dtype=float)
    return tuple((1.0 - amount) * rgb + amount * white)


def make_category_color_map(values):
    """Create deterministic pastel colors per category value."""
    categories = sorted(pd.Series(values).fillna("Unknown").astype(str).unique())
    base = plt.get_cmap("tab20")
    colors = {}
    for idx, cat in enumerate(categories):
        colors[cat] = _bleach_color(base(idx % 20), amount=0.45)
    return colors


def add_one_to_one_line(ax, x_vals, y_vals):
    """Add y=x dashed line spanning current value range."""
    combined = pd.concat([x_vals, y_vals], axis=0)
    combined = pd.to_numeric(combined, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if combined.empty:
        return
    lo = float(combined.min())
    hi = float(combined.max())
    if hi <= lo:
        hi = lo + 1e-6
    ax.plot([lo, hi], [lo, hi], linestyle="--", linewidth=1.2, color="black", alpha=0.8)


def build_country_table(pop_df, boundaries, zonal_col, color_col):
    """Create merged country table with required aggregate and median metrics."""
    d = pop_df.copy()

    d["ratio_area_pop"] = np.where(
        pd.to_numeric(d[zonal_col], errors="coerce") > 0,
        pd.to_numeric(d["total_area"], errors="coerce") / pd.to_numeric(d[zonal_col], errors="coerce"),
        np.nan,
    )
    d["ratio_round_total"] = np.where(
        pd.to_numeric(d["total_area"], errors="coerce") > 0,
        pd.to_numeric(d["round_area"], errors="coerce") / pd.to_numeric(d["total_area"], errors="coerce"),
        np.nan,
    )

    grouped = d.groupby("ISO_2", as_index=False).agg(
        total_area_sum=("total_area", "sum"),
        zonal_sum_sum=(zonal_col, "sum"),
        round_area_sum=("round_area", "sum"),
        ratio_area_pop_median=("ratio_area_pop", "median"),
        ratio_round_total_median=("ratio_round_total", "median"),
    )

    grouped["ratio_area_pop_agg"] = np.where(
        grouped["zonal_sum_sum"] > 0,
        grouped["total_area_sum"] / grouped["zonal_sum_sum"],
        np.nan,
    )
    grouped["ratio_round_total_agg"] = np.where(
        grouped["total_area_sum"] > 0,
        grouped["round_area_sum"] / grouped["total_area_sum"],
        np.nan,
    )

    boundary_id_col = "ISO_A2" if "ISO_A2" in boundaries.columns else "ISO_A2_EH"
    merged = grouped.merge(
        boundaries[[boundary_id_col, color_col]].rename(columns={boundary_id_col: "ISO_A2"}),
        left_on="ISO_2",
        right_on="ISO_A2",
        how="left",
    )
    merged[color_col] = merged[color_col].fillna("Unknown").astype(str)
    return merged


def make_histogram_plot(pop_df, zonal_col, out_path, lower_q, upper_q):
    """Generate and save Plot A with two side-by-side histograms."""
    d = pop_df.copy()
    ratio_area_pop = np.where(
        pd.to_numeric(d[zonal_col], errors="coerce") > 0,
        pd.to_numeric(d["total_area"], errors="coerce") / pd.to_numeric(d[zonal_col], errors="coerce"),
        np.nan,
    )
    ratio_round_total = np.where(
        pd.to_numeric(d["total_area"], errors="coerce") > 0,
        pd.to_numeric(d["round_area"], errors="coerce") / pd.to_numeric(d["total_area"], errors="coerce"),
        np.nan,
    )

    s_left = clip_outliers(pd.Series(ratio_area_pop), lower_q=lower_q, upper_q=upper_q)
    s_right = clip_outliers(pd.Series(ratio_round_total), lower_q=lower_q, upper_q=upper_q)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6), dpi=300)

    axes[0].hist(s_left, bins=60, color="#90caf9", edgecolor="white", linewidth=0.6)
    axes[0].set_title("Plot A1: Distribution of Total Area / Population Proxy", fontsize=12)
    axes[0].set_xlabel(f"total_area / {zonal_col}")
    axes[0].set_ylabel("Count")
    axes[0].grid(True, alpha=0.25)

    axes[1].hist(s_right, bins=60, color="#ffcc80", edgecolor="white", linewidth=0.6)
    axes[1].set_title("Plot A2: Distribution of Circular Fraction", fontsize=12)
    axes[1].set_xlabel("round_area / total_area")
    axes[1].set_ylabel("Count")
    axes[1].grid(True, alpha=0.25)

    fig.suptitle("Plot A: Facility-Level Ratio Distributions (Outliers Trimmed)", fontsize=14, y=1.02)
    fig.tight_layout()
    ensure_output_dir_for_file(out_path)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def make_scatter_plot(country_df, color_col, out_path):
    """Generate and save Plot B with two side-by-side scatter plots and 1:1 lines."""
    fig, axes = plt.subplots(1, 2, figsize=(17, 7), dpi=300)

    color_map = make_category_color_map(country_df[color_col])
    point_colors = country_df[color_col].map(color_map)

    x1 = pd.to_numeric(country_df["ratio_area_pop_agg"], errors="coerce")
    y1 = pd.to_numeric(country_df["ratio_area_pop_median"], errors="coerce")

    axes[0].scatter(x1, y1, s=46, c=point_colors, alpha=0.9, edgecolors="white", linewidths=0.7)
    add_one_to_one_line(axes[0], x1, y1)
    axes[0].set_title("Plot B1: Aggregate vs Median (Total Area / Population Proxy)", fontsize=12)
    axes[0].set_xlabel("sum(total_area) / sum(zonal_sum)")
    axes[0].set_ylabel("median(total_area / zonal_sum)")
    axes[0].grid(True, alpha=0.3)

    for _, row in country_df.iterrows():
        xx = pd.to_numeric(row["ratio_area_pop_agg"], errors="coerce")
        yy = pd.to_numeric(row["ratio_area_pop_median"], errors="coerce")
        if np.isfinite(xx) and np.isfinite(yy):
            axes[0].text(xx, yy, str(row["ISO_A2"]), fontsize=6.5, color=color_map[str(row[color_col])], alpha=0.9)

    x2 = pd.to_numeric(country_df["ratio_round_total_agg"], errors="coerce")
    y2 = pd.to_numeric(country_df["ratio_round_total_median"], errors="coerce")

    axes[1].scatter(x2, y2, s=46, c=point_colors, alpha=0.9, edgecolors="white", linewidths=0.7)
    add_one_to_one_line(axes[1], x2, y2)
    axes[1].set_title("Plot B2: Aggregate vs Median (Circular Fraction)", fontsize=12)
    axes[1].set_xlabel("sum(round_area) / sum(total_area)")
    axes[1].set_ylabel("median(round_area / total_area)")
    axes[1].grid(True, alpha=0.3)

    for _, row in country_df.iterrows():
        xx = pd.to_numeric(row["ratio_round_total_agg"], errors="coerce")
        yy = pd.to_numeric(row["ratio_round_total_median"], errors="coerce")
        if np.isfinite(xx) and np.isfinite(yy):
            axes[1].text(xx, yy, str(row["ISO_A2"]), fontsize=6.5, color=color_map[str(row[color_col])], alpha=0.9)

    handles = []
    labels = []
    for cat, color in color_map.items():
        handles.append(plt.Line2D([], [], marker="o", linestyle="", color=color, markeredgecolor="white", markersize=7))
        labels.append(cat)

    if len(labels) <= 20:
        axes[1].legend(handles, labels, title=color_col, fontsize=8, title_fontsize=9, loc="best")

    fig.suptitle("Plot B: Country-Level Ratio Consistency Diagnostics", fontsize=14, y=1.02)
    fig.tight_layout()
    ensure_output_dir_for_file(out_path)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def parse_args():
    """Parse CLI arguments for composite figure generation."""
    parser = argparse.ArgumentParser(description="Generate composite histograms and scatter plots.")
    parser.add_argument("--approach", type=str, default=None, help="Approach key for create_pop_output_paths (e.g., 0, 1, 2, 1_only_round)")
    parser.add_argument("--color-col", type=str, default="ECONOMY", help="Boundary column used for categorical color coding.")
    parser.add_argument("--zonal-col", type=str, default=None, help="Override zonal-sum column. Defaults to config zonal_sum_default_column.")
    parser.add_argument("--hist-lower-q", type=float, default=0.01, help="Lower quantile for histogram outlier trimming.")
    parser.add_argument("--hist-upper-q", type=float, default=0.99, help="Upper quantile for histogram outlier trimming.")

    parser.add_argument("level", nargs="?", default=None, help="Optional config level override")
    parser.add_argument("version", nargs="?", default=None, help="Optional config version override")
    parser.add_argument("buffer", nargs="?", default=None, help="Optional config buffer override")
    parser.add_argument("weight_method", nargs="?", default=None, help="Optional config weight_method override")
    parser.add_argument("weight_func", nargs="?", default=None, help="Optional config weight_func override")
    parser.add_argument("dynamic_buffering", nargs="?", default=None, help="Optional dynamic buffering override")
    parser.add_argument("dynamic_buffer_k", nargs="?", default=None, help="Optional dynamic buffer scaling override")
    return parser.parse_args()


def main():
    """Entry point for composite ratio figure generation."""
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parse_args()
    overrides = parse_config_overrides(args=args)
    cfg = load_config(**overrides)

    approach = str(args.approach if args.approach is not None else cfg["figures"].get("approach", 1))
    pop_path = os.path.abspath(create_pop_output_paths(cfg)["voronoi"][approach])
    boundaries_path = cfg["paths"]["country_boundaries_filepath"]

    pop_df = gpd.read_file(pop_path)
    boundaries = gpd.read_file(boundaries_path)

    required_cols = {"ISO_2", "total_area", "round_area"}
    missing = required_cols - set(pop_df.columns)
    if missing:
        raise KeyError(f"Missing required columns in pop filepath: {sorted(missing)}")

    zonal_col = resolve_zonal_sum_column(pop_df, args.zonal_col or cfg["zonal_sum_default_column"])

    color_col = args.color_col
    if color_col not in boundaries.columns:
        raise KeyError(f"Color column '{color_col}' was not found in boundaries file.")

    make_histogram_plot(
        pop_df=pop_df,
        zonal_col=zonal_col,
        out_path=cfg["paths"]["composite_histogram_filepath"],
        lower_q=args.hist_lower_q,
        upper_q=args.hist_upper_q,
    )

    country_df = build_country_table(pop_df=pop_df, boundaries=boundaries, zonal_col=zonal_col, color_col=color_col)
    make_scatter_plot(
        country_df=country_df,
        color_col=color_col,
        out_path=cfg["paths"]["composite_scatter_filepath"],
    )


if __name__ == "__main__":
    main()
