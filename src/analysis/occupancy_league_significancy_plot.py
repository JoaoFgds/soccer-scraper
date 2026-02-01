"""
Occupancy League Significance Plot - Creates scatter plots showing the relationship
between mean occupancy difference and -log10(p-value) (significance) for each league.

This script computes per-league metrics using pooled team-season data:
- X-axis: -log10(p-value) from Mann-Whitney U test
- Y-axis: Mean difference (unbalanced - balanced) in average_occupancy

Two classification modes are available:
1. G-type mode (default): Uses pre-computed G_type columns with fixed thresholds
2. P-value mode: Uses Spearman p-value to dynamically classify schedules

Usage:
    # Single threshold (default G-type 0.300)
    python -m src.analysis.occupancy_league_significancy_plot
    
    # All thresholds from parameters.py
    python -m src.analysis.occupancy_league_significancy_plot --all
    
    # Custom single threshold
    python -m src.analysis.occupancy_league_significancy_plot --mode gtype --threshold 0.250
    python -m src.analysis.occupancy_league_significancy_plot --mode pvalue --p-threshold 0.10
"""

import argparse
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from pathlib import Path
from typing import Tuple, Optional
from scipy import stats

from src.utils.paths import (
    GOLD_DATA_DIR,
    SPEARMAN_BALANCE_PATH,
    MANN_WHITNEY_ATT_AUDIT_DIR,
)
from src.utils.logger import setup_logging
from src.analysis.parameters import (
    G_TYPE_THRESHOLDS,
    P_VALUE_THRESHOLDS,
    DEFAULT_G_TYPE_THRESHOLD,
    DEFAULT_P_VALUE_THRESHOLD,
)

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_BALANCE_PATH = SPEARMAN_BALANCE_PATH
DEFAULT_OCCUPANCY_PATH = MANN_WHITNEY_ATT_AUDIT_DIR / "occupancy_audit_audience_filled_fb.csv"
DEFAULT_OUTPUT_DIR = GOLD_DATA_DIR / "analysis" / "occupancy_league_significance"

# Classification modes
MODE_GTYPE = "gtype"
MODE_PVALUE = "pvalue"

# Color and marker configurations for leagues
MARKERS = ['o', 's', '^', 'D', 'v', 'p', '*', 'h', 'X', 'P', '<', '>', '8', 'd', 'H', '+', 'x']
COLORS = plt.cm.tab20.colors


def load_schedule_balance(csv_path: Path) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    logger.info(f"Loading schedule balance data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} records from {df['league_name'].nunique()} leagues")
    return df


def load_occupancy_data(csv_path: Path) -> pd.DataFrame:
    """Load the occupancy audit CSV file."""
    logger.info(f"Loading occupancy data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} occupancy records")
    return df


def merge_data(balance_df: pd.DataFrame, occupancy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge schedule balance data with occupancy data.
    
    Join on: league_name, season_year, team_canonical
    """
    logger.info("Merging schedule balance with occupancy data...")
    
    merged = pd.merge(
        balance_df,
        occupancy_df[['league_name', 'season_year', 'team_canonical', 'average_occupancy']],
        on=['league_name', 'season_year', 'team_canonical'],
        how='inner'
    )
    
    logger.info(f"Merged dataset has {len(merged)} records "
                f"({len(balance_df)} balance records, {len(occupancy_df)} occupancy records)")
    
    # Report any missing matches
    missing_count = len(balance_df) - len(merged)
    if missing_count > 0:
        logger.warning(f"{missing_count} balance records had no matching occupancy data")
    
    return merged


def classify_by_pvalue(df: pd.DataFrame, p_threshold: float) -> pd.DataFrame:
    """
    Dynamically classify teams based on Spearman p-value and G sign.
    
    Classification logic:
    - If P_value < p_threshold AND G < 0 → unbalanced_weak
    - If P_value < p_threshold AND G > 0 → unbalanced_strong
    - If P_value >= p_threshold → balanced
    """
    df = df.copy()
    
    def classify_row(row):
        if pd.isna(row['P_value']) or pd.isna(row['G']):
            return 'balanced'
        if row['P_value'] < p_threshold:
            if row['G'] < 0:
                return 'unbalanced_weak'
            elif row['G'] > 0:
                return 'unbalanced_strong'
        return 'balanced'
    
    df['g_type_dynamic'] = df.apply(classify_row, axis=1)
    
    dist = df['g_type_dynamic'].value_counts()
    logger.info(f"P-value classification (threshold={p_threshold}): {dist.to_dict()}")
    
    return df


def compute_league_metrics(
    df: pd.DataFrame,
    league_name: str,
    g_type_col: str,
    unbalanced_type: str
) -> dict:
    """
    Compute mean difference and Mann-Whitney p-value for a single league.
    
    Each data point is the average_occupancy of a team in a season.
    """
    league_df = df[df['league_name'] == league_name]
    
    # Get occupancy values for each group (pooled across all seasons)
    unbalanced = league_df[league_df[g_type_col] == unbalanced_type]['average_occupancy'].values
    balanced = league_df[league_df[g_type_col] == 'balanced']['average_occupancy'].values
    
    n_unbal = len(unbalanced)
    n_bal = len(balanced)
    
    # Compute means
    mean_unbal = np.mean(unbalanced) if n_unbal > 0 else np.nan
    mean_bal = np.mean(balanced) if n_bal > 0 else np.nan
    
    # Compute mean difference
    mean_diff = mean_unbal - mean_bal if (n_unbal > 0 and n_bal > 0) else np.nan
    
    # Compute Mann-Whitney p-value
    p_value = np.nan
    stat = np.nan
    if n_unbal >= 2 and n_bal >= 2:
        try:
            stat, p_value = stats.mannwhitneyu(
                unbalanced, balanced, alternative='two-sided'
            )
        except Exception as e:
            logger.warning(f"Mann-Whitney test failed for {league_name}: {e}")
    
    # Compute -log10(p-value)
    neg_log_p = np.nan if (pd.isna(p_value) or p_value == 0) else -np.log10(p_value)
    
    return {
        'league_name': league_name,
        'mean_difference': mean_diff,
        'mann_whitney_stat': stat,
        'p_value': p_value,
        'neg_log_p': neg_log_p,
        'n_unbalanced': n_unbal,
        'n_balanced': n_bal,
        'mean_unbalanced': mean_unbal,
        'mean_balanced': mean_bal,
    }


def compute_all_leagues_metrics(
    df: pd.DataFrame,
    g_type_col: str,
    unbalanced_type: str
) -> pd.DataFrame:
    """Compute metrics for all leagues."""
    leagues = df['league_name'].unique()
    results = []
    
    for league in leagues:
        metrics = compute_league_metrics(df, league, g_type_col, unbalanced_type)
        results.append(metrics)
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('mean_difference', key=abs, ascending=False)
    return results_df


def create_scatter_plot(
    metrics_df: pd.DataFrame,
    comparison_label: str,
    output_path: str,
    threshold_label: str
) -> None:
    """Create scatter plot with unique marker/color per league."""
    plot_df = metrics_df.dropna(subset=['mean_difference', 'neg_log_p']).copy()
    
    if len(plot_df) == 0:
        logger.warning(f"No valid data for {comparison_label}")
        return
    
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_axes([0.08, 0.12, 0.55, 0.78])
    
    league_styles = {}
    for i, league in enumerate(plot_df['league_name'].values):
        league_styles[league] = {
            'color': COLORS[i % len(COLORS)],
            'marker': MARKERS[i % len(MARKERS)]
        }
    
    for _, row in plot_df.iterrows():
        league = row['league_name']
        style = league_styles[league]
        ax.scatter(
            row['neg_log_p'], row['mean_difference'],
            c=[style['color']], marker=style['marker'],
            s=120, edgecolors='black', linewidths=0.5, alpha=0.85
        )
    
    # Reference lines
    p_05_line = -np.log10(0.05)
    ax.axvline(x=p_05_line, color='red', linestyle='--', alpha=0.5, linewidth=1.5)
    ax.text(p_05_line + 0.05, ax.get_ylim()[1] * 0.95, 'p = 0.05', 
            color='red', fontsize=9, alpha=0.7)
    ax.axhline(y=0, color='gray', linestyle='-', alpha=0.3, linewidth=1)
    
    ax.set_xlabel('-log₁₀(p-value)', fontsize=12)
    ax.set_ylabel('Mean Difference (Unbalanced - Balanced)', fontsize=12)
    ax.set_title(
        f"Occupancy Significance by League\n({comparison_label} | {threshold_label})",
        fontsize=14, fontweight='bold'
    )
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    
    # Quadrant labels
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    text_x = xlim[1] * 0.85
    y_range = ylim[1] - ylim[0]
    ax.text(text_x, ylim[1] - 0.1 * y_range, 'Higher attendance\n(significant)', 
            fontsize=8, ha='center', alpha=0.5, style='italic')
    ax.text(text_x, ylim[0] + 0.1 * y_range, 'Lower attendance\n(significant)', 
            fontsize=8, ha='center', alpha=0.5, style='italic')
    
    # Legend
    legend_ax = fig.add_axes([0.68, 0.12, 0.30, 0.78])
    legend_ax.axis('off')
    
    legend_elements = []
    legend_labels = []
    for league in plot_df['league_name'].values:
        style = league_styles[league]
        element = Line2D(
            [0], [0], marker=style['marker'], color='w',
            markerfacecolor=style['color'], markeredgecolor='black',
            markeredgewidth=0.5, markersize=10, linestyle='None'
        )
        legend_elements.append(element)
        row = plot_df[plot_df['league_name'] == league].iloc[0]
        legend_labels.append(f"{league} (Δ={row['mean_difference']:.3f}, p={row['p_value']:.3f})")
    
    legend_ax.legend(
        legend_elements, legend_labels, loc='upper left',
        fontsize=8, frameon=True, fancybox=True, shadow=False,
        title='League (Δ, p-value)', title_fontsize=10
    )
    
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"Saved plot to {output_path}")


def print_summary_statistics(
    metrics_g_minus: pd.DataFrame,
    metrics_g_plus: pd.DataFrame,
    threshold_label: str
) -> None:
    """Print summary statistics for both comparisons."""
    print("\n" + "=" * 70)
    print(f"OCCUPANCY LEAGUE SIGNIFICANCE SUMMARY ({threshold_label})")
    print("=" * 70)
    
    for label, df in [("G- vs G0", metrics_g_minus), ("G+ vs G0", metrics_g_plus)]:
        valid_df = df.dropna(subset=['mean_difference', 'p_value'])
        significant = valid_df[valid_df['p_value'] < 0.05]
        
        print(f"\n{label}:")
        print(f"  Leagues analyzed: {len(valid_df)}")
        print(f"  Significant (p < 0.05): {len(significant)}")
        
        if len(valid_df) > 0:
            print(f"  Mean difference: {valid_df['mean_difference'].mean():.4f}")
            print(f"  Median difference: {valid_df['mean_difference'].median():.4f}")
            print(f"  Range: [{valid_df['mean_difference'].min():.4f}, {valid_df['mean_difference'].max():.4f}]")
        
        if len(significant) > 0:
            print(f"\n  Significant leagues:")
            for _, row in significant.iterrows():
                effect_dir = "higher" if row['mean_difference'] > 0 else "lower"
                print(f"    - {row['league_name']}: Δ={row['mean_difference']:.4f} ({effect_dir}), p={row['p_value']:.4f}")


def save_metrics_to_csv(metrics_df: pd.DataFrame, output_path: Path, comparison_label: str) -> None:
    """Save metrics DataFrame to CSV."""
    metrics_df.to_csv(output_path, index=False)
    logger.info(f"Saved {comparison_label} metrics to {output_path}")


def run_analysis(
    df: pd.DataFrame,
    g_type_col: str,
    threshold_label: str,
    output_dir: Path
) -> None:
    """
    Run the full analysis pipeline for a given classification column.
    
    Args:
        df: Merged DataFrame with classification and occupancy data
        g_type_col: Name of the classification column
        threshold_label: Human-readable label for the threshold
        output_dir: Directory for output files (threshold-specific subfolder)
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Compute metrics for G- vs G0
    logger.info("Computing metrics for G- vs G0...")
    metrics_g_minus = compute_all_leagues_metrics(df, g_type_col, 'unbalanced_weak')
    
    # Compute metrics for G+ vs G0
    logger.info("Computing metrics for G+ vs G0...")
    metrics_g_plus = compute_all_leagues_metrics(df, g_type_col, 'unbalanced_strong')
    
    # Save metrics to CSV
    save_metrics_to_csv(
        metrics_g_minus,
        output_dir / "league_metrics_gminus_vs_g0.csv",
        "G- vs G0"
    )
    save_metrics_to_csv(
        metrics_g_plus,
        output_dir / "league_metrics_gplus_vs_g0.csv",
        "G+ vs G0"
    )
    
    # Create plots
    logger.info("Creating scatter plots...")
    create_scatter_plot(
        metrics_g_minus,
        "G- vs G0",
        str(output_dir / "occupancy_significance_gminus_vs_g0.png"),
        threshold_label
    )
    create_scatter_plot(
        metrics_g_plus,
        "G+ vs G0",
        str(output_dir / "occupancy_significance_gplus_vs_g0.png"),
        threshold_label
    )
    
    # Print summary
    print_summary_statistics(metrics_g_minus, metrics_g_plus, threshold_label)


def run_single_threshold(
    df: pd.DataFrame,
    mode: str,
    g_threshold: Optional[str],
    p_threshold: Optional[float],
    output_dir: Path
) -> None:
    """Run analysis for a single threshold."""
    if mode == MODE_GTYPE:
        g_type_col = f"G_type_{g_threshold}"
        if g_type_col not in df.columns:
            raise ValueError(f"Column {g_type_col} not found in data")
        
        threshold_label = f"G threshold: {g_threshold}"
        subfolder = f"gtype_{g_threshold}"
        
        logger.info(f"Mode: G-type | Threshold: {g_threshold}")
        run_analysis(df, g_type_col, threshold_label, output_dir / subfolder)
        
    elif mode == MODE_PVALUE:
        if 'P_value' not in df.columns or 'G' not in df.columns:
            raise ValueError("P-value mode requires 'P_value' and 'G' columns")
        
        df_classified = classify_by_pvalue(df, p_threshold)
        threshold_label = f"Spearman p < {p_threshold}"
        subfolder = f"pvalue_{p_threshold}"
        
        logger.info(f"Mode: P-value | Threshold: {p_threshold}")
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_dir / subfolder)


def run_all_thresholds(df: pd.DataFrame, output_dir: Path) -> None:
    """Run analysis for all thresholds defined in parameters.py."""
    logger.info("Running analysis for ALL thresholds from parameters.py...")
    
    # Run all G-type thresholds
    for g_threshold in G_TYPE_THRESHOLDS:
        g_type_col = f"G_type_{g_threshold}"
        if g_type_col not in df.columns:
            logger.warning(f"Skipping {g_type_col} - column not found")
            continue
        
        threshold_label = f"G threshold: {g_threshold}"
        subfolder = f"gtype_{g_threshold}"
        
        logger.info(f"Processing G-type threshold: {g_threshold}")
        run_analysis(df, g_type_col, threshold_label, output_dir / subfolder)
    
    # Run all P-value thresholds
    for p_threshold in P_VALUE_THRESHOLDS:
        df_classified = classify_by_pvalue(df, p_threshold)
        threshold_label = f"Spearman p < {p_threshold}"
        subfolder = f"pvalue_{p_threshold}"
        
        logger.info(f"Processing P-value threshold: {p_threshold}")
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_dir / subfolder)
    
    logger.info(f"Completed all thresholds. Results saved in subfolders under {output_dir}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate occupancy significance scatter plots by league"
    )
    
    # Batch processing flag
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run analysis for ALL thresholds defined in parameters.py"
    )
    
    # Mode selection
    parser.add_argument(
        "--mode",
        type=str,
        default=MODE_GTYPE,
        choices=[MODE_GTYPE, MODE_PVALUE],
        help=f"Classification mode (ignored if --all is used). Default: {MODE_GTYPE}"
    )
    
    # G-type mode arguments
    parser.add_argument(
        "--threshold",
        type=str,
        default=DEFAULT_G_TYPE_THRESHOLD,
        choices=G_TYPE_THRESHOLDS,
        help=f"G_type threshold for gtype mode. Default: {DEFAULT_G_TYPE_THRESHOLD}"
    )
    
    # P-value mode arguments
    parser.add_argument(
        "--p-threshold",
        type=float,
        default=DEFAULT_P_VALUE_THRESHOLD,
        help=f"P-value threshold for pvalue mode. Default: {DEFAULT_P_VALUE_THRESHOLD}"
    )
    
    # Path arguments
    parser.add_argument(
        "--input-balance",
        type=str,
        default=str(DEFAULT_BALANCE_PATH),
        help="Path to strength_schedule_balance.csv"
    )
    parser.add_argument(
        "--input-occupancy",
        type=str,
        default=str(DEFAULT_OCCUPANCY_PATH),
        help="Path to occupancy audit CSV"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Base directory for output files"
    )
    
    args = parser.parse_args()
    
    # Setup paths
    balance_path = Path(args.input_balance)
    occupancy_path = Path(args.input_occupancy)
    output_dir = Path(args.output_dir)
    
    # Load and merge data
    balance_df = load_schedule_balance(balance_path)
    occupancy_df = load_occupancy_data(occupancy_path)
    merged_df = merge_data(balance_df, occupancy_df)
    
    # Execute based on flags
    if args.all:
        run_all_thresholds(merged_df, output_dir)
    else:
        run_single_threshold(
            merged_df, args.mode, args.threshold, args.p_threshold, output_dir
        )
    
    logger.info("Done!")


if __name__ == "__main__":
    main()
