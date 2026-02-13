"""
League Effect Size Significance Plot - Creates scatter plots showing the relationship
between Cliff's Delta (effect size) and -log10(p-value) (significance) for each league.

This script computes per-league metrics using within-season comparisons:
- Y-axis: Cliff's Delta comparing unbalanced vs balanced groups
- X-axis: -log10(p-value) from Mann-Whitney U test

Two classification modes are available:
1. G-type mode (default): Uses pre-computed G_type columns with fixed thresholds
2. P-value mode: Uses Spearman p-value to dynamically classify schedules

Usage:
    # Single threshold (default G-type 0.300)
    python -m src.analysis.league_effect_significance_plot
    
    # All thresholds from parameters.py
    python -m src.analysis.league_effect_significance_plot --all
    
    # Custom single threshold
    python -m src.analysis.league_effect_significance_plot --mode gtype --threshold 0.250
    python -m src.analysis.league_effect_significance_plot --mode pvalue --p-threshold 0.10
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

from src.utils.paths import GOLD_DATA_DIR, SPEARMAN_BALANCE_PATH
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
DEFAULT_INPUT_PATH = SPEARMAN_BALANCE_PATH
DEFAULT_OUTPUT_DIR = GOLD_DATA_DIR / "analysis" / "league_significance"

# Classification modes
MODE_GTYPE = "gtype"
MODE_PVALUE = "pvalue"

# Color and marker configurations for leagues
MARKERS = ['o', 's', '^', 'D', 'v', 'p', '*', 'h', 'X', 'P', '<', '>', '8', 'd', 'H']
COLORS = plt.cm.tab20.colors


def load_data(csv_path: str) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    logger.info(f"Loading data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} records from {df['league_name'].nunique()} leagues")
    return df


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


def compute_league_cliffs_delta(
    df: pd.DataFrame,
    league_name: str,
    g_type_col: str,
    unbalanced_type: str
) -> Tuple[float, int, int, int]:
    """Compute Cliff's Delta for a league using within-season comparisons."""
    league_df = df[df['league_name'] == league_name]
    seasons = league_df['season_year'].unique()
    
    total_greater = 0
    total_less = 0
    total_pairs = 0
    n_unbalanced_total = 0
    n_balanced_total = 0
    
    for season in seasons:
        season_df = league_df[league_df['season_year'] == season]
        unbalanced = season_df[season_df[g_type_col] == unbalanced_type]['final_position'].values
        balanced = season_df[season_df[g_type_col] == 'balanced']['final_position'].values
        
        n_unbalanced_total += len(unbalanced)
        n_balanced_total += len(balanced)
        
        if len(unbalanced) == 0 or len(balanced) == 0:
            continue
        
        for u_pos in unbalanced:
            for b_pos in balanced:
                if u_pos < b_pos:
                    total_less += 1   # mudei aqui
                elif u_pos > b_pos:
                    total_greater += 1
                total_pairs += 1
    
    if total_pairs == 0:
        return np.nan, n_unbalanced_total, n_balanced_total, 0
    
    delta = (total_greater - total_less) / total_pairs
    return delta, n_unbalanced_total, n_balanced_total, total_pairs


def compute_league_mann_whitney(
    df: pd.DataFrame,
    league_name: str,
    g_type_col: str,
    unbalanced_type: str
) -> Tuple[float, float]:
    """Compute Mann-Whitney U test for a league (pooled across seasons)."""
    league_df = df[df['league_name'] == league_name]
    
    unbalanced = league_df[league_df[g_type_col] == unbalanced_type]['final_position'].values
    balanced = league_df[league_df[g_type_col] == 'balanced']['final_position'].values
    
    if len(unbalanced) < 2 or len(balanced) < 2:
        return np.nan, np.nan
    
    try:
        statistic, p_value = stats.mannwhitneyu(
            unbalanced, balanced, alternative='two-sided'
        )
        return statistic, p_value
    except Exception as e:
        logger.warning(f"Mann-Whitney test failed for {league_name}: {e}")
        return np.nan, np.nan


def compute_all_leagues_metrics(
    df: pd.DataFrame,
    g_type_col: str,
    unbalanced_type: str
) -> pd.DataFrame:
    """Compute Cliff's Delta and Mann-Whitney p-value for all leagues."""
    leagues = df['league_name'].unique()
    results = []
    
    for league in leagues:
        delta, n_unbal, n_bal, n_pairs = compute_league_cliffs_delta(
            df, league, g_type_col, unbalanced_type
        )
        stat, p_val = compute_league_mann_whitney(
            df, league, g_type_col, unbalanced_type
        )
        
        neg_log_p = np.nan if (pd.isna(p_val) or p_val == 0) else -np.log10(p_val)
        
        results.append({
            'league_name': league,
            'cliffs_delta': delta,
            'mann_whitney_stat': stat,
            'p_value': p_val,
            'neg_log_p': neg_log_p,
            'n_unbalanced': n_unbal,
            'n_balanced': n_bal,
            'n_pairs': n_pairs
        })
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('cliffs_delta', key=abs, ascending=False)
    return results_df


def create_scatter_plot(
    metrics_df: pd.DataFrame,
    comparison_label: str,
    output_path: str,
    threshold_label: str
) -> None:
    """Create scatter plot with unique marker/color per league.
    
    Optimized for inclusion in Springer LNCS single-column LaTeX documents.
    Text width ~12.2 cm means the figure is scaled to ~50%, so all font sizes
    are set large enough to remain readable at the final printed size.
    """
    plot_df = metrics_df.dropna(subset=['cliffs_delta', 'neg_log_p']).copy()
    
    if len(plot_df) == 0:
        logger.warning(f"No valid data for {comparison_label}")
        return
    
    # --- LaTeX-friendly matplotlib settings ---
    plt.rcParams.update({
        'font.family': 'serif',
        'font.serif': ['Computer Modern Roman', 'DejaVu Serif', 'Times New Roman'],
        'font.size': 13,
        'axes.labelsize': 16,
        'axes.titlesize': 16,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
        'legend.fontsize': 10,
        'legend.title_fontsize': 12,
        'mathtext.fontset': 'cm',
    })
    
    fig = plt.figure(figsize=(10, 7))
    ax = fig.add_axes([0.10, 0.12, 0.52, 0.82])
    
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
            row['neg_log_p'], row['cliffs_delta'],
            c=[style['color']], marker=style['marker'],
            s=160, edgecolors='black', linewidths=0.6, alpha=0.85
        )
    
    # Reference lines
    p_05_line = -np.log10(0.05)
    ax.axvline(x=p_05_line, color='red', linestyle='--', alpha=0.5, linewidth=2.0)
    ax.text(p_05_line + 0.05, ax.get_ylim()[1] * 0.95, '$p = 0.05$', 
            color='red', fontsize=12, alpha=0.7)
    ax.axhline(y=0, color='gray', linestyle='-', alpha=0.3, linewidth=1)
    
    # Negligible effect threshold lines (Cliff's Delta)
    negligible_threshold = 0.147
    ax.axhline(y=negligible_threshold, color='blue', linestyle='--', alpha=0.5, linewidth=2.0)
    ax.axhline(y=-negligible_threshold, color='blue', linestyle='--', alpha=0.5, linewidth=2.0)
    xlim = ax.get_xlim()
    ax.text(xlim[1] * 0.02, negligible_threshold + 0.04, r'$|\delta| < 0.147$: negligible effect', 
            color='blue', fontsize=11, alpha=0.7, va='bottom')
    
    ax.set_xlabel(r'$-\log_{10}(p\text{-value})$', fontsize=16)
    ax.set_ylabel(r"Cliff's $\delta$", fontsize=16)
    # No in-figure title — use \caption{} in LaTeX
    ax.set_ylim(-1.1, 1.1)
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    
    # Quadrant labels
    xlim = ax.get_xlim()
    text_x = xlim[1] * 0.85
    ax.text(text_x, 0.8, 'Worse ranking\n(significant)', fontsize=11, 
            ha='center', alpha=0.5, style='italic')
    ax.text(text_x, -0.8, 'Better ranking\n(significant)', fontsize=11, 
            ha='center', alpha=0.5, style='italic')
    
    # Legend
    legend_ax = fig.add_axes([0.66, 0.12, 0.32, 0.82])
    legend_ax.axis('off')
    
    legend_elements = []
    legend_labels = []
    for league in plot_df['league_name'].values:
        style = league_styles[league]
        element = Line2D(
            [0], [0], marker=style['marker'], color='w',
            markerfacecolor=style['color'], markeredgecolor='black',
            markeredgewidth=0.6, markersize=11, linestyle='None'
        )
        legend_elements.append(element)
        row = plot_df[plot_df['league_name'] == league].iloc[0]
        legend_labels.append(f"{league} ($\\delta$={row['cliffs_delta']:.2f}, p={row['p_value']:.3f})")
    
    legend_ax.legend(
        legend_elements, legend_labels, loc='upper left',
        fontsize=10, frameon=True, fancybox=True, shadow=False,
        title=r'League ($\delta$, $p$-value)', title_fontsize=12
    )
    
    # Save as PNG (300 DPI) and PDF (vector)
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    pdf_path = output_path.replace('.png', '.pdf')
    plt.savefig(pdf_path, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"Saved plot to {output_path} and {pdf_path}")


def print_summary_statistics(
    metrics_g_minus: pd.DataFrame,
    metrics_g_plus: pd.DataFrame,
    threshold_label: str
) -> None:
    """Print summary statistics for both comparisons."""
    print("\n" + "=" * 70)
    print(f"LEAGUE EFFECT SIZE SUMMARY ({threshold_label})")
    print("=" * 70)
    
    for label, df in [("G- vs G0", metrics_g_minus), ("G+ vs G0", metrics_g_plus)]:
        valid_df = df.dropna(subset=['cliffs_delta', 'p_value'])
        significant = valid_df[valid_df['p_value'] < 0.05]
        
        print(f"\n{label}:")
        print(f"  Leagues analyzed: {len(valid_df)}")
        print(f"  Significant (p < 0.05): {len(significant)}")
        
        if len(valid_df) > 0:
            print(f"  Mean Cliff's Delta: {valid_df['cliffs_delta'].mean():.3f}")
            print(f"  Median Cliff's Delta: {valid_df['cliffs_delta'].median():.3f}")
            print(f"  Range: [{valid_df['cliffs_delta'].min():.3f}, {valid_df['cliffs_delta'].max():.3f}]")
        
        if len(significant) > 0:
            print(f"\n  Significant leagues:")
            for _, row in significant.iterrows():
                effect_dir = "worse" if row['cliffs_delta'] > 0 else "better"
                print(f"    - {row['league_name']}: δ={row['cliffs_delta']:.3f} ({effect_dir}), p={row['p_value']:.4f}")


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
        df: DataFrame with classification column
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
        str(output_dir / "league_significance_gminus_vs_g0.png"),
        threshold_label
    )
    create_scatter_plot(
        metrics_g_plus,
        "G+ vs G0",
        str(output_dir / "league_significance_gplus_vs_g0.png"),
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
            raise ValueError(f"Column {g_type_col} not found")
        
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
        description="Generate league effect size vs significance scatter plots"
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
    
    # Common arguments
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_INPUT_PATH),
        help="Path to strength_schedule_balance.csv"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Base directory for output files"
    )
    
    args = parser.parse_args()
    
    # Setup paths
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    
    # Load data once
    df = load_data(str(input_path))
    
    # Execute based on flags
    if args.all:
        run_all_thresholds(df, output_dir)
    else:
        run_single_threshold(
            df, args.mode, args.threshold, args.p_threshold, output_dir
        )
    
    logger.info("Done!")


if __name__ == "__main__":
    main()
