"""
League Effect Size Significance Plot - Creates scatter plots showing the relationship
between Cliff's Delta (effect size) and -log10(p-value) (significance) for each league.

This script computes per-league metrics using within-season comparisons:
- Y-axis: Cliff's Delta comparing unbalanced vs balanced groups
- X-axis: -log10(p-value) from Mann-Whitney U test

Two plots are generated:
1. G- (unbalanced_weak) vs G0 (balanced)
2. G+ (unbalanced_strong) vs G0 (balanced)

Usage:
    uv run python -m src.analysis.league_effect_significance_plot
    uv run python -m src.analysis.league_effect_significance_plot --threshold 0.250
"""

import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from pathlib import Path
from typing import Tuple, Dict, List
from scipy import stats

from src.utils.paths import GOLD_DIR
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default paths
DEFAULT_INPUT_PATH = GOLD_DIR / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"
DEFAULT_OUTPUT_DIR = GOLD_DIR / "analysis" / "league_significance"

# Available G_type thresholds
AVAILABLE_THRESHOLDS = ["0.200", "0.250", "0.300", "0.350", "0.400"]

# Group mapping for display
GROUP_MAP = {
    "unbalanced_weak": "G-",
    "balanced": "G0", 
    "unbalanced_strong": "G+"
}

# Color and marker configurations for leagues
MARKERS = ['o', 's', '^', 'D', 'v', 'p', '*', 'h', 'X', 'P', '<', '>', '8', 'd', 'H']
COLORS = plt.cm.tab20.colors


def load_data(csv_path: str) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    logger.info(f"Loading data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} records from {df['league_name'].nunique()} leagues")
    return df


def compute_league_cliffs_delta(
    df: pd.DataFrame,
    league_name: str,
    g_type_col: str,
    unbalanced_type: str
) -> Tuple[float, int, int, int]:
    """
    Compute Cliff's Delta for a league using within-season comparisons.
    
    For each season, compares final_position of unbalanced teams to balanced teams.
    A positive delta means unbalanced teams tend to have BETTER (lower) positions.
    A negative delta means unbalanced teams tend to have WORSE (higher) positions.
    
    Args:
        df: Full DataFrame
        league_name: Name of the league to analyze
        g_type_col: Column name for G_type classification
        unbalanced_type: 'unbalanced_weak' or 'unbalanced_strong'
    
    Returns:
        Tuple of (delta, n_unbalanced, n_balanced, n_pairs)
    """
    league_df = df[df['league_name'] == league_name]
    seasons = league_df['season_year'].unique()
    
    total_greater = 0  # unbalanced has better (lower) position
    total_less = 0     # unbalanced has worse (higher) position
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
        
        # Count pairwise comparisons
        for u_pos in unbalanced:
            for b_pos in balanced:
                if u_pos < b_pos:  # unbalanced has better position (lower is better)
                    total_greater += 1
                elif u_pos > b_pos:  # unbalanced has worse position
                    total_less += 1
                # if equal, no contribution
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
    """
    Compute Mann-Whitney U test for a league (pooled across seasons).
    
    Note: We pool all seasons because Mann-Whitney doesn't naturally handle
    within-group structure. The null hypothesis is that the distributions are equal.
    
    Args:
        df: Full DataFrame
        league_name: Name of the league
        g_type_col: Column name for G_type classification
        unbalanced_type: 'unbalanced_weak' or 'unbalanced_strong'
    
    Returns:
        Tuple of (statistic, p_value)
    """
    league_df = df[df['league_name'] == league_name]
    
    unbalanced = league_df[league_df[g_type_col] == unbalanced_type]['final_position'].values
    balanced = league_df[league_df[g_type_col] == 'balanced']['final_position'].values
    
    if len(unbalanced) < 2 or len(balanced) < 2:
        return np.nan, np.nan
    
    try:
        statistic, p_value = stats.mannwhitneyu(
            unbalanced, 
            balanced, 
            alternative='two-sided'
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
    """
    Compute Cliff's Delta and Mann-Whitney p-value for all leagues.
    
    Returns:
        DataFrame with columns: league_name, cliffs_delta, p_value, neg_log_p,
                               n_unbalanced, n_balanced, n_pairs
    """
    leagues = df['league_name'].unique()
    results = []
    
    for league in leagues:
        delta, n_unbal, n_bal, n_pairs = compute_league_cliffs_delta(
            df, league, g_type_col, unbalanced_type
        )
        stat, p_val = compute_league_mann_whitney(
            df, league, g_type_col, unbalanced_type
        )
        
        # Compute -log10(p_value), handling edge cases
        if pd.isna(p_val) or p_val == 0:
            neg_log_p = np.nan
        else:
            neg_log_p = -np.log10(p_val)
        
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
    
    # Sort by absolute effect size
    results_df = results_df.sort_values('cliffs_delta', key=abs, ascending=False)
    
    return results_df


def create_scatter_plot(
    metrics_df: pd.DataFrame,
    comparison_label: str,
    output_path: str,
    g_type_threshold: str
) -> None:
    """
    Create scatter plot with unique marker/color per league.
    
    Args:
        metrics_df: DataFrame with league metrics
        comparison_label: "G- vs G0" or "G+ vs G0"
        output_path: Path to save the plot
        g_type_threshold: The threshold used (e.g., "0.300")
    """
    # Filter out leagues with missing data
    plot_df = metrics_df.dropna(subset=['cliffs_delta', 'neg_log_p']).copy()
    
    if len(plot_df) == 0:
        logger.warning(f"No valid data for {comparison_label}")
        return
    
    # Create figure with two subplots: main plot and legend
    fig = plt.figure(figsize=(14, 8))
    
    # Main scatter plot (left side)
    ax = fig.add_axes([0.08, 0.12, 0.55, 0.78])
    
    # Assign unique marker/color to each league
    league_styles = {}
    for i, league in enumerate(plot_df['league_name'].values):
        color = COLORS[i % len(COLORS)]
        marker = MARKERS[i % len(MARKERS)]
        league_styles[league] = {'color': color, 'marker': marker}
    
    # Plot each league
    for _, row in plot_df.iterrows():
        league = row['league_name']
        style = league_styles[league]
        ax.scatter(
            row['neg_log_p'],
            row['cliffs_delta'],
            c=[style['color']],
            marker=style['marker'],
            s=120,
            edgecolors='black',
            linewidths=0.5,
            alpha=0.85
        )
    
    # Add reference lines
    # Vertical line at p = 0.05 (-log10(0.05) ≈ 1.301)
    p_05_line = -np.log10(0.05)
    ax.axvline(x=p_05_line, color='red', linestyle='--', alpha=0.5, linewidth=1.5)
    ax.text(p_05_line + 0.05, ax.get_ylim()[1] * 0.95, 'p = 0.05', 
            color='red', fontsize=9, alpha=0.7)
    
    # Horizontal line at delta = 0
    ax.axhline(y=0, color='gray', linestyle='-', alpha=0.3, linewidth=1)
    
    # Labels and title
    ax.set_xlabel('-log₁₀(p-value)', fontsize=12)
    ax.set_ylabel("Cliff's Delta", fontsize=12)
    ax.set_title(
        f"League Effect Size vs Significance\n({comparison_label} | Threshold: {g_type_threshold})",
        fontsize=14,
        fontweight='bold'
    )
    
    # Set y-axis limits
    ax.set_ylim(-1.1, 1.1)
    
    # Add grid
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    
    # Add quadrant labels
    xlim = ax.get_xlim()
    text_x = xlim[1] * 0.85
    ax.text(text_x, 0.8, 'Better ranking\n(significant)', fontsize=8, 
            ha='center', alpha=0.5, style='italic')
    ax.text(text_x, -0.8, 'Worse ranking\n(significant)', fontsize=8, 
            ha='center', alpha=0.5, style='italic')
    
    # Create legend table on the right side
    legend_ax = fig.add_axes([0.68, 0.12, 0.30, 0.78])
    legend_ax.axis('off')
    
    # Create legend entries
    legend_elements = []
    legend_labels = []
    
    for league in plot_df['league_name'].values:
        style = league_styles[league]
        element = Line2D(
            [0], [0],
            marker=style['marker'],
            color='w',
            markerfacecolor=style['color'],
            markeredgecolor='black',
            markeredgewidth=0.5,
            markersize=10,
            linestyle='None'
        )
        legend_elements.append(element)
        
        # Get metrics for this league
        row = plot_df[plot_df['league_name'] == league].iloc[0]
        label = f"{league} (δ={row['cliffs_delta']:.2f}, p={row['p_value']:.3f})"
        legend_labels.append(label)
    
    legend_ax.legend(
        legend_elements,
        legend_labels,
        loc='upper left',
        fontsize=8,
        frameon=True,
        fancybox=True,
        shadow=False,
        title='League (δ, p-value)',
        title_fontsize=10
    )
    
    # Save figure
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    logger.info(f"Saved plot to {output_path}")


def print_summary_statistics(
    metrics_g_minus: pd.DataFrame,
    metrics_g_plus: pd.DataFrame,
    g_type_threshold: str
) -> None:
    """Print summary statistics for both comparisons."""
    print("\n" + "=" * 70)
    print(f"LEAGUE EFFECT SIZE SUMMARY (Threshold: {g_type_threshold})")
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
                effect_dir = "better" if row['cliffs_delta'] > 0 else "worse"
                print(f"    - {row['league_name']}: δ={row['cliffs_delta']:.3f} ({effect_dir}), p={row['p_value']:.4f}")


def save_metrics_to_csv(
    metrics_df: pd.DataFrame,
    output_path: Path,
    comparison_label: str
) -> None:
    """Save metrics DataFrame to CSV."""
    metrics_df.to_csv(output_path, index=False)
    logger.info(f"Saved {comparison_label} metrics to {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate league effect size vs significance scatter plots"
    )
    parser.add_argument(
        "--threshold",
        type=str,
        default="0.300",
        choices=AVAILABLE_THRESHOLDS,
        help="G_type threshold to use (default: 0.300)"
    )
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
        help="Directory for output files"
    )
    
    args = parser.parse_args()
    
    # Setup paths
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    g_type_col = f"G_type_{args.threshold}"
    
    # Load data
    df = load_data(str(input_path))
    
    # Verify the G_type column exists
    if g_type_col not in df.columns:
        raise ValueError(f"Column {g_type_col} not found. Available: {[c for c in df.columns if 'G_type' in c]}")
    
    logger.info(f"Using G_type column: {g_type_col}")
    
    # Compute metrics for G- vs G0
    logger.info("Computing metrics for G- vs G0...")
    metrics_g_minus = compute_all_leagues_metrics(df, g_type_col, 'unbalanced_weak')
    
    # Compute metrics for G+ vs G0
    logger.info("Computing metrics for G+ vs G0...")
    metrics_g_plus = compute_all_leagues_metrics(df, g_type_col, 'unbalanced_strong')
    
    # Save metrics to CSV
    save_metrics_to_csv(
        metrics_g_minus,
        output_dir / f"league_metrics_gminus_vs_g0_{args.threshold}.csv",
        "G- vs G0"
    )
    save_metrics_to_csv(
        metrics_g_plus,
        output_dir / f"league_metrics_gplus_vs_g0_{args.threshold}.csv",
        "G+ vs G0"
    )
    
    # Create plots
    logger.info("Creating scatter plots...")
    create_scatter_plot(
        metrics_g_minus,
        "G- vs G0",
        str(output_dir / f"league_significance_gminus_vs_g0_{args.threshold}.png"),
        args.threshold
    )
    create_scatter_plot(
        metrics_g_plus,
        "G+ vs G0",
        str(output_dir / f"league_significance_gplus_vs_g0_{args.threshold}.png"),
        args.threshold
    )
    
    # Print summary
    print_summary_statistics(metrics_g_minus, metrics_g_plus, args.threshold)
    
    logger.info("Done!")


if __name__ == "__main__":
    main()
