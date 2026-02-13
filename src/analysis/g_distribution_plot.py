"""
G Distribution Histogram Plot

Creates a histogram with KDE overlay showing the distribution of G values
(Spearman's rank correlation coefficient) across all team-season combinations.

The plot includes:
- Histogram of G values with KDE overlay
- Vertical demarcation lines at G = ±0.2, ±0.3, ±0.4
- Mean G values for significant subsets (p < 0.10 and p < 0.05)

Usage:
    python -m src.analysis.g_distribution_plot
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np
from pathlib import Path


# =============================================================================
# Data Loading
# =============================================================================

def load_data(csv_path: str) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    return pd.read_csv(csv_path)


# =============================================================================
# Statistics
# =============================================================================

def compute_statistics(df: pd.DataFrame) -> dict:
    """
    Compute summary statistics for G values.
    
    Returns dict with stats for:
    - All observations
    - p < 0.10 separated by G direction (negative/positive)
    - p < 0.05 separated by G direction (negative/positive)
    """
    stats = {}
    
    # All observations
    stats['all'] = {
        'n': len(df),
        'mean': df['G'].mean(),
        'std': df['G'].std(),
        'median': df['G'].median(),
    }
    
    # p < 0.10 - separated by G direction
    df_p10_neg = df[(df['P_value'] < 0.10) & (df['G'] < 0)]
    df_p10_pos = df[(df['P_value'] < 0.10) & (df['G'] > 0)]
    stats['p_0.10_neg'] = {
        'n': len(df_p10_neg),
        'mean': df_p10_neg['G'].mean() if len(df_p10_neg) > 0 else np.nan,
    }
    stats['p_0.10_pos'] = {
        'n': len(df_p10_pos),
        'mean': df_p10_pos['G'].mean() if len(df_p10_pos) > 0 else np.nan,
    }
    
    # p < 0.05 - separated by G direction
    df_p05_neg = df[(df['P_value'] < 0.05) & (df['G'] < 0)]
    df_p05_pos = df[(df['P_value'] < 0.05) & (df['G'] > 0)]
    stats['p_0.05_neg'] = {
        'n': len(df_p05_neg),
        'mean': df_p05_neg['G'].mean() if len(df_p05_neg) > 0 else np.nan,
    }
    stats['p_0.05_pos'] = {
        'n': len(df_p05_pos),
        'mean': df_p05_pos['G'].mean() if len(df_p05_pos) > 0 else np.nan,
    }
    
    return stats


def count_by_threshold(df: pd.DataFrame, thresholds: list) -> dict:
    """Count teams in each G zone defined by thresholds."""
    counts = {}
    g_values = df['G']
    
    for t in thresholds:
        counts[t] = {
            'unbalanced_weak': (g_values < -t).sum(),
            'balanced': ((g_values >= -t) & (g_values <= t)).sum(),
            'unbalanced_strong': (g_values > t).sum(),
        }
    
    return counts


def count_by_pvalue(df: pd.DataFrame, p_thresholds: list) -> dict:
    """
    Count teams in each G zone based on p-value significance.
    
    Classification:
    - G < 0 AND p < threshold -> unbalanced_weak
    - G > 0 AND p < threshold -> unbalanced_strong
    - p >= threshold -> balanced
    """
    counts = {}
    
    for p in p_thresholds:
        n_weak = ((df['P_value'] < p) & (df['G'] < 0)).sum()
        n_strong = ((df['P_value'] < p) & (df['G'] > 0)).sum()
        n_balanced = len(df) - n_weak - n_strong
        
        counts[p] = {
            'unbalanced_weak': n_weak,
            'balanced': n_balanced,
            'unbalanced_strong': n_strong,
        }
    
    return counts


# =============================================================================
# Plotting
# =============================================================================

def create_g_distribution_plot(
    df: pd.DataFrame,
    stats: dict,
    output_path: str
):
    """
    Create histogram with KDE overlay showing G distribution.
    
    Args:
        df: DataFrame with G values
        stats: Dictionary with computed statistics
        output_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(14, 8))
    sns.set_style("whitegrid")
    
    # Threshold values for demarcation lines
    thresholds = [0.2, 0.3, 0.4]
    threshold_colors = {
        0.2: '#e74c3c',  # Red
        0.3: '#f39c12',  # Orange
        0.4: '#9b59b6',  # Purple
    }
    
    # Create histogram with KDE
    sns.histplot(
        data=df,
        x='G',
        bins=50,
        kde=True,
        color='#3498db',
        alpha=0.6,
        edgecolor='white',
        linewidth=0.5,
        ax=ax
    )
    
    # Get y-axis max for line heights
    y_max = ax.get_ylim()[1]
    
    # Add vertical demarcation lines at ±thresholds
    for t in thresholds:
        color = threshold_colors[t]
        # Positive threshold
        ax.axvline(
            x=t,
            color=color,
            linestyle='--',
            linewidth=2,
            alpha=0.8,
            label=f'G = ±{t}'
        )
        # Negative threshold
        ax.axvline(
            x=-t,
            color=color,
            linestyle='--',
            linewidth=2,
            alpha=0.8
        )
    
    # Add mean line for all data
    mean_all = stats['all']['mean']
    ax.axvline(
        x=mean_all,
        color='#2c3e50',
        linestyle='-',
        linewidth=2.5,
        label=f"Mean (all): {mean_all:.3f}"
    )
    
    # Add mean lines for significant subsets - separated by G direction
    # p < 0.10
    mean_p10_neg = stats['p_0.10_neg']['mean']
    mean_p10_pos = stats['p_0.10_pos']['mean']
    
    if not np.isnan(mean_p10_neg):
        ax.axvline(
            x=mean_p10_neg,
            color='#27ae60',
            linestyle='-.',
            linewidth=2,
            label=f"Mean G- (p<0.10, n={stats['p_0.10_neg']['n']}): {mean_p10_neg:.3f}"
        )
    
    if not np.isnan(mean_p10_pos):
        ax.axvline(
            x=mean_p10_pos,
            color='#27ae60',
            linestyle='-.',
            linewidth=2,
            label=f"Mean G+ (p<0.10, n={stats['p_0.10_pos']['n']}): {mean_p10_pos:.3f}"
        )
    
    # p < 0.05
    mean_p05_neg = stats['p_0.05_neg']['mean']
    mean_p05_pos = stats['p_0.05_pos']['mean']
    
    if not np.isnan(mean_p05_neg):
        ax.axvline(
            x=mean_p05_neg,
            color='#c0392b',
            linestyle=':',
            linewidth=2.5,
            label=f"Mean G- (p<0.05, n={stats['p_0.05_neg']['n']}): {mean_p05_neg:.3f}"
        )
    
    if not np.isnan(mean_p05_pos):
        ax.axvline(
            x=mean_p05_pos,
            color='#c0392b',
            linestyle=':',
            linewidth=2.5,
            label=f"Mean G+ (p<0.05, n={stats['p_0.05_pos']['n']}): {mean_p05_pos:.3f}"
        )
    
    # Customize plot
    ax.set_xlabel("G (Spearman's Rank Correlation Coefficient)", fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title(
        "Distribution of Schedule Balance (G) Across All Team-Seasons\n"
        f"(n = {stats['all']['n']:,} team-season combinations)",
        fontsize=14,
        fontweight='bold'
    )
    
    # Set x-axis limits symmetrically around 0
    x_limit = max(abs(df['G'].min()), abs(df['G'].max())) * 1.05
    ax.set_xlim(-x_limit, x_limit)
    
    # Add legend
    ax.legend(
        loc='upper left',
        fontsize=9,
        framealpha=0.9
    )
    
    # Add statistics text box
    stats_text = (
        f"Summary Statistics:\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"All (n={stats['all']['n']:,}):\n"
        f"  Mean: {stats['all']['mean']:.4f}\n"
        f"  Std:  {stats['all']['std']:.4f}\n"
        f"\n"
        f"p < 0.10:\n"
        f"  G- (n={stats['p_0.10_neg']['n']:,}): {stats['p_0.10_neg']['mean']:.4f}\n"
        f"  G+ (n={stats['p_0.10_pos']['n']:,}): {stats['p_0.10_pos']['mean']:.4f}\n"
        f"\n"
        f"p < 0.05:\n"
        f"  G- (n={stats['p_0.05_neg']['n']:,}): {stats['p_0.05_neg']['mean']:.4f}\n"
        f"  G+ (n={stats['p_0.05_pos']['n']:,}): {stats['p_0.05_pos']['mean']:.4f}"
    )
    
    ax.text(
        0.98, 0.98, stats_text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='top',
        horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.9),
        fontfamily='monospace'
    )
    
    # Add zone count annotations at top
    counts = count_by_threshold(df, [0.3])  # Use 0.3 as default
    zone_text = (
        f"Classification (|G| > 0.3):\n"
        f"G- (Unbalanced Weak): {counts[0.3]['unbalanced_weak']:,} "
        f"({100*counts[0.3]['unbalanced_weak']/len(df):.1f}%)\n"
        f"G0 (Balanced): {counts[0.3]['balanced']:,} "
        f"({100*counts[0.3]['balanced']/len(df):.1f}%)\n"
        f"G+ (Unbalanced Strong): {counts[0.3]['unbalanced_strong']:,} "
        f"({100*counts[0.3]['unbalanced_strong']/len(df):.1f}%)"
    )
    
    ax.text(
        0.02, 0.98, zone_text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='top',
        horizontalalignment='left',
        bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9),
        fontfamily='monospace'
    )
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved: {output_path}")
    plt.close()


def save_statistics_txt(stats: dict, counts: dict, pvalue_counts: dict, output_path: Path):
    """
    Save summary statistics to a text file.
    
    Args:
        stats: Statistics dictionary from compute_statistics
        counts: G-threshold based counts from count_by_threshold
        pvalue_counts: P-value based counts from count_by_pvalue
        output_path: Path to save the text file
    """
    lines = []
    lines.append("=" * 70)
    lines.append("G DISTRIBUTION STATISTICS")
    lines.append("=" * 70)
    
    lines.append(f"\nAll Observations (n={stats['all']['n']:,}):")
    lines.append(f"  Mean:   {stats['all']['mean']:.4f}")
    lines.append(f"  Std:    {stats['all']['std']:.4f}")
    lines.append(f"  Median: {stats['all']['median']:.4f}")
    
    lines.append(f"\nSignificant at p < 0.10:")
    lines.append(f"  G- (n={stats['p_0.10_neg']['n']:,}): Mean = {stats['p_0.10_neg']['mean']:.4f}")
    lines.append(f"  G+ (n={stats['p_0.10_pos']['n']:,}): Mean = {stats['p_0.10_pos']['mean']:.4f}")
    
    lines.append(f"\nSignificant at p < 0.05:")
    lines.append(f"  G- (n={stats['p_0.05_neg']['n']:,}): Mean = {stats['p_0.05_neg']['mean']:.4f}")
    lines.append(f"  G+ (n={stats['p_0.05_pos']['n']:,}): Mean = {stats['p_0.05_pos']['mean']:.4f}")
    
    # P-value based classification
    lines.append("\n" + "-" * 70)
    lines.append("CLASSIFICATION BY P-VALUE SIGNIFICANCE")
    lines.append("-" * 70)
    
    for p in sorted(pvalue_counts.keys(), reverse=True):
        total = sum(pvalue_counts[p].values())
        lines.append(f"\np < {p}:")
        lines.append(f"  G- (Unbalanced Weak):   {pvalue_counts[p]['unbalanced_weak']:,} "
                     f"({100*pvalue_counts[p]['unbalanced_weak']/total:.1f}%)")
        lines.append(f"  G0 (Balanced):          {pvalue_counts[p]['balanced']:,} "
                     f"({100*pvalue_counts[p]['balanced']/total:.1f}%)")
        lines.append(f"  G+ (Unbalanced Strong): {pvalue_counts[p]['unbalanced_strong']:,} "
                     f"({100*pvalue_counts[p]['unbalanced_strong']/total:.1f}%)")
    
    # G-threshold based classification
    lines.append("\n" + "-" * 70)
    lines.append("CLASSIFICATION BY G THRESHOLD")
    lines.append("-" * 70)
    
    for t in sorted(counts.keys()):
        total = sum(counts[t].values())
        lines.append(f"\n|G| > {t}:")
        lines.append(f"  G- (Unbalanced Weak):   {counts[t]['unbalanced_weak']:,} "
                     f"({100*counts[t]['unbalanced_weak']/total:.1f}%)")
        lines.append(f"  G0 (Balanced):          {counts[t]['balanced']:,} "
                     f"({100*counts[t]['balanced']/total:.1f}%)")
        lines.append(f"  G+ (Unbalanced Strong): {counts[t]['unbalanced_strong']:,} "
                     f"({100*counts[t]['unbalanced_strong']/total:.1f}%)")
    
    lines.append("\n" + "=" * 70)
    
    # Write to file
    with open(output_path, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"Statistics saved: {output_path}")


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Create G distribution histogram with KDE overlay",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default=None,
        help='Path to input CSV file (default: auto-detected)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory (default: data/gold/analysis/g_distribution)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    
    if args.input:
        csv_path = Path(args.input)
    else:
        csv_path = (
            project_root / "data" / "gold" / "analysis" / 
            "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"
        )
    
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = project_root / "data" / "gold" / "analysis" / "g_distribution"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "g_distribution_histogram.png"
    
    print(f"Loading data from: {csv_path}")
    
    # Load data
    df = load_data(csv_path)
    print(f"Loaded {len(df):,} records")
    
    # Compute statistics
    stats = compute_statistics(df)
    counts = count_by_threshold(df, [0.2, 0.3, 0.4])
    pvalue_counts = count_by_pvalue(df, [0.10, 0.05])
    
    # Save statistics to text file
    stats_path = output_dir / "g_distribution_statistics.txt"
    save_statistics_txt(stats, counts, pvalue_counts, stats_path)
    
    # Create plot
    create_g_distribution_plot(df, stats, str(output_path))
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
