"""
Cliff's Delta Effect Size Plot - Creates box plots showing the distribution
of Cliff's Delta values across all season-league combinations.

This script calculates Cliff's Delta for:
- G- (unbalanced_weak) vs G0 (balanced)
- G+ (unbalanced_strong) vs G0 (balanced)

For each league-season combination, computing how often the test group
(G- or G+) has higher ranks (worse performance) than the control group (G0).

Cliff's Delta Interpretation:
- δ = 1.0: Every test group value is larger than every control group value
- δ = 0.0: Groups overlap completely (no difference)
- δ = -1.0: Every test group value is smaller than every control group value

Since lower ranks are better in football:
- Positive δ: Test group (G-/G+) tends to have worse ranks than control (G0)
- Negative δ: Test group (G-/G+) tends to have better ranks than control (G0)

Usage:
    # Run with default G-type threshold (0.300)
    python -m src.analysis.cliffs_delta_effect_size

    # Run with specific G-type threshold
    python -m src.analysis.cliffs_delta_effect_size --mode gtype --threshold 0.250

    # Run with P-value mode
    python -m src.analysis.cliffs_delta_effect_size --mode pvalue --p-threshold 0.10

    # Run ALL thresholds from parameters.py
    python -m src.analysis.cliffs_delta_effect_size --all
"""

import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Tuple, Optional

from src.analysis.parameters import (
    G_TYPE_THRESHOLDS,
    P_VALUE_THRESHOLDS,
    DEFAULT_G_TYPE_THRESHOLD,
    DEFAULT_P_VALUE_THRESHOLD,
)


# =============================================================================
# Data Loading & Classification
# =============================================================================

def load_data(csv_path: str) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    return pd.read_csv(csv_path)


def classify_by_pvalue(df: pd.DataFrame, p_threshold: float) -> pd.DataFrame:
    """
    Dynamically classify teams based on the Spearman correlation p-value.

    Classification logic:
    - P_value < threshold AND G < 0 → unbalanced_weak
    - P_value < threshold AND G > 0 → unbalanced_strong
    - P_value >= threshold → balanced

    Args:
        df: DataFrame with G and P_value columns
        p_threshold: P-value threshold for significance

    Returns:
        DataFrame with added 'g_type_dynamic' column
    """
    df = df.copy()

    conditions = [
        (df['P_value'] < p_threshold) & (df['G'] < 0),
        (df['P_value'] < p_threshold) & (df['G'] > 0),
        df['P_value'] >= p_threshold,
    ]
    choices = ['unbalanced_weak', 'unbalanced_strong', 'balanced']

    df['g_type_dynamic'] = np.select(conditions, choices, default='balanced')
    return df


# =============================================================================
# Cliff's Delta Calculation
# =============================================================================

def compute_cliffs_delta(group1: np.ndarray, group2: np.ndarray) -> float:
    """
    Compute Cliff's Delta between two groups.

    δ = (#(group1 > group2) - #(group1 < group2)) / (n1 * n2)

    Args:
        group1: Array of values from the test group (G- or G+)
        group2: Array of values from the control group (G0)

    Returns:
        Cliff's Delta value in range [-1, 1]
    """
    n1, n2 = len(group1), len(group2)
    if n1 == 0 or n2 == 0:
        return np.nan

    # Count pairwise comparisons
    greater = 0
    lesser = 0

    for v1 in group1:
        for v2 in group2:
            if v1 > v2:
                greater += 1
            elif v1 < v2:
                lesser += 1

    return (greater - lesser) / (n1 * n2)


def interpret_effect_size(delta: float) -> str:
    """
    Interpret the effect size magnitude using Vargha and Delaney's thresholds.

    |δ| < 0.147: negligible
    |δ| < 0.33: small
    |δ| < 0.474: medium
    |δ| >= 0.474: large
    """
    abs_delta = abs(delta)
    if abs_delta < 0.147:
        return 'negligible'
    elif abs_delta < 0.33:
        return 'small'
    elif abs_delta < 0.474:
        return 'medium'
    else:
        return 'large'


def calculate_season_deltas(
    df: pd.DataFrame,
    g_type_col: str = 'G_type_0.300'
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate Cliff's Delta for each season-league combination.

    Args:
        df: DataFrame with team data
        g_type_col: Column name for G_type classification

    Returns:
        Tuple of (deltas_g_minus_df, deltas_g_plus_df)
    """
    groups = df.groupby(['league_name', 'season_year'])

    deltas_g_minus = []
    deltas_g_plus = []

    for (league, season), group_df in groups:
        g0_positions = group_df[group_df[g_type_col] == 'balanced']['final_position'].values
        g_minus_positions = group_df[group_df[g_type_col] == 'unbalanced_weak']['final_position'].values
        g_plus_positions = group_df[group_df[g_type_col] == 'unbalanced_strong']['final_position'].values

        # Calculate Cliff's Delta for G- vs G0
        if len(g_minus_positions) > 0 and len(g0_positions) > 0:
            delta_g_minus = compute_cliffs_delta(g_minus_positions, g0_positions)
            deltas_g_minus.append({
                'league': league,
                'season': season,
                'delta': delta_g_minus,
                'n_test': len(g_minus_positions),
                'n_control': len(g0_positions),
                'comparison': 'G- vs G0'
            })

        # Calculate Cliff's Delta for G+ vs G0
        if len(g_plus_positions) > 0 and len(g0_positions) > 0:
            delta_g_plus = compute_cliffs_delta(g_plus_positions, g0_positions)
            deltas_g_plus.append({
                'league': league,
                'season': season,
                'delta': delta_g_plus,
                'n_test': len(g_plus_positions),
                'n_control': len(g0_positions),
                'comparison': 'G+ vs G0'
            })

    return pd.DataFrame(deltas_g_minus), pd.DataFrame(deltas_g_plus)


# =============================================================================
# Plotting
# =============================================================================

def create_effect_size_plot(
    deltas_df: pd.DataFrame,
    comparison_label: str,
    output_path: str,
    threshold_label: str
):
    """
    Create a box plot showing the distribution of Cliff's Delta values
    with 3 boxplots: All seasons, Before 2005, After 2005.

    Optimized for inclusion in Springer LNCS single-column LaTeX documents.
    Text blocks are placed in a dedicated right-side panel to avoid
    overlapping data and remain readable at the final printed size.

    Args:
        deltas_df: DataFrame with delta values for each season-league
        comparison_label: Label for the comparison (e.g., "G- vs G0")
        output_path: Path to save the plot
        threshold_label: Label describing the threshold used (e.g., "G: 0.300" or "p<0.10")
    """
    # --- LaTeX-friendly matplotlib settings ---
    plt.rcParams.update({
        'font.family': 'serif',
        'font.serif': ['Computer Modern Roman', 'DejaVu Serif', 'Times New Roman'],
        'font.size': 13,
        'axes.labelsize': 16,
        'axes.titlesize': 16,
        'xtick.labelsize': 13,
        'ytick.labelsize': 13,
        'legend.fontsize': 11,
        'legend.title_fontsize': 12,
        'mathtext.fontset': 'cm',
    })

    sns.set_style("whitegrid")

    fig = plt.figure(figsize=(12, 7))
    ax = fig.add_axes([0.08, 0.12, 0.52, 0.82])

    # Color based on comparison type
    if 'G-' in comparison_label:
        base_color = '#e74c3c'  # Red for G-
        test_label = r'$G^-$ (Easy Start / Unbalanced Weak)'
    else:
        base_color = '#3498db'  # Blue for G+
        test_label = r'$G^+$ (Hard Start / Unbalanced Strong)'

    # Split data by time period
    all_data = deltas_df['delta'].dropna()
    before_2005 = deltas_df[deltas_df['season'] < 2005]['delta'].dropna()
    after_2005 = deltas_df[deltas_df['season'] >= 2005]['delta'].dropna()

    # Define colors with different shades
    colors = [base_color, '#f39c12', '#27ae60']  # Base, Orange, Green

    # Prepare data for boxplot
    data_groups = [all_data, before_2005, after_2005]
    positions = [1, 2, 3]
    labels = ['All Seasons', 'Before 2005', '2005 and After']

    # Create box plots
    box = ax.boxplot(
        [d.values for d in data_groups],
        positions=positions,
        patch_artist=True,
        widths=0.6
    )

    # Style each box plot
    for i, (patch, median) in enumerate(zip(box['boxes'], box['medians'])):
        patch.set_facecolor(colors[i])
        patch.set_alpha(0.6)
        patch.set_edgecolor('black')
        patch.set_linewidth(2.0)
        median.set_color('black')
        median.set_linewidth(2.5)

    # Style whiskers and caps
    for whisker in box['whiskers']:
        whisker.set_linewidth(2.0)
    for cap in box['caps']:
        cap.set_linewidth(2.0)

    # Add jittered points for each group
    for i, (data, pos, color) in enumerate(zip(data_groups, positions, colors)):
        jitter = np.random.normal(pos, 0.08, size=len(data))
        ax.scatter(
            jitter,
            data,
            alpha=0.4,
            color=color,
            s=35,
            edgecolor='white',
            linewidth=0.5,
            zorder=3
        )

    # Add reference lines
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=2, alpha=0.7,
               label=r'No Effect ($\delta$=0)')
    ax.axhline(y=0.33, color='orange', linestyle=':', linewidth=2.0, alpha=0.7,
               label=r'Small Effect Threshold ($\pm$0.33)')
    ax.axhline(y=-0.33, color='orange', linestyle=':', linewidth=2.0, alpha=0.7)
    ax.axhline(y=0.474, color='red', linestyle=':', linewidth=2.0, alpha=0.7,
               label=r'Large Effect Threshold ($\pm$0.474)')
    ax.axhline(y=-0.474, color='red', linestyle=':', linewidth=2.0, alpha=0.7)

    # Customize plot axes
    ax.set_ylabel(r"Cliff's $\delta$", fontsize=16)
    ax.set_ylim(-1.1, 1.1)
    ax.set_xlim(0.3, 3.7)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, fontsize=13)
    # No in-figure title — use \caption{} in LaTeX

    ax.legend(loc='upper right', fontsize=11)

    # --- Right-side text panel ---
    text_ax = fig.add_axes([0.64, 0.12, 0.34, 0.82])
    text_ax.axis('off')

    # Statistics block
    stats_lines = []
    for label_text, data in zip(labels, data_groups):
        n = len(data)
        if n > 0:
            median = data.median()
            n_pos = (data > 0).sum()
            pct_pos = 100 * n_pos / n
            effect = interpret_effect_size(median)
            stats_lines.append(f"{label_text}:")
            stats_lines.append(f"  N={n}, Med $\\delta$={median:.3f} ({effect})")
            stats_lines.append(f"  $\\delta$>0: {n_pos} ({pct_pos:.1f}%)")

    stats_text = "\n".join(stats_lines)
    text_ax.text(
        0.05, 0.98, stats_text,
        transform=text_ax.transAxes,
        fontsize=10,
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
        fontfamily='serif'
    )

    # Interpretation guide
    interpretation = (
        "Interpretation:\n"
        f"$\\delta$ > 0 \u2192 {test_label}\n"
        f"  has worse ranks (higher numbers)\n"
        f"$\\delta$ < 0 \u2192 {test_label}\n"
        f"  has better ranks (lower numbers)"
    )
    text_ax.text(
        0.05, 0.38, interpretation,
        transform=text_ax.transAxes,
        fontsize=10,
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8),
        fontfamily='serif'
    )

    # Save as PNG (300 DPI) and PDF (vector)
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    pdf_path = output_path.replace('.png', '.pdf')
    plt.savefig(pdf_path, bbox_inches='tight', facecolor='white')
    print(f"  Plot saved: {output_path} and {pdf_path}")
    plt.close()


# =============================================================================
# Summary Statistics
# =============================================================================

def generate_summary_csv(
    deltas_g_minus: pd.DataFrame,
    deltas_g_plus: pd.DataFrame,
    output_path: Path,
    threshold_label: str
):
    """
    Generate a CSV summary file with statistics for both comparisons.

    Args:
        deltas_g_minus: DataFrame with G- vs G0 delta values
        deltas_g_plus: DataFrame with G+ vs G0 delta values
        output_path: Path to save the CSV file
        threshold_label: Label describing the threshold used
    """
    summaries = []

    for comparison, df in [("G- vs G0", deltas_g_minus), ("G+ vs G0", deltas_g_plus)]:
        if len(df) == 0:
            continue

        deltas = df['delta'].dropna()
        n_total = len(deltas)

        # Effect size breakdown
        effect_counts = {
            'negligible': sum(deltas.apply(lambda x: interpret_effect_size(x) == 'negligible')),
            'small': sum(deltas.apply(lambda x: interpret_effect_size(x) == 'small')),
            'medium': sum(deltas.apply(lambda x: interpret_effect_size(x) == 'medium')),
            'large': sum(deltas.apply(lambda x: interpret_effect_size(x) == 'large')),
        }

        summary = {
            'comparison': comparison,
            'threshold_label': threshold_label,
            'n_season_league_combinations': n_total,
            'mean_n_test': df['n_test'].mean() if 'n_test' in df.columns else np.nan,
            'mean_n_control': df['n_control'].mean() if 'n_control' in df.columns else np.nan,
            'mean_delta': deltas.mean(),
            'median_delta': deltas.median(),
            'std_delta': deltas.std(),
            'min_delta': deltas.min(),
            'max_delta': deltas.max(),
            'n_positive_delta': (deltas > 0).sum(),
            'pct_positive_delta': 100 * (deltas > 0).sum() / n_total if n_total > 0 else 0,
            'n_negative_delta': (deltas < 0).sum(),
            'pct_negative_delta': 100 * (deltas < 0).sum() / n_total if n_total > 0 else 0,
            'effect_size_interpretation': interpret_effect_size(deltas.median()),
            'n_negligible_effect': effect_counts['negligible'],
            'n_small_effect': effect_counts['small'],
            'n_medium_effect': effect_counts['medium'],
            'n_large_effect': effect_counts['large'],
        }
        summaries.append(summary)

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(output_path, index=False)
    print(f"  Summary CSV saved: {output_path}")


def print_summary_statistics(
    deltas_g_minus: pd.DataFrame,
    deltas_g_plus: pd.DataFrame,
    threshold_label: str
):
    """Print summary statistics for both comparisons."""
    print("\n" + "=" * 70)
    print(f"CLIFF'S DELTA EFFECT SIZE SUMMARY (Classification: {threshold_label})")
    print("=" * 70)

    for label, df in [("G- vs G0", deltas_g_minus), ("G+ vs G0", deltas_g_plus)]:
        if len(df) == 0:
            print(f"\n{label}: No data available")
            continue

        deltas = df['delta']
        print(f"\n{label}:")
        print(f"  Number of season-league combinations: {len(df)}")
        print(f"  Median Cliff's Delta: {deltas.median():.4f}")
        print(f"  Mean Cliff's Delta: {deltas.mean():.4f}")
        print(f"  Std Dev: {deltas.std():.4f}")
        print(f"  Min: {deltas.min():.4f}")
        print(f"  Max: {deltas.max():.4f}")
        print(f"  Effect Size (median): {interpret_effect_size(deltas.median())}")
        print(f"  Seasons with δ > 0: {(deltas > 0).sum()} ({100*(deltas > 0).sum()/len(deltas):.1f}%)")
        print(f"  Seasons with δ < 0: {(deltas < 0).sum()} ({100*(deltas < 0).sum()/len(deltas):.1f}%)")


# =============================================================================
# Analysis Pipeline
# =============================================================================

def run_analysis(
    df: pd.DataFrame,
    g_type_col: str,
    threshold_label: str,
    output_dir: Path
):
    """
    Run the full analysis pipeline for a given classification.

    Args:
        df: DataFrame with team data
        g_type_col: Column name for G_type classification
        threshold_label: Human-readable label for the threshold
        output_dir: Directory to save outputs
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n--- Running analysis for: {threshold_label} ---")
    print(f"  Using classification column: {g_type_col}")

    # Calculate deltas
    deltas_g_minus, deltas_g_plus = calculate_season_deltas(df, g_type_col)

    print(f"  Calculated {len(deltas_g_minus)} deltas for G- vs G0")
    print(f"  Calculated {len(deltas_g_plus)} deltas for G+ vs G0")

    # Create plots
    if len(deltas_g_minus) > 0:
        output_path_g_minus = output_dir / "cliffs_delta_gminus_vs_g0.png"
        create_effect_size_plot(
            deltas_g_minus,
            "G- vs G0",
            str(output_path_g_minus),
            threshold_label
        )

    if len(deltas_g_plus) > 0:
        output_path_g_plus = output_dir / "cliffs_delta_gplus_vs_g0.png"
        create_effect_size_plot(
            deltas_g_plus,
            "G+ vs G0",
            str(output_path_g_plus),
            threshold_label
        )

    # Generate summary CSV
    summary_path = output_dir / "summary_statistics.csv"
    generate_summary_csv(deltas_g_minus, deltas_g_plus, summary_path, threshold_label)

    # Print summary to console
    print_summary_statistics(deltas_g_minus, deltas_g_plus, threshold_label)


def run_all_thresholds(df: pd.DataFrame, output_base_dir: Path):
    """
    Run analysis for all thresholds defined in parameters.py.

    Args:
        df: DataFrame with team data
        output_base_dir: Base directory for output
    """
    print("\n" + "=" * 70)
    print("RUNNING BATCH ANALYSIS FOR ALL THRESHOLDS")
    print("=" * 70)

    # Run G-type thresholds
    for g_threshold in G_TYPE_THRESHOLDS:
        g_type_col = f"G_type_{g_threshold}"
        if g_type_col not in df.columns:
            print(f"\nWarning: Column '{g_type_col}' not found in data, skipping...")
            continue

        subfolder = f"gtype_{g_threshold}"
        threshold_label = f"G: {g_threshold}"
        run_analysis(df, g_type_col, threshold_label, output_base_dir / subfolder)

    # Run P-value thresholds
    for p_threshold in P_VALUE_THRESHOLDS:
        df_classified = classify_by_pvalue(df, p_threshold)
        subfolder = f"pvalue_{p_threshold}"
        threshold_label = f"p<{p_threshold}"
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_base_dir / subfolder)


def run_single_threshold(
    df: pd.DataFrame,
    mode: str,
    g_threshold: str,
    p_threshold: float,
    output_base_dir: Path
):
    """
    Run analysis for a single threshold.

    Args:
        df: DataFrame with team data
        mode: Classification mode ('gtype' or 'pvalue')
        g_threshold: G-type threshold (used if mode='gtype')
        p_threshold: P-value threshold (used if mode='pvalue')
        output_base_dir: Base directory for output
    """
    if mode == 'gtype':
        g_type_col = f"G_type_{g_threshold}"
        if g_type_col not in df.columns:
            print(f"Error: Column '{g_type_col}' not found in data")
            return

        subfolder = f"gtype_{g_threshold}"
        threshold_label = f"G: {g_threshold}"
        run_analysis(df, g_type_col, threshold_label, output_base_dir / subfolder)

    elif mode == 'pvalue':
        df_classified = classify_by_pvalue(df, p_threshold)
        subfolder = f"pvalue_{p_threshold}"
        threshold_label = f"p<{p_threshold}"
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_base_dir / subfolder)


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Calculate and plot Cliff's Delta effect sizes for SoSB analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default G-type threshold (0.300)
  python -m src.analysis.cliffs_delta_effect_size

  # Run with specific G-type threshold
  python -m src.analysis.cliffs_delta_effect_size --mode gtype --threshold 0.250

  # Run with P-value mode
  python -m src.analysis.cliffs_delta_effect_size --mode pvalue --p-threshold 0.10

  # Run ALL thresholds from parameters.py
  python -m src.analysis.cliffs_delta_effect_size --all
        """
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Run analysis for ALL thresholds defined in parameters.py'
    )

    parser.add_argument(
        '--mode',
        type=str,
        default='gtype',
        choices=['gtype', 'pvalue'],
        help='Classification mode: gtype (G coefficient threshold) or pvalue (significance threshold)'
    )

    parser.add_argument(
        '--threshold',
        type=str,
        default=DEFAULT_G_TYPE_THRESHOLD,
        choices=G_TYPE_THRESHOLDS,
        help=f'G_type threshold for gtype mode (default: {DEFAULT_G_TYPE_THRESHOLD})'
    )

    parser.add_argument(
        '--p-threshold',
        type=float,
        default=DEFAULT_P_VALUE_THRESHOLD,
        help=f'P-value threshold for pvalue mode (default: {DEFAULT_P_VALUE_THRESHOLD})'
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
        help='Base output directory (default: auto-detected)'
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
        csv_path = project_root / "data" / "gold" / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"

    if args.output_dir:
        output_base_dir = Path(args.output_dir)
    else:
        output_base_dir = project_root / "data" / "gold" / "analysis" / "cliffs_delta"

    print(f"Loading data from: {csv_path}")

    # Load data
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records")

    # Run analysis
    if args.all:
        run_all_thresholds(df, output_base_dir)
    else:
        run_single_threshold(df, args.mode, args.threshold, args.p_threshold, output_base_dir)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
