"""
SoSB Density Plot - Creates KDE density plots of normalized rankings
<<<<<<< HEAD
grouped by G_type (balanced, unbalanced_weak, unbalanced_strong).

This script visualizes how teams with different schedule balance types
are distributed across final league positions.

Normalized Rank:
- 0 = Champion (best position)
- 1 = Last Place (worst position)

Usage:
    # Run with default G-type threshold (0.300)
    python -m src.analysis.sosb_density_plot

    # Run with specific G-type threshold
    python -m src.analysis.sosb_density_plot --mode gtype --threshold 0.250

    # Run with P-value mode
    python -m src.analysis.sosb_density_plot --mode pvalue --p-threshold 0.10

    # Run ALL thresholds from parameters.py
    python -m src.analysis.sosb_density_plot --all
=======
grouped by G_type (balanced, unbalanced_weak, unbalanced_strong)
for each threshold level (0.200, 0.250, 0.300, 0.350)
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
"""

import argparse
import ast
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional

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
<<<<<<< HEAD
    df = df.copy()

    conditions = [
        (df['P_value'] < p_threshold) & (df['G'] < 0),
        (df['P_value'] < p_threshold) & (df['G'] > 0),
        df['P_value'] >= p_threshold,
    ]
    choices = ['unbalanced_weak', 'unbalanced_strong', 'balanced']

    df['g_type_dynamic'] = np.select(conditions, choices, default='balanced')
    return df


def compute_normalized_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the normalized rank for each team.

    NormalizedRank = (Rank - 1) / (Teams - 1)
    where 0 = Champion and 1 = Last Place

    The number of teams is derived from len(R_array) + 1
    (R_array contains opponents, so +1 for the team itself)
    """
    df = df.copy()
    df['num_teams'] = df['R_array'].apply(lambda x: len(ast.literal_eval(x)) + 1)
    df['normalized_rank'] = (df['final_position'] - 1) / (df['num_teams'] - 1)
    return df


# =============================================================================
# Plotting
# =============================================================================

def create_density_plot(
    df: pd.DataFrame,
    g_type_col: str,
    threshold_label: str,
    output_path: str
):
    """
    Create a KDE density plot for a single threshold.

    Args:
        df: DataFrame with normalized_rank and g_type column
        g_type_col: Column name for G_type classification
        threshold_label: Human-readable label for the threshold
        output_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.set_style("whitegrid")

    # Map the G_type values to display labels
    g_type_mapping = {
        'balanced': 'G0 (Balanced)',
        'unbalanced_weak': 'G- (Unbalanced Weak)',
        'unbalanced_strong': 'G+ (Unbalanced Strong)'
    }

    # Define colors for each group
    colors = {
        'balanced': '#2ecc71',           # Green
        'unbalanced_weak': '#e74c3c',    # Red
        'unbalanced_strong': '#3498db'   # Blue
    }

    # Create KDE plot for each group
    for g_type, label in g_type_mapping.items():
        subset = df[df[g_type_col] == g_type]['normalized_rank']
        if len(subset) > 0:
            sns.kdeplot(
                data=subset,
                label=f"{label} (n={len(subset)})",
                color=colors[g_type],
                linewidth=2.5,
                fill=True,
                alpha=0.3,
                ax=ax
            )

    # Customize plot
    ax.set_xlabel('Normalized Rank (0 = Champion, 1 = Last Place)', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title(f'Distribution of Final Positions by Schedule Balance Type\n(Classification: {threshold_label})',
                 fontsize=14, fontweight='bold')
    ax.legend(title='Schedule Balance Type', fontsize=10, title_fontsize=11)
    ax.set_xlim(0, 1)
    ax.axvline(x=0.5, color='gray', linestyle='--', alpha=0.5, label='Midpoint')

    # Add statistics text box
    stats_lines = []
    for g_type, label in g_type_mapping.items():
        subset = df[df[g_type_col] == g_type]['normalized_rank']
        if len(subset) > 0:
            stats_lines.append(f"{g_type_mapping[g_type]}:")
            stats_lines.append(f"  Mean: {subset.mean():.3f}, Median: {subset.median():.3f}")

    stats_text = "\n".join(stats_lines)
    ax.text(
        0.02, 0.98, stats_text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
        fontfamily='monospace'
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"  Plot saved: {output_path}")
    plt.close()


def create_multi_density_plot(
    df: pd.DataFrame,
    g_type_columns: list,
    threshold_labels: list,
    output_path: str
):
    """
    Create a 2x2 grid of KDE density plots for comparison.

    Args:
        df: DataFrame with normalized_rank
        g_type_columns: List of G_type column names
        threshold_labels: List of human-readable threshold labels
        output_path: Path to save the plot
=======
    df['num_teams'] = df['R_array'].apply(lambda x: len(ast.literal_eval(x)) + 1)
    df['normalized_rank'] = (df['final_position'] - 1) / (df['num_teams'] - 1)
    return df


def create_multi_density_plot(df: pd.DataFrame, output_path: str = None):
    """
    Create a 2x2 grid of KDE density plots, one for each G_type threshold.
    Each plot shows three curves: G0 (balanced), G- (unbalanced_weak), G+ (unbalanced_strong)
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
    """
    # G_type columns to plot
    g_type_columns = ['G_type_0.250', 'G_type_0.300', 'G_type_0.350', 'G_type_0.400']
    threshold_labels = ['α = 0.250', 'α = 0.300', 'α = 0.350', 'α = 0.400']
    
    # Map the G_type values to display labels
    g_type_mapping = {
        'balanced': 'G0 (Balanced)',
        'unbalanced_weak': 'G- (Unbalanced Weak)',
        'unbalanced_strong': 'G+ (Unbalanced Strong)'
    }
<<<<<<< HEAD

=======
    
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
    # Define colors for each group
    colors = {
        'balanced': '#2ecc71',           # Green
        'unbalanced_weak': '#e74c3c',    # Red
        'unbalanced_strong': '#3498db'   # Blue
    }
<<<<<<< HEAD

    # Set up the figure with 2x2 subplots
    n_plots = len(g_type_columns)
    n_cols = 2
    n_rows = (n_plots + 1) // 2
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(14, 6 * n_rows))
    axes = axes.flatten() if n_plots > 1 else [axes]
    sns.set_style("whitegrid")

    for idx, (g_type_col, threshold_label) in enumerate(zip(g_type_columns, threshold_labels)):
        ax = axes[idx]

=======
    
    # Set up the figure with 2x2 subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    axes = axes.flatten()
    sns.set_style("whitegrid")
    
    for idx, (g_type_col, threshold_label) in enumerate(zip(g_type_columns, threshold_labels)):
        ax = axes[idx]
        
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
        # Create KDE plot for each group
        for g_type, label in g_type_mapping.items():
            subset = df[df[g_type_col] == g_type]['normalized_rank']
            if len(subset) > 0:
                sns.kdeplot(
                    data=subset,
                    label=f"{label} (n={len(subset)})",
                    color=colors[g_type],
                    linewidth=2.5,
                    fill=True,
                    alpha=0.3,
                    ax=ax
                )
<<<<<<< HEAD

=======
        
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
        # Customize each subplot
        ax.set_xlabel('Normalized Rank (0 = Champion, 1 = Last Place)', fontsize=10)
        ax.set_ylabel('Density', fontsize=10)
        ax.set_title(f'SoSB Distribution - {threshold_label}', fontsize=12, fontweight='bold')
        ax.legend(title='Schedule Balance Type', fontsize=8, title_fontsize=9)
        ax.set_xlim(0, 1)
        ax.axvline(x=0.5, color='gray', linestyle='--', alpha=0.5)
<<<<<<< HEAD

    # Hide extra subplots if odd number
    for idx in range(n_plots, len(axes)):
        axes[idx].set_visible(False)

    # Overall title
    fig.suptitle('Distribution of Final Positions by Schedule Balance Type\nAcross Different Classification Thresholds',
                 fontsize=14, fontweight='bold', y=1.02)

=======
    
    # Overall title
    fig.suptitle('Distribution of Final Positions by Schedule Balance Type\nAcross Different Significance Thresholds', 
                 fontsize=14, fontweight='bold', y=1.02)
    
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"  Multi-panel plot saved: {output_path}")
    plt.close()


<<<<<<< HEAD
# =============================================================================
# Summary Statistics
# =============================================================================

def generate_summary_csv(
    df: pd.DataFrame,
    g_type_col: str,
    output_path: Path,
    threshold_label: str
):
    """
    Generate a CSV summary file with statistics for each group.

    Args:
        df: DataFrame with normalized_rank and g_type column
        g_type_col: Column name for G_type classification
        output_path: Path to save the CSV file
        threshold_label: Label describing the threshold used
    """
    summaries = []

    for g_type in ['balanced', 'unbalanced_weak', 'unbalanced_strong']:
        subset = df[df[g_type_col] == g_type]
        ranks = subset['normalized_rank']

        if len(ranks) == 0:
            continue

        summary = {
            'g_type': g_type,
            'g_type_display': {
                'balanced': 'G0 (Balanced)',
                'unbalanced_weak': 'G- (Unbalanced Weak)',
                'unbalanced_strong': 'G+ (Unbalanced Strong)'
            }[g_type],
            'threshold_label': threshold_label,
            'n_teams': len(ranks),
            'mean_normalized_rank': ranks.mean(),
            'median_normalized_rank': ranks.median(),
            'std_normalized_rank': ranks.std(),
            'min_normalized_rank': ranks.min(),
            'max_normalized_rank': ranks.max(),
            'n_top_half': (ranks < 0.5).sum(),
            'pct_top_half': 100 * (ranks < 0.5).sum() / len(ranks),
            'n_bottom_half': (ranks >= 0.5).sum(),
            'pct_bottom_half': 100 * (ranks >= 0.5).sum() / len(ranks),
        }
        summaries.append(summary)

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(output_path, index=False)
    print(f"  Summary CSV saved: {output_path}")


def print_statistics(df: pd.DataFrame, g_type_col: str, threshold_label: str):
    """Print summary statistics for each G_type group."""
    print("\n" + "=" * 60)
    print(f"Statistics for {threshold_label}")
    print("=" * 60)

    for g_type in ['balanced', 'unbalanced_weak', 'unbalanced_strong']:
        subset = df[df[g_type_col] == g_type]['normalized_rank']
        print(f"\n{g_type.upper()}:")
        print(f"  Count: {len(subset)}")
        if len(subset) > 0:
            print(f"  Mean Normalized Rank: {subset.mean():.4f}")
            print(f"  Std Dev: {subset.std():.4f}")
            print(f"  Median: {subset.median():.4f}")
            print(f"  Top Half (rank < 0.5): {(subset < 0.5).sum()} ({100*(subset < 0.5).sum()/len(subset):.1f}%)")


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
        df: DataFrame with team data (must have normalized_rank computed)
        g_type_col: Column name for G_type classification
        threshold_label: Human-readable label for the threshold
        output_dir: Directory to save outputs
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n--- Running analysis for: {threshold_label} ---")
    print(f"  Using classification column: {g_type_col}")

    # Create single density plot
    output_path = output_dir / "sosb_density_plot.png"
    create_density_plot(df, g_type_col, threshold_label, str(output_path))

    # Generate summary CSV
    summary_path = output_dir / "summary_statistics.csv"
    generate_summary_csv(df, g_type_col, summary_path, threshold_label)

    # Print statistics
    print_statistics(df, g_type_col, threshold_label)


def run_all_thresholds(df: pd.DataFrame, output_base_dir: Path):
    """
    Run analysis for all thresholds defined in parameters.py.

    Also generates a comparison multi-panel plot.

    Args:
        df: DataFrame with team data (must have normalized_rank computed)
        output_base_dir: Base directory for output
    """
    print("\n" + "=" * 70)
    print("RUNNING BATCH ANALYSIS FOR ALL THRESHOLDS")
    print("=" * 70)

    g_type_columns = []
    threshold_labels = []

    # Run G-type thresholds
    for g_threshold in G_TYPE_THRESHOLDS:
        g_type_col = f"G_type_{g_threshold}"
        if g_type_col not in df.columns:
            print(f"\nWarning: Column '{g_type_col}' not found in data, skipping...")
            continue

        subfolder = f"gtype_{g_threshold}"
        threshold_label = f"G: {g_threshold}"
        run_analysis(df, g_type_col, threshold_label, output_base_dir / subfolder)

        g_type_columns.append(g_type_col)
        threshold_labels.append(threshold_label)

    # Run P-value thresholds
    for p_threshold in P_VALUE_THRESHOLDS:
        df_classified = classify_by_pvalue(df, p_threshold)
        subfolder = f"pvalue_{p_threshold}"
        threshold_label = f"p<{p_threshold}"
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_base_dir / subfolder)

    # Create comparison multi-panel plot for G-type thresholds
    if len(g_type_columns) > 0:
        output_base_dir.mkdir(parents=True, exist_ok=True)
        multi_plot_path = output_base_dir / "sosb_density_comparison.png"
        create_multi_density_plot(df, g_type_columns, threshold_labels, str(multi_plot_path))


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
        df: DataFrame with team data (must have normalized_rank computed)
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
        description="Create SoSB density plots showing distribution of final positions by schedule balance type",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default G-type threshold (0.300)
  python -m src.analysis.sosb_density_plot

  # Run with specific G-type threshold
  python -m src.analysis.sosb_density_plot --mode gtype --threshold 0.250

  # Run with P-value mode
  python -m src.analysis.sosb_density_plot --mode pvalue --p-threshold 0.10

  # Run ALL thresholds from parameters.py
  python -m src.analysis.sosb_density_plot --all
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
=======
def print_statistics(df: pd.DataFrame):
    """Print summary statistics for each G_type column and group."""
    g_type_columns = ['G_type_0.250', 'G_type_0.300', 'G_type_0.350', 'G_type_0.400']
    
    for g_type_col in g_type_columns:
        print("\n" + "="*60)
        print(f"Statistics for {g_type_col}")
        print("="*60)
        
        for g_type in ['balanced', 'unbalanced_weak', 'unbalanced_strong']:
            subset = df[df[g_type_col] == g_type]['normalized_rank']
            print(f"\n{g_type.upper()}:")
            print(f"  Count: {len(subset)}")
            print(f"  Mean Normalized Rank: {subset.mean():.4f}")
            print(f"  Std Dev: {subset.std():.4f}")
            print(f"  Median: {subset.median():.4f}")
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c


def main():
    """Main entry point."""
    args = parse_args()

    # Define paths
    project_root = Path(__file__).parent.parent.parent
<<<<<<< HEAD

    if args.input:
        csv_path = Path(args.input)
    else:
        csv_path = project_root / "data" / "gold" / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"

    if args.output_dir:
        output_base_dir = Path(args.output_dir)
    else:
        output_base_dir = project_root / "data" / "gold" / "analysis" / "sosb_density"

=======
    csv_path = project_root / "data" / "gold" / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"
    output_dir = project_root / "data" / "gold" / "analysis" / "figures"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "sosb_density_plot_multi.png"
    
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c
    print(f"Loading data from: {csv_path}")

    # Load data
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records")

    # Compute normalized ranks
    df = compute_normalized_rank(df)
<<<<<<< HEAD

    # Run analysis
    if args.all:
        run_all_thresholds(df, output_base_dir)
    else:
        run_single_threshold(df, args.mode, args.threshold, args.p_threshold, output_base_dir)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
=======
    
    # Print statistics
    print_statistics(df)
    
    # Create and save the multi-panel density plot
    create_multi_density_plot(df, output_path=str(output_path))
>>>>>>> 238249e8da800de30fa99aa4c9535cd037f80c6c


if __name__ == "__main__":
    main()
