"""
G-Type League Summary - Generates a table showing the percentage of G+, G-, 
and G_0 per league, sorted by G_0 percentage in ascending order.

This script creates a CSV summary table that shows the distribution of 
schedule balance types (G+, G-, G_0) across different leagues.

Usage:
    # Run with default G-type threshold (0.300)
    python -m src.analysis.g_type_league_summary

    # Run with specific G-type threshold
    python -m src.analysis.g_type_league_summary --mode gtype --threshold 0.250

    # Run with P-value mode
    python -m src.analysis.g_type_league_summary --mode pvalue --p-threshold 0.10

    # Run ALL thresholds from parameters.py
    python -m src.analysis.g_type_league_summary --all
"""

import argparse
import numpy as np
import pandas as pd
from pathlib import Path

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
# Summary Table Generation
# =============================================================================

def generate_league_summary(
    df: pd.DataFrame,
    g_type_col: str,
    threshold_label: str
) -> pd.DataFrame:
    """
    Generate a summary table with G-type percentages per league.

    Args:
        df: DataFrame with league_name and g_type column
        g_type_col: Column name for G_type classification
        threshold_label: Label describing the threshold used

    Returns:
        DataFrame with G-type distribution per league, sorted by G_0 ascending
    """
    summaries = []

    for league_name in df['league_name'].unique():
        league_df = df[df['league_name'] == league_name]
        n_teams = len(league_df)

        # Count each G-type
        n_g_plus = (league_df[g_type_col] == 'unbalanced_strong').sum()
        n_g_minus = (league_df[g_type_col] == 'unbalanced_weak').sum()
        n_g_0 = (league_df[g_type_col] == 'balanced').sum()

        # Calculate percentages
        pct_g_plus = 100 * n_g_plus / n_teams if n_teams > 0 else 0
        pct_g_minus = 100 * n_g_minus / n_teams if n_teams > 0 else 0
        pct_g_0 = 100 * n_g_0 / n_teams if n_teams > 0 else 0

        summary = {
            'league_name': league_name,
            'n_teams': n_teams,
            'n_G_plus': n_g_plus,
            'n_G_minus': n_g_minus,
            'n_G_0': n_g_0,
            'pct_G_plus': round(pct_g_plus, 2),
            'pct_G_minus': round(pct_g_minus, 2),
            'pct_G_0': round(pct_g_0, 2),
            'threshold': threshold_label,
        }
        summaries.append(summary)

    # Create DataFrame and sort by G_0 percentage ascending
    summary_df = pd.DataFrame(summaries)
    summary_df = summary_df.sort_values('pct_G_0', ascending=True).reset_index(drop=True)

    return summary_df


def save_summary_csv(
    summary_df: pd.DataFrame,
    output_path: Path,
    threshold_label: str
):
    """
    Save the summary table to a CSV file.

    Args:
        summary_df: DataFrame with league summary
        output_path: Path to save the CSV file
        threshold_label: Label describing the threshold used
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(output_path, index=False)
    print(f"  Summary CSV saved: {output_path}")


def print_summary(summary_df: pd.DataFrame, threshold_label: str):
    """Print a formatted summary of the G-type distribution per league."""
    print(f"\n{'=' * 80}")
    print(f"G-Type Distribution by League - {threshold_label}")
    print(f"{'=' * 80}")
    print(f"(Sorted by G_0 percentage in ascending order)")
    print()

    # Print header
    print(f"{'League':<30} {'N':<8} {'G+':<10} {'G-':<10} {'G0':<10}")
    print(f"{'-' * 30} {'-' * 8} {'-' * 10} {'-' * 10} {'-' * 10}")

    for _, row in summary_df.iterrows():
        print(f"{row['league_name']:<30} {row['n_teams']:<8} "
              f"{row['pct_G_plus']:>6.2f}%   {row['pct_G_minus']:>6.2f}%   {row['pct_G_0']:>6.2f}%")

    print()


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

    # Generate summary table
    summary_df = generate_league_summary(df, g_type_col, threshold_label)

    # Save to CSV
    output_path = output_dir / "g_type_league_summary.csv"
    save_summary_csv(summary_df, output_path, threshold_label)

    # Print summary
    print_summary(summary_df, threshold_label)


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
        description="Generate a table showing G+, G-, and G_0 percentages per league",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default G-type threshold (0.300)
  python -m src.analysis.g_type_league_summary

  # Run with specific G-type threshold
  python -m src.analysis.g_type_league_summary --mode gtype --threshold 0.250

  # Run with P-value mode
  python -m src.analysis.g_type_league_summary --mode pvalue --p-threshold 0.10

  # Run ALL thresholds from parameters.py
  python -m src.analysis.g_type_league_summary --all
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
        output_base_dir = project_root / "data" / "gold" / "analysis" / "g_type_distribution"

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
