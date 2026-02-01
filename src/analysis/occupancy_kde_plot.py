"""
Occupancy KDE Plot - Creates KDE density plots of average stadium occupancy
grouped by G_type (balanced, unbalanced_weak, unbalanced_strong).

This script computes the distribution of average stadium occupancy for each
schedule balance group, allowing comparison of whether teams with different
schedule characteristics tend to have different stadium attendances.

Two classification modes are available:
1. G-type mode (default): Uses pre-computed G_type columns with fixed thresholds
2. P-value mode: Uses Spearman p-value to dynamically classify schedules

Usage:
    # Single threshold (default G-type 0.300)
    python -m src.analysis.occupancy_kde_plot
    
    # All thresholds from parameters.py
    python -m src.analysis.occupancy_kde_plot --all
    
    # Custom single threshold
    python -m src.analysis.occupancy_kde_plot --mode gtype --threshold 0.250
    python -m src.analysis.occupancy_kde_plot --mode pvalue --p-threshold 0.10
"""

import argparse
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional, Dict
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
DEFAULT_OUTPUT_DIR = GOLD_DATA_DIR / "analysis" / "occupancy_kde"

# Classification modes
MODE_GTYPE = "gtype"
MODE_PVALUE = "pvalue"

# Group display names and colors (consistent with sosb_density_plot.py)
GROUP_CONFIG = {
    'balanced': {
        'label': 'G0 (Balanced)',
        'color': '#2ecc71',  # Green
    },
    'unbalanced_weak': {
        'label': 'G- (Unbalanced Weak)',
        'color': '#e74c3c',  # Red
    },
    'unbalanced_strong': {
        'label': 'G+ (Unbalanced Strong)',
        'color': '#3498db',  # Blue
    },
}


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


def compute_summary_statistics(
    df: pd.DataFrame,
    g_type_col: str
) -> pd.DataFrame:
    """
    Compute summary statistics for each group.
    
    Returns DataFrame with:
    - group, n_samples, mean_occupancy, median_occupancy, std_occupancy,
    - min_occupancy, max_occupancy, mw_pvalue_vs_balanced
    """
    results = []
    groups = ['balanced', 'unbalanced_weak', 'unbalanced_strong']
    
    balanced_occupancy = df[df[g_type_col] == 'balanced']['average_occupancy'].values
    
    for group in groups:
        subset = df[df[g_type_col] == group]['average_occupancy']
        
        if len(subset) == 0:
            results.append({
                'group': group,
                'n_samples': 0,
                'mean_occupancy': np.nan,
                'median_occupancy': np.nan,
                'std_occupancy': np.nan,
                'min_occupancy': np.nan,
                'max_occupancy': np.nan,
                'mw_pvalue_vs_balanced': np.nan,
            })
            continue
        
        # Compute Mann-Whitney p-value against balanced group
        mw_pvalue = np.nan
        if group != 'balanced' and len(balanced_occupancy) >= 2 and len(subset) >= 2:
            try:
                _, mw_pvalue = stats.mannwhitneyu(
                    subset.values, balanced_occupancy, alternative='two-sided'
                )
            except Exception as e:
                logger.warning(f"Mann-Whitney test failed for {group}: {e}")
        
        results.append({
            'group': group,
            'n_samples': len(subset),
            'mean_occupancy': subset.mean(),
            'median_occupancy': subset.median(),
            'std_occupancy': subset.std(),
            'min_occupancy': subset.min(),
            'max_occupancy': subset.max(),
            'mw_pvalue_vs_balanced': mw_pvalue,
        })
    
    return pd.DataFrame(results)


def create_kde_plot(
    df: pd.DataFrame,
    g_type_col: str,
    threshold_label: str,
    output_path: Path
) -> None:
    """
    Create a KDE density plot comparing average occupancy distributions
    across the three schedule balance groups.
    """
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.set_style("whitegrid")
    
    # Plot KDE for each group
    for g_type, config in GROUP_CONFIG.items():
        subset = df[df[g_type_col] == g_type]['average_occupancy']
        if len(subset) > 1:  # Need at least 2 points for KDE
            sns.kdeplot(
                data=subset,
                label=f"{config['label']} (n={len(subset)})",
                color=config['color'],
                linewidth=2.5,
                fill=True,
                alpha=0.3,
                ax=ax
            )
    
    # Customize plot
    ax.set_xlabel('Average Stadium Occupancy (0 = Empty, 1 = Full)', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title(
        f'Distribution of Stadium Occupancy by Schedule Balance Type\n({threshold_label})',
        fontsize=14,
        fontweight='bold'
    )
    ax.legend(title='Schedule Balance Type', fontsize=10, title_fontsize=11)
    ax.set_xlim(0, 1.1)
    
    # Add reference lines
    ax.axvline(x=0.5, color='gray', linestyle='--', alpha=0.5, linewidth=1)
    ax.axvline(x=0.75, color='gray', linestyle=':', alpha=0.3, linewidth=1)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    logger.info(f"Saved KDE plot to {output_path}")


def print_summary(summary_df: pd.DataFrame, threshold_label: str) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print(f"OCCUPANCY KDE SUMMARY ({threshold_label})")
    print("=" * 70)
    
    for _, row in summary_df.iterrows():
        group_label = GROUP_CONFIG.get(row['group'], {}).get('label', row['group'])
        print(f"\n{group_label}:")
        print(f"  Samples: {row['n_samples']}")
        if row['n_samples'] > 0:
            print(f"  Mean Occupancy: {row['mean_occupancy']:.3f}")
            print(f"  Median Occupancy: {row['median_occupancy']:.3f}")
            print(f"  Std Dev: {row['std_occupancy']:.3f}")
            print(f"  Range: [{row['min_occupancy']:.3f}, {row['max_occupancy']:.3f}]")
            if pd.notna(row['mw_pvalue_vs_balanced']):
                sig = "***" if row['mw_pvalue_vs_balanced'] < 0.001 else \
                      "**" if row['mw_pvalue_vs_balanced'] < 0.01 else \
                      "*" if row['mw_pvalue_vs_balanced'] < 0.05 else ""
                print(f"  MW p-value vs Balanced: {row['mw_pvalue_vs_balanced']:.4f} {sig}")


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
    
    # Compute summary statistics
    logger.info("Computing summary statistics...")
    summary_df = compute_summary_statistics(df, g_type_col)
    
    # Save summary to CSV
    summary_path = output_dir / "occupancy_summary.csv"
    summary_df.to_csv(summary_path, index=False)
    logger.info(f"Saved summary to {summary_path}")
    
    # Create KDE plot
    logger.info("Creating KDE plot...")
    plot_path = output_dir / "occupancy_kde_all_groups.png"
    create_kde_plot(df, g_type_col, threshold_label, plot_path)
    
    # Print summary to console
    print_summary(summary_df, threshold_label)


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
        description="Generate KDE density plots of stadium occupancy by schedule balance type"
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
