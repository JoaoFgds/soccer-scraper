"""
Attendance Momentum Plot - Creates time series plots showing how normalized
stadium occupancy evolves across rounds within a season, grouped by schedule
balance type (G-, G0, G+).

This script visualizes the "momentum" of stadium attendance throughout the
season, comparing teams with different schedule balance characteristics.
Each line represents a group, with 95% confidence intervals showing the
uncertainty in the mean occupancy estimate per round.

Design Decisions (see ATTENDANCE_MOMENTUM_PLOT.md for full documentation):
- X-axis: Round number (converted to integer)
- Y-axis: Average normalized occupancy (attendance / max_attendance)
- Grouping: By rounds across all teams (not per-team home game sequence)
- Aggregation: All leagues and seasons combined
- X-axis range: Limited to minimum rounds across all leagues
- Confidence intervals: 95% CI using mean ± 1.96 * (std / sqrt(n))

Two classification modes are available:
1. G-type mode (default): Uses pre-computed G_type columns with fixed thresholds
2. P-value mode: Uses Spearman p-value to dynamically classify schedules

Usage:
    # Single threshold (default G-type 0.300)
    python -m src.analysis.attendance_momentum_plot
    
    # All thresholds from parameters.py
    python -m src.analysis.attendance_momentum_plot --all
    
    # Custom single threshold
    python -m src.analysis.attendance_momentum_plot --mode gtype --threshold 0.250
    python -m src.analysis.attendance_momentum_plot --mode pvalue --p-threshold 0.10
"""

import argparse
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional, Tuple
from scipy import stats

from src.utils.paths import (
    GOLD_DATA_DIR,
    GAMES_VALID_PATH,
    SPEARMAN_BALANCE_PATH,
    MANN_WHITNEY_ATT_AUDIT_DIR,
    ATTENDANCE_MOMENTUM_DIR,
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
DEFAULT_GAMES_PATH = GAMES_VALID_PATH
DEFAULT_BALANCE_PATH = SPEARMAN_BALANCE_PATH
DEFAULT_OCCUPANCY_PATH = MANN_WHITNEY_ATT_AUDIT_DIR / "occupancy_audit_audience_filled_fb.csv"
DEFAULT_OUTPUT_DIR = ATTENDANCE_MOMENTUM_DIR

# Classification modes
MODE_GTYPE = "gtype"
MODE_PVALUE = "pvalue"

# Group display names and colors (consistent with other plot scripts)
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


def load_games_data(csv_path: Path) -> pd.DataFrame:
    """Load the team_games_valid.csv file and filter to home games only."""
    logger.info(f"Loading games data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Convert round to integer, handling potential non-numeric values
    df['round_int'] = pd.to_numeric(df['round'], errors='coerce')
    df = df.dropna(subset=['round_int'])
    df['round_int'] = df['round_int'].astype(int)
    
    logger.info(f"Loaded {len(df)} game records from {df['league_name'].nunique()} leagues")
    logger.info(f"Round range: {df['round_int'].min()} to {df['round_int'].max()}")
    
    return df


def load_occupancy_audit(csv_path: Path) -> pd.DataFrame:
    """Load the occupancy audit CSV file containing max_attendance."""
    logger.info(f"Loading occupancy audit data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} occupancy audit records")
    return df


def load_schedule_balance(csv_path: Path) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    logger.info(f"Loading schedule balance data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} schedule balance records")
    return df


def merge_all_data(
    games_df: pd.DataFrame,
    occupancy_df: pd.DataFrame,
    balance_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge all three data sources.
    
    Join keys:
    - games <-> occupancy: (league_name, season_year, home_team_canonical)
    - games <-> balance: (league_name, season_year, home_team_canonical)
    """
    logger.info("Merging datasets...")
    
    # First, join games with occupancy to get max_attendance
    # The occupancy audit uses 'team_canonical' which corresponds to 'home_team_canonical' in games
    merged = pd.merge(
        games_df,
        occupancy_df[['league_name', 'season_year', 'team_canonical', 'max_attendance']],
        left_on=['league_name', 'season_year', 'home_team_canonical'],
        right_on=['league_name', 'season_year', 'team_canonical'],
        how='inner'
    )
    
    logger.info(f"After joining with occupancy: {len(merged)} records")
    
    # Get relevant columns from balance_df for classification
    balance_cols = ['league_name', 'season_year', 'team_canonical', 'G', 'P_value']
    # Add all G_type columns
    g_type_cols = [col for col in balance_df.columns if col.startswith('G_type_')]
    balance_cols.extend(g_type_cols)
    
    # Join with schedule balance
    merged = pd.merge(
        merged,
        balance_df[balance_cols],
        left_on=['league_name', 'season_year', 'home_team_canonical'],
        right_on=['league_name', 'season_year', 'team_canonical'],
        how='inner',
        suffixes=('', '_balance')
    )
    
    logger.info(f"After joining with balance: {len(merged)} records")
    
    return merged


def compute_normalized_occupancy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute normalized occupancy as attendance / max_attendance.
    
    normalized_occupancy = audience_filled_fb / max_attendance
    """
    df = df.copy()
    
    # Ensure we have the required columns
    if 'audience_filled_fb' not in df.columns:
        raise ValueError("Column 'audience_filled_fb' not found in data")
    if 'max_attendance' not in df.columns:
        raise ValueError("Column 'max_attendance' not found in data")
    
    # Compute normalized occupancy
    df['normalized_occupancy'] = df['audience_filled_fb'] / df['max_attendance']
    
    # Clip to [0, 1] range (in case of data issues)
    df['normalized_occupancy'] = df['normalized_occupancy'].clip(0, 1)
    
    # Log statistics
    logger.info(f"Normalized occupancy stats: "
                f"mean={df['normalized_occupancy'].mean():.3f}, "
                f"median={df['normalized_occupancy'].median():.3f}, "
                f"std={df['normalized_occupancy'].std():.3f}")
    
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


def determine_min_rounds(df: pd.DataFrame) -> int:
    """
    Determine the minimum number of rounds across all leagues.
    
    This ensures all rounds in the plot have comparable sample sizes.
    """
    # Group by league and season to find max round per league-season
    max_rounds_per_season = df.groupby(['league_name', 'season_year'])['round_int'].max()
    
    # Get the minimum across all league-seasons
    min_rounds = max_rounds_per_season.min()
    
    logger.info(f"Minimum rounds across all leagues: {min_rounds}")
    logger.info(f"Max rounds range: {max_rounds_per_season.min()} to {max_rounds_per_season.max()}")
    
    return min_rounds


def compute_round_statistics(
    df: pd.DataFrame,
    g_type_col: str,
    min_rounds: int
) -> pd.DataFrame:
    """
    Compute statistics for each (round, g_type) combination.
    
    Returns DataFrame with:
    - round, g_type, n_samples, mean_occupancy, std_occupancy, ci_lower, ci_upper
    """
    # Filter to rounds <= min_rounds
    df_filtered = df[df['round_int'] <= min_rounds].copy()
    
    logger.info(f"Computing statistics for rounds 1 to {min_rounds}")
    logger.info(f"Records after filtering: {len(df_filtered)}")
    
    results = []
    groups = ['balanced', 'unbalanced_weak', 'unbalanced_strong']
    
    for round_num in range(1, min_rounds + 1):
        round_data = df_filtered[df_filtered['round_int'] == round_num]
        
        for g_type in groups:
            subset = round_data[round_data[g_type_col] == g_type]['normalized_occupancy']
            n = len(subset)
            
            if n == 0:
                results.append({
                    'round': round_num,
                    'g_type': g_type,
                    'n_samples': 0,
                    'mean_occupancy': np.nan,
                    'std_occupancy': np.nan,
                    'ci_lower': np.nan,
                    'ci_upper': np.nan,
                })
                continue
            
            mean_occ = subset.mean()
            std_occ = subset.std()
            
            # Compute 95% CI
            if n >= 2:
                # Use t-distribution for small samples
                if n < 30:
                    t_val = stats.t.ppf(0.975, df=n-1)
                    margin = t_val * (std_occ / np.sqrt(n))
                else:
                    # Use z-score for large samples
                    margin = 1.96 * (std_occ / np.sqrt(n))
                
                ci_lower = mean_occ - margin
                ci_upper = mean_occ + margin
            else:
                ci_lower = np.nan
                ci_upper = np.nan
            
            results.append({
                'round': round_num,
                'g_type': g_type,
                'n_samples': n,
                'mean_occupancy': mean_occ,
                'std_occupancy': std_occ,
                'ci_lower': ci_lower,
                'ci_upper': ci_upper,
            })
    
    return pd.DataFrame(results)


def create_momentum_plot(
    stats_df: pd.DataFrame,
    threshold_label: str,
    output_path: Path
) -> None:
    """
    Create a time series plot showing attendance momentum with CI bands.
    """
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot each group
    for g_type, config in GROUP_CONFIG.items():
        group_data = stats_df[stats_df['g_type'] == g_type].sort_values('round')
        
        if len(group_data) == 0 or group_data['mean_occupancy'].isna().all():
            continue
        
        rounds = group_data['round'].values
        means = group_data['mean_occupancy'].values
        ci_lower = group_data['ci_lower'].values
        ci_upper = group_data['ci_upper'].values
        n_total = group_data['n_samples'].sum()
        
        # Plot mean line
        ax.plot(
            rounds, means,
            label=f"{config['label']} (n={n_total})",
            color=config['color'],
            linewidth=2.5,
            marker='o',
            markersize=4,
            alpha=0.9
        )
        
        # Plot CI band
        ax.fill_between(
            rounds, ci_lower, ci_upper,
            color=config['color'],
            alpha=0.2
        )
    
    # Customize plot
    ax.set_xlabel('Round (Game Number in Season)', fontsize=12)
    ax.set_ylabel('Average Normalized Occupancy', fontsize=12)
    ax.set_title(
        f'Stadium Attendance Momentum Throughout Season\n({threshold_label})',
        fontsize=14,
        fontweight='bold'
    )
    
    # Set axis limits
    ax.set_xlim(1, stats_df['round'].max() + 0.5)
    ax.set_ylim(0, 1.05)
    
    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Add legend
    ax.legend(
        title='Schedule Balance Type',
        fontsize=10,
        title_fontsize=11,
        loc='lower right'
    )
    
    # Add reference line at 50% occupancy
    ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5, linewidth=1)
    
    # Add annotation for CI
    ax.text(
        0.02, 0.02,
        'Shaded areas: 95% Confidence Interval',
        transform=ax.transAxes,
        fontsize=9,
        color='gray',
        style='italic'
    )
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    logger.info(f"Saved momentum plot to {output_path}")


def print_summary(stats_df: pd.DataFrame, threshold_label: str) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print(f"ATTENDANCE MOMENTUM SUMMARY ({threshold_label})")
    print("=" * 70)
    
    for g_type in ['balanced', 'unbalanced_weak', 'unbalanced_strong']:
        group_data = stats_df[stats_df['g_type'] == g_type]
        label = GROUP_CONFIG.get(g_type, {}).get('label', g_type)
        
        print(f"\n{label}:")
        print(f"  Total samples across all rounds: {group_data['n_samples'].sum()}")
        print(f"  Overall mean occupancy: {group_data['mean_occupancy'].mean():.3f}")
        print(f"  Range of means: [{group_data['mean_occupancy'].min():.3f}, "
              f"{group_data['mean_occupancy'].max():.3f}]")


def run_analysis(
    df: pd.DataFrame,
    g_type_col: str,
    threshold_label: str,
    output_dir: Path,
    min_rounds: int
) -> None:
    """
    Run the full analysis pipeline for a given classification column.
    
    Args:
        df: Merged DataFrame with normalized occupancy and classification
        g_type_col: Name of the classification column
        threshold_label: Human-readable label for the threshold
        output_dir: Directory for output files (threshold-specific subfolder)
        min_rounds: Minimum number of rounds to include
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Compute round statistics
    logger.info("Computing round statistics...")
    stats_df = compute_round_statistics(df, g_type_col, min_rounds)
    
    # Save statistics to CSV
    stats_path = output_dir / "round_statistics.csv"
    stats_df.to_csv(stats_path, index=False)
    logger.info(f"Saved round statistics to {stats_path}")
    
    # Create momentum plot
    logger.info("Creating momentum plot...")
    plot_path = output_dir / "attendance_momentum_plot.png"
    create_momentum_plot(stats_df, threshold_label, plot_path)
    
    # Print summary to console
    print_summary(stats_df, threshold_label)


def run_single_threshold(
    df: pd.DataFrame,
    mode: str,
    g_threshold: Optional[str],
    p_threshold: Optional[float],
    output_dir: Path,
    min_rounds: int
) -> None:
    """Run analysis for a single threshold."""
    if mode == MODE_GTYPE:
        g_type_col = f"G_type_{g_threshold}"
        if g_type_col not in df.columns:
            raise ValueError(f"Column {g_type_col} not found in data")
        
        threshold_label = f"G threshold: {g_threshold}"
        subfolder = f"gtype_{g_threshold}"
        
        logger.info(f"Mode: G-type | Threshold: {g_threshold}")
        run_analysis(df, g_type_col, threshold_label, output_dir / subfolder, min_rounds)
        
    elif mode == MODE_PVALUE:
        if 'P_value' not in df.columns or 'G' not in df.columns:
            raise ValueError("P-value mode requires 'P_value' and 'G' columns")
        
        df_classified = classify_by_pvalue(df, p_threshold)
        threshold_label = f"Spearman p < {p_threshold}"
        subfolder = f"pvalue_{p_threshold}"
        
        logger.info(f"Mode: P-value | Threshold: {p_threshold}")
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_dir / subfolder, min_rounds)


def run_all_thresholds(df: pd.DataFrame, output_dir: Path, min_rounds: int) -> None:
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
        run_analysis(df, g_type_col, threshold_label, output_dir / subfolder, min_rounds)
    
    # Run all P-value thresholds
    for p_threshold in P_VALUE_THRESHOLDS:
        df_classified = classify_by_pvalue(df, p_threshold)
        threshold_label = f"Spearman p < {p_threshold}"
        subfolder = f"pvalue_{p_threshold}"
        
        logger.info(f"Processing P-value threshold: {p_threshold}")
        run_analysis(df_classified, 'g_type_dynamic', threshold_label, output_dir / subfolder, min_rounds)
    
    logger.info(f"Completed all thresholds. Results saved in subfolders under {output_dir}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate attendance momentum plots showing normalized occupancy across rounds"
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
        "--input-games",
        type=str,
        default=str(DEFAULT_GAMES_PATH),
        help="Path to team_games_valid.csv"
    )
    parser.add_argument(
        "--input-occupancy",
        type=str,
        default=str(DEFAULT_OCCUPANCY_PATH),
        help="Path to occupancy audit CSV"
    )
    parser.add_argument(
        "--input-balance",
        type=str,
        default=str(DEFAULT_BALANCE_PATH),
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
    games_path = Path(args.input_games)
    occupancy_path = Path(args.input_occupancy)
    balance_path = Path(args.input_balance)
    output_dir = Path(args.output_dir)
    
    # Load data
    games_df = load_games_data(games_path)
    occupancy_df = load_occupancy_audit(occupancy_path)
    balance_df = load_schedule_balance(balance_path)
    
    # Merge all data sources
    merged_df = merge_all_data(games_df, occupancy_df, balance_df)
    
    # Compute normalized occupancy
    merged_df = compute_normalized_occupancy(merged_df)
    
    # Determine minimum rounds across all leagues
    min_rounds = determine_min_rounds(merged_df)
    
    # Execute based on flags
    if args.all:
        run_all_thresholds(merged_df, output_dir, min_rounds)
    else:
        run_single_threshold(
            merged_df, args.mode, args.threshold, args.p_threshold, output_dir, min_rounds
        )
    
    logger.info("Done!")


if __name__ == "__main__":
    main()
