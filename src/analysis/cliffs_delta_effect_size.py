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
"""

import ast
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Tuple, Optional


def load_data(csv_path: str) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    return pd.read_csv(csv_path)


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
    # Group by league and season
    groups = df.groupby(['league_name', 'season_year'])
    
    deltas_g_minus = []
    deltas_g_plus = []
    
    for (league, season), group_df in groups:
        # Get final positions for each G_type
        g0_positions = group_df[group_df[g_type_col] == 'balanced']['final_position'].values
        g_minus_positions = group_df[group_df[g_type_col] == 'unbalanced_weak']['final_position'].values
        g_plus_positions = group_df[group_df[g_type_col] == 'unbalanced_strong']['final_position'].values
        
        # Calculate Cliff's Delta for G- vs G0
        # Test group = G- (unbalanced_weak), Control = G0 (balanced)
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
        # Test group = G+ (unbalanced_strong), Control = G0 (balanced)
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


def create_effect_size_plot(
    deltas_df: pd.DataFrame,
    comparison_label: str,
    output_path: str,
    g_type_threshold: str
):
    """
    Create a box plot showing the distribution of Cliff's Delta values
    with 3 boxplots: All seasons, Before 2005, After 2005.
    
    Args:
        deltas_df: DataFrame with delta values for each season-league
        comparison_label: Label for the comparison (e.g., "G- vs G0")
        output_path: Path to save the plot
        g_type_threshold: The G_type threshold used (e.g., "0.300")
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.set_style("whitegrid")
    
    # Color based on comparison type
    if 'G-' in comparison_label:
        base_color = '#e74c3c'  # Red for G-
        test_label = 'G- (Easy Start / Unbalanced Weak)'
    else:
        base_color = '#3498db'  # Blue for G+
        test_label = 'G+ (Hard Start / Unbalanced Strong)'
    
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
        patch.set_linewidth(1.5)
        median.set_color('black')
        median.set_linewidth(2)
    
    # Style whiskers and caps
    for whisker in box['whiskers']:
        whisker.set_linewidth(1.5)
    for cap in box['caps']:
        cap.set_linewidth(1.5)
    
    # Add jittered points for each group
    for i, (data, pos, color) in enumerate(zip(data_groups, positions, colors)):
        jitter = np.random.normal(pos, 0.08, size=len(data))
        ax.scatter(
            jitter, 
            data, 
            alpha=0.4, 
            color=color, 
            s=25,
            edgecolor='white',
            linewidth=0.5,
            zorder=3
        )
    
    # Add reference lines
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=2, alpha=0.7, label='No Effect (δ=0)')
    ax.axhline(y=0.33, color='orange', linestyle=':', linewidth=1.5, alpha=0.7, label='Small Effect Threshold (±0.33)')
    ax.axhline(y=-0.33, color='orange', linestyle=':', linewidth=1.5, alpha=0.7)
    ax.axhline(y=0.474, color='red', linestyle=':', linewidth=1.5, alpha=0.7, label='Large Effect Threshold (±0.474)')
    ax.axhline(y=-0.474, color='red', linestyle=':', linewidth=1.5, alpha=0.7)
    
    # Calculate and display statistics for each group
    stats_lines = []
    for label_text, data in zip(labels, data_groups):
        n = len(data)
        if n > 0:
            median = data.median()
            n_pos = (data > 0).sum()
            pct_pos = 100 * n_pos / n
            effect = interpret_effect_size(median)
            stats_lines.append(f"{label_text}:")
            stats_lines.append(f"  N={n}, Med δ={median:.3f} ({effect})")
            stats_lines.append(f"  δ>0: {n_pos} ({pct_pos:.1f}%)")
    
    stats_text = "\n".join(stats_lines)
    
    ax.text(
        0.02, 0.98, stats_text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
        fontfamily='monospace'
    )
    
    # Customize plot
    ax.set_ylabel("Cliff's Delta (δ)", fontsize=12)
    ax.set_ylim(-1.1, 1.1)
    ax.set_xlim(0.3, 3.7)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, fontsize=11)
    
    # Title with interpretation guide
    title = f"Effect Size Distribution: {comparison_label}\n(G_type threshold α = {g_type_threshold})"
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    
    # Add interpretation guide
    interpretation = (
        "Interpretation:\n"
        f"δ > 0 → {test_label} has worse ranks (higher numbers)\n"
        f"δ < 0 → {test_label} has better ranks (lower numbers)"
    )
    ax.text(
        0.98, 0.02, interpretation,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='bottom',
        horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8)
    )
    
    ax.legend(loc='upper right', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")
    plt.close()


def print_summary_statistics(
    deltas_g_minus: pd.DataFrame, 
    deltas_g_plus: pd.DataFrame,
    g_type_threshold: str
):
    """Print summary statistics for both comparisons."""
    print("\n" + "="*70)
    print(f"CLIFF'S DELTA EFFECT SIZE SUMMARY (G_type threshold = {g_type_threshold})")
    print("="*70)
    
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


def save_delta_results(
    deltas_g_minus: pd.DataFrame, 
    deltas_g_plus: pd.DataFrame,
    output_dir: Path,
    g_type_threshold: str
):
    """Save the delta results to CSV files."""
    metrics_dir = output_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    
    threshold_suffix = g_type_threshold.replace(".", "")
    
    g_minus_path = metrics_dir / f"cliffs_delta_g_minus_vs_g0_threshold_{threshold_suffix}.csv"
    g_plus_path = metrics_dir / f"cliffs_delta_g_plus_vs_g0_threshold_{threshold_suffix}.csv"
    
    deltas_g_minus.to_csv(g_minus_path, index=False)
    deltas_g_plus.to_csv(g_plus_path, index=False)
    
    print(f"\nDelta results saved to:")
    print(f"  {g_minus_path}")
    print(f"  {g_plus_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Calculate and plot Cliff's Delta effect sizes for SoSB analysis"
    )
    parser.add_argument(
        '--threshold',
        type=str,
        default='0.300',
        choices=['0.200', '0.250', '0.300', '0.350', '0.400'],
        help='G_type threshold to use (default: 0.300)'
    )
    parser.add_argument(
        '--save-csv',
        action='store_true',
        help='Save delta results to CSV files'
    )
    
    args = parser.parse_args()
    g_type_col = f'G_type_{args.threshold}'
    
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    csv_path = project_root / "data" / "gold" / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"
    output_dir = project_root / "data" / "gold" / "analysis" / "figures"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading data from: {csv_path}")
    print(f"Using G_type column: {g_type_col}")
    
    # Load data
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records")
    
    # Check if the column exists
    if g_type_col not in df.columns:
        print(f"Error: Column '{g_type_col}' not found in data")
        return
    
    # Calculate deltas for each season-league combination
    print("\nCalculating Cliff's Delta for each season-league combination...")
    deltas_g_minus, deltas_g_plus = calculate_season_deltas(df, g_type_col)
    
    print(f"Calculated {len(deltas_g_minus)} deltas for G- vs G0")
    print(f"Calculated {len(deltas_g_plus)} deltas for G+ vs G0")
    
    # Print summary statistics
    print_summary_statistics(deltas_g_minus, deltas_g_plus, args.threshold)
    
    # Create and save plots
    threshold_suffix = args.threshold.replace(".", "")
    
    # Plot for G- vs G0
    output_path_g_minus = output_dir / f"cliffs_delta_g_minus_vs_g0_threshold_{threshold_suffix}.png"
    create_effect_size_plot(
        deltas_g_minus, 
        "G- vs G0", 
        str(output_path_g_minus),
        args.threshold
    )
    
    # Plot for G+ vs G0
    output_path_g_plus = output_dir / f"cliffs_delta_g_plus_vs_g0_threshold_{threshold_suffix}.png"
    create_effect_size_plot(
        deltas_g_plus, 
        "G+ vs G0", 
        str(output_path_g_plus),
        args.threshold
    )
    
    # Optionally save CSV results
    if args.save_csv:
        effect_size_dir = project_root / "data" / "gold" / "analysis" / "effect_size"
        save_delta_results(deltas_g_minus, deltas_g_plus, effect_size_dir, args.threshold)


if __name__ == "__main__":
    main()
