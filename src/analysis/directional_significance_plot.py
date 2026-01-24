"""
Directional Significance Diverging Bar Chart

This script creates a diverging bar chart showing the directional impact
of schedule imbalance on tournament performance. For seasons where the
Mann-Whitney U test shows statistical significance (p < 0.05), it determines
whether the unbalanced group (G- or G+) performed better or worse than
the balanced group (G0) by comparing median final positions.

Logic:
- If p < 0.05 AND Median(G_test) < Median(G0): Better Performance (lower rank = higher position)
- If p < 0.05 AND Median(G_test) > Median(G0): Worse Performance (higher rank = lower position)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def load_data(
    pvalues_path: str,
    schedule_balance_path: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the p-values and schedule balance data."""
    pvalues_df = pd.read_csv(pvalues_path)
    schedule_df = pd.read_csv(schedule_balance_path)
    return pvalues_df, schedule_df


def get_significant_seasons(
    pvalues_df: pd.DataFrame,
    comparison_col: str,
    alpha: float = 0.05
) -> list[tuple[str, int]]:
    """
    Get the list of (league_name, season_year) tuples where the p-value
    for the given comparison is significant (< alpha).
    """
    significant = pvalues_df[
        (pvalues_df[comparison_col].notna()) &
        (pvalues_df[comparison_col] < alpha)
    ]
    return list(zip(significant['league_name'], significant['season_year']))


def compute_median_ranks(
    schedule_df: pd.DataFrame,
    league_name: str,
    season_year: int,
    g_type_col: str
) -> dict[str, float]:
    """
    Compute median final_position for each group type in a specific season.
    
    Returns a dict with keys 'balanced', 'unbalanced_weak', 'unbalanced_strong'
    mapped to their median ranks.
    """
    season_data = schedule_df[
        (schedule_df['league_name'] == league_name) &
        (schedule_df['season_year'] == season_year)
    ]
    
    medians = {}
    for group_type in ['balanced', 'unbalanced_weak', 'unbalanced_strong']:
        group_data = season_data[season_data[g_type_col] == group_type]
        if len(group_data) > 0:
            medians[group_type] = group_data['final_position'].median()
        else:
            medians[group_type] = np.nan
    
    return medians


def analyze_directional_significance(
    pvalues_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
    g_type_col: str = 'G_type_0.300',
    alpha: float = 0.05
) -> dict:
    """
    Analyze directional significance for both G- vs G0 and G+ vs G0 comparisons.
    
    Returns a dict with counts of better, worse, and no significant difference
    for each comparison type.
    """
    results = {
        'G-_vs_G0': {'better': 0, 'worse': 0, 'not_significant': 0},
        'G+_vs_G0': {'better': 0, 'worse': 0, 'not_significant': 0}
    }
    
    # Analyze G- vs G0
    g_minus_significant = get_significant_seasons(pvalues_df, 'G-_vs_G0', alpha)
    for league, year in g_minus_significant:
        medians = compute_median_ranks(schedule_df, league, year, g_type_col)
        
        if np.isnan(medians.get('unbalanced_weak', np.nan)) or np.isnan(medians.get('balanced', np.nan)):
            continue
            
        if medians['unbalanced_weak'] < medians['balanced']:
            # G- has lower median rank = better final position
            results['G-_vs_G0']['better'] += 1
        elif medians['unbalanced_weak'] > medians['balanced']:
            # G- has higher median rank = worse final position
            results['G-_vs_G0']['worse'] += 1
    
    # Analyze G+ vs G0 (note: column name is G0_vs_G+ in the CSV)
    g_plus_significant = get_significant_seasons(pvalues_df, 'G0_vs_G+', alpha)
    for league, year in g_plus_significant:
        medians = compute_median_ranks(schedule_df, league, year, g_type_col)
        
        if np.isnan(medians.get('unbalanced_strong', np.nan)) or np.isnan(medians.get('balanced', np.nan)):
            continue
            
        if medians['unbalanced_strong'] < medians['balanced']:
            # G+ has lower median rank = better final position
            results['G+_vs_G0']['better'] += 1
        elif medians['unbalanced_strong'] > medians['balanced']:
            # G+ has higher median rank = worse final position
            results['G+_vs_G0']['worse'] += 1
    
    # Count non-significant seasons
    total_g_minus = pvalues_df['G-_vs_G0'].notna().sum()
    total_g_plus = pvalues_df['G0_vs_G+'].notna().sum()
    
    results['G-_vs_G0']['not_significant'] = total_g_minus - len(g_minus_significant)
    results['G+_vs_G0']['not_significant'] = total_g_plus - len(g_plus_significant)
    
    return results


def create_diverging_bar_chart(
    results: dict,
    output_path: str,
    g_type_col: str = 'G_type_0.300'
) -> None:
    """
    Create a diverging bar chart showing directional significance.
    
    Left (red): Count of seasons where unbalanced group performed WORSE
    Right (green): Count of seasons where unbalanced group performed BETTER
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Data preparation
    categories = ['G⁻ vs G₀\n(Easy Start vs Balanced)', 'G⁺ vs G₀\n(Hard Start vs Balanced)']
    better_counts = [results['G-_vs_G0']['better'], results['G+_vs_G0']['better']]
    worse_counts = [results['G-_vs_G0']['worse'], results['G+_vs_G0']['worse']]
    not_sig_counts = [results['G-_vs_G0']['not_significant'], results['G+_vs_G0']['not_significant']]
    
    y_pos = np.arange(len(categories))
    bar_height = 0.5
    
    # Create diverging bars
    # Worse performance (left side, negative values, red)
    bars_worse = ax.barh(y_pos, [-w for w in worse_counts], height=bar_height, 
                         color='#E74C3C', label='Worse Performance', edgecolor='white', linewidth=0.5)
    
    # Better performance (right side, positive values, green)
    bars_better = ax.barh(y_pos, better_counts, height=bar_height,
                          color='#27AE60', label='Better Performance', edgecolor='white', linewidth=0.5)
    
    # Add count labels on bars
    for i, (worse, better) in enumerate(zip(worse_counts, better_counts)):
        # Worse count label (left side)
        if worse > 0:
            ax.text(-worse/2, i, str(worse), ha='center', va='center', 
                   fontweight='bold', color='white', fontsize=12)
        
        # Better count label (right side)
        if better > 0:
            ax.text(better/2, i, str(better), ha='center', va='center',
                   fontweight='bold', color='white', fontsize=12)
    
    # Add not significant counts as annotations
    for i, not_sig in enumerate(not_sig_counts):
        max_val = max(max(worse_counts), max(better_counts)) + 2
        ax.text(max_val + 1, i, f'(NS: {not_sig})', ha='left', va='center',
               fontsize=10, color='gray', style='italic')
    
    # Customize axes
    ax.set_yticks(y_pos)
    ax.set_yticklabels(categories, fontsize=12)
    ax.set_xlabel('Number of Seasons', fontsize=12)
    
    # Add vertical line at zero
    ax.axvline(x=0, color='black', linewidth=1)
    
    # Set x-axis to be symmetric
    max_count = max(max(worse_counts), max(better_counts))
    ax.set_xlim(-max_count - 3, max_count + 8)
    
    # Custom x-axis labels (absolute values)
    current_ticks = ax.get_xticks()
    ax.set_xticklabels([str(abs(int(t))) for t in current_ticks])
    
    # Add labels for left/right sides
    ax.text(-max_count/2, len(categories) + 0.3, 'WORSE\n(Higher Final Rank)', 
           ha='center', va='bottom', fontsize=10, color='#E74C3C', fontweight='bold')
    ax.text(max_count/2, len(categories) + 0.3, 'BETTER\n(Lower Final Rank)', 
           ha='center', va='bottom', fontsize=10, color='#27AE60', fontweight='bold')
    
    # Title and legend
    threshold = g_type_col.replace('G_type_', '')
    ax.set_title(f'Directional Significance of Schedule Imbalance Impact\n'
                 f'(Threshold: {threshold}, α = 0.05)', fontsize=14, fontweight='bold', pad=30)
    
    ax.legend(loc='lower right', framealpha=0.9)
    
    # Grid
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    # Add footnote
    fig.text(0.02, 0.02, 'NS = Not Significant (p ≥ 0.05)', fontsize=9, color='gray', style='italic')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"Chart saved to: {output_path}")


def main(g_type_col: str = 'G_type_0.300'):
    """Main function to generate the directional significance plot."""
    # Define paths
    base_path = Path(__file__).parent.parent.parent
    pvalues_path = base_path / 'data/gold/analysis/mann_whitney/seasons/metrics/per_season_mann_whitney_p_values.csv'
    schedule_path = base_path / 'data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance.csv'
    output_dir = base_path / 'data/gold/analysis/figures'
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    print("Loading data...")
    pvalues_df, schedule_df = load_data(str(pvalues_path), str(schedule_path))
    
    # Verify the g_type_col exists
    if g_type_col not in schedule_df.columns:
        available_cols = [c for c in schedule_df.columns if c.startswith('G_type')]
        raise ValueError(f"Column '{g_type_col}' not found. Available: {available_cols}")
    
    # Analyze directional significance
    print(f"Analyzing directional significance with threshold: {g_type_col}...")
    results = analyze_directional_significance(pvalues_df, schedule_df, g_type_col)
    
    # Print summary
    print("\n=== Results Summary ===")
    print(f"\nG⁻ vs G₀ (Easy Start vs Balanced):")
    print(f"  Better Performance: {results['G-_vs_G0']['better']}")
    print(f"  Worse Performance:  {results['G-_vs_G0']['worse']}")
    print(f"  Not Significant:    {results['G-_vs_G0']['not_significant']}")
    
    print(f"\nG⁺ vs G₀ (Hard Start vs Balanced):")
    print(f"  Better Performance: {results['G+_vs_G0']['better']}")
    print(f"  Worse Performance:  {results['G+_vs_G0']['worse']}")
    print(f"  Not Significant:    {results['G+_vs_G0']['not_significant']}")
    
    # Create chart
    threshold = g_type_col.replace('G_type_', '')
    output_path = output_dir / f'directional_significance_{threshold}.png'
    print(f"\nGenerating chart...")
    create_diverging_bar_chart(results, str(output_path), g_type_col)
    
    return results


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate directional significance diverging bar chart'
    )
    parser.add_argument(
        '--threshold',
        type=str,
        default='0.300',
        help='G-type threshold to use (e.g., 0.200, 0.250, 0.300, 0.350, 0.400). Default: 0.300'
    )
    
    args = parser.parse_args()
    g_type_col = f'G_type_{args.threshold}'
    
    main(g_type_col)
