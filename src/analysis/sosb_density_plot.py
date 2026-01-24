"""
SoSB Density Plot - Creates KDE density plots of normalized rankings
grouped by G_type (balanced, unbalanced_weak, unbalanced_strong)
for each threshold level (0.200, 0.250, 0.300, 0.350)
"""

import ast
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def load_data(csv_path: str) -> pd.DataFrame:
    """Load the strength schedule balance CSV file."""
    return pd.read_csv(csv_path)


def compute_normalized_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the normalized rank for each team.
    
    NormalizedRank = (Rank - 1) / (Teams - 1)
    where 0 = Champion and 1 = Last Place
    
    The number of teams is derived from len(R_array) + 1
    (R_array contains opponents, so +1 for the team itself)
    """
    df['num_teams'] = df['R_array'].apply(lambda x: len(ast.literal_eval(x)) + 1)
    df['normalized_rank'] = (df['final_position'] - 1) / (df['num_teams'] - 1)
    return df


def create_multi_density_plot(df: pd.DataFrame, output_path: str = None):
    """
    Create a 2x2 grid of KDE density plots, one for each G_type threshold.
    Each plot shows three curves: G0 (balanced), G- (unbalanced_weak), G+ (unbalanced_strong)
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
    
    # Define colors for each group
    colors = {
        'balanced': '#2ecc71',           # Green
        'unbalanced_weak': '#e74c3c',    # Red
        'unbalanced_strong': '#3498db'   # Blue
    }
    
    # Set up the figure with 2x2 subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    axes = axes.flatten()
    sns.set_style("whitegrid")
    
    for idx, (g_type_col, threshold_label) in enumerate(zip(g_type_columns, threshold_labels)):
        ax = axes[idx]
        
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
        
        # Customize each subplot
        ax.set_xlabel('Normalized Rank (0 = Champion, 1 = Last Place)', fontsize=10)
        ax.set_ylabel('Density', fontsize=10)
        ax.set_title(f'SoSB Distribution - {threshold_label}', fontsize=12, fontweight='bold')
        ax.legend(title='Schedule Balance Type', fontsize=8, title_fontsize=9)
        ax.set_xlim(0, 1)
        ax.axvline(x=0.5, color='gray', linestyle='--', alpha=0.5)
    
    # Overall title
    fig.suptitle('Distribution of Final Positions by Schedule Balance Type\nAcross Different Significance Thresholds', 
                 fontsize=14, fontweight='bold', y=1.02)
    
    plt.tight_layout()
    
    # Save the plot if output path is provided
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_path}")
    
    plt.show()


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


def main():
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    csv_path = project_root / "data" / "gold" / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"
    output_dir = project_root / "data" / "gold" / "analysis" / "figures"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "sosb_density_plot_multi.png"
    
    print(f"Loading data from: {csv_path}")
    
    # Load and process data
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records")
    
    # Compute normalized ranks
    df = compute_normalized_rank(df)
    
    # Print statistics
    print_statistics(df)
    
    # Create and save the multi-panel density plot
    create_multi_density_plot(df, output_path=str(output_path))


if __name__ == "__main__":
    main()
