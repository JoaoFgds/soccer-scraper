"""
SoSB Density Plot - Creates a KDE density plot of normalized rankings
grouped by G_type (balanced, unbalanced_weak, unbalanced_strong)
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
    # Parse R_array and get its length to determine number of opponents
    # Total teams = number of opponents + 1 (the team itself)
    df['num_teams'] = df['R_array'].apply(lambda x: len(ast.literal_eval(x)) + 1)
    
    # Compute normalized rank: (Rank - 1) / (Teams - 1)
    df['normalized_rank'] = (df['final_position'] - 1) / (df['num_teams'] - 1)
    
    return df


def create_density_plot(df: pd.DataFrame, g_type_column: str = 'G_type_0.300', 
                        output_path: str = None):
    """
    Create a KDE density plot with three curves:
    - G0: balanced
    - G-: unbalanced_weak  
    - G+: unbalanced_strong
    """
    # Map the G_type values to display labels
    g_type_mapping = {
        'balanced': 'G0 (Balanced)',
        'unbalanced_weak': 'G- (Unbalanced Weak)',
        'unbalanced_strong': 'G+ (Unbalanced Strong)'
    }
    
    # Set up the plot style
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    
    # Define colors for each group
    colors = {
        'G0 (Balanced)': '#2ecc71',           # Green
        'G- (Unbalanced Weak)': '#e74c3c',    # Red
        'G+ (Unbalanced Strong)': '#3498db'   # Blue
    }
    
    # Create KDE plot for each group
    for g_type, label in g_type_mapping.items():
        subset = df[df[g_type_column] == g_type]['normalized_rank']
        if len(subset) > 0:
            sns.kdeplot(
                data=subset,
                label=f"{label} (n={len(subset)})",
                color=colors[label],
                linewidth=2.5,
                fill=True,
                alpha=0.3
            )
    
    # Customize the plot
    plt.xlabel('Normalized Rank (0 = Champion, 1 = Last Place)', fontsize=12)
    plt.ylabel('Density', fontsize=12)
    plt.title('Distribution of Final Positions by Schedule Balance Type\n(Strength of Schedule Balance - SoSB)', 
              fontsize=14, fontweight='bold')
    plt.legend(title='Schedule Balance Type', fontsize=10, title_fontsize=11)
    plt.xlim(0, 1)
    
    # Add vertical line at midpoint for reference
    plt.axvline(x=0.5, color='gray', linestyle='--', alpha=0.5, label='_nolegend_')
    
    plt.tight_layout()
    
    # Save the plot if output path is provided
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_path}")
    
    plt.show()


def print_statistics(df: pd.DataFrame, g_type_column: str = 'G_type_0.300'):
    """Print summary statistics for each G_type group."""
    print("\n" + "="*60)
    print("Summary Statistics by Schedule Balance Type")
    print("="*60)
    
    for g_type in ['balanced', 'unbalanced_weak', 'unbalanced_strong']:
        subset = df[df[g_type_column] == g_type]['normalized_rank']
        print(f"\n{g_type.upper()}:")
        print(f"  Count: {len(subset)}")
        print(f"  Mean Normalized Rank: {subset.mean():.4f}")
        print(f"  Std Dev: {subset.std():.4f}")
        print(f"  Median: {subset.median():.4f}")
        print(f"  Min: {subset.min():.4f}, Max: {subset.max():.4f}")


def main():
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    csv_path = project_root / "data" / "gold" / "analysis" / "spearman_coefficient" / "metrics" / "strength_schedule_balance.csv"
    output_dir = project_root / "data" / "gold" / "analysis" / "figures"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "sosb_density_plot.png"
    
    print(f"Loading data from: {csv_path}")
    
    # Load and process data
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records")
    
    # Compute normalized ranks
    df = compute_normalized_rank(df)
    
    # Print statistics
    print_statistics(df)
    
    # Create and save the density plot
    create_density_plot(df, output_path=str(output_path))


if __name__ == "__main__":
    main()
