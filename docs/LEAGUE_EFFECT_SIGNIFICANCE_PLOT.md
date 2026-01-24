# League Effect Size Significance Plot

This document explains the methodology and usage of the `league_effect_significance_plot.py` script, which creates scatter plots visualizing the relationship between effect size and statistical significance of schedule imbalance across leagues.

---

## Overview

The plot answers the question: **Does schedule imbalance have a meaningful and statistically significant effect on tournament performance across different leagues?**

Each point on the scatter plot represents a single league, with:
- **Y-axis (Cliff's Delta)**: Magnitude and direction of the effect
- **X-axis (-log₁₀(p-value))**: Statistical significance of the effect

---

## Methodology

### 1. Cliff's Delta (Effect Size)

Cliff's Delta (δ) measures the probability that a randomly selected team from one group outperforms a randomly selected team from another group.

#### Formula

```
δ = (#(unbalanced < balanced) - #(unbalanced > balanced)) / total_pairs
```

Where:
- `unbalanced < balanced` means the unbalanced team has a **better** (lower) final position
- `unbalanced > balanced` means the unbalanced team has a **worse** (higher) final position

#### Interpretation

| δ Value | Interpretation |
|---------|----------------|
| +1.0 | Every unbalanced team finishes above every balanced team |
| +0.5 | Unbalanced teams have 75% chance of better position |
| 0.0 | No difference between groups |
| -0.5 | Unbalanced teams have 25% chance of better position |
| -1.0 | Every unbalanced team finishes below every balanced team |

#### Within-Season Comparisons

**Critical design choice**: Comparisons are only made between teams from the **same season**.

```
For each league:
    For each season:
        For each unbalanced team in season:
            For each balanced team in season:
                Compare final positions
```

This ensures we compare "apples to apples" — a 5th place finish in 2015 is only compared to teams from 2015, not to teams from different seasons where competitive dynamics may differ.

### 2. Mann-Whitney U Test (Statistical Significance)

The Mann-Whitney U test is a non-parametric test that determines whether the distribution of final positions differs significantly between the unbalanced and balanced groups.

- **Null Hypothesis**: Final positions are equally distributed between groups
- **Alternative Hypothesis**: Distributions differ (two-sided test)
- **p-value < 0.05**: Evidence to reject null hypothesis

### 3. -log₁₀(p-value) Transformation

The x-axis uses `-log₁₀(p-value)` to create an intuitive significance scale:

| p-value | -log₁₀(p) | Interpretation |
|---------|-----------|----------------|
| 0.1 | 1.0 | Not significant |
| 0.05 | 1.3 | Marginally significant |
| 0.01 | 2.0 | Significant |
| 0.001 | 3.0 | Highly significant |

Higher values indicate stronger statistical evidence, placing significant leagues on the **right side** of the plot.

---

## Plot Interpretation

```
                         Effect Size vs Significance
    
    1.0 ┤ Unbalanced teams      ★         │  ★ Significant +
        │ finish BETTER              ●    │    large effect
    0.5 ┤                    ▲            │
        │              ◆                  │
    0.0 ┼────────────────────────────────────────────────
        │         ○                       │
   -0.5 ┤                    □            │
        │ Unbalanced teams           △    │  △ Significant +
   -1.0 ┤ finish WORSE                    │    large effect
        └──────────┬─────────┬───────────────
                   1       1.3           2
              Not significant   Significant
                        ↑
                    p = 0.05
```

### Quadrants

| Location | Interpretation |
|----------|----------------|
| Top-right | Unbalanced teams perform **better**, statistically significant |
| Top-left | Unbalanced teams perform **better**, not significant |
| Bottom-right | Unbalanced teams perform **worse**, statistically significant |
| Bottom-left | Unbalanced teams perform **worse**, not significant |

---

## Generated Comparisons

The script generates two separate plots:

### 1. G- (Unbalanced Weak) vs G0 (Balanced)

- **G- teams**: Teams that faced stronger opponents early in the season (negative Spearman G coefficient)
- **Question**: Do teams with a "harder start" perform differently than those with balanced schedules?

### 2. G+ (Unbalanced Strong) vs G0 (Balanced)

- **G+ teams**: Teams that faced weaker opponents early in the season (positive Spearman G coefficient)
- **Question**: Do teams with an "easier start" perform differently than those with balanced schedules?

---

## Output Files

The script generates the following outputs in `data/gold/analysis/league_significance/`:

| File | Description |
|------|-------------|
| `league_metrics_gminus_vs_g0_{threshold}.csv` | Raw metrics for G- vs G0 comparison |
| `league_metrics_gplus_vs_g0_{threshold}.csv` | Raw metrics for G+ vs G0 comparison |
| `league_significance_gminus_vs_g0_{threshold}.png` | Scatter plot for G- vs G0 |
| `league_significance_gplus_vs_g0_{threshold}.png` | Scatter plot for G+ vs G0 |

### CSV Columns

| Column | Description |
|--------|-------------|
| `league_name` | Name of the league |
| `cliffs_delta` | Effect size (-1 to +1) |
| `mann_whitney_stat` | Mann-Whitney U statistic |
| `p_value` | Two-sided p-value |
| `neg_log_p` | -log₁₀(p-value) |
| `n_unbalanced` | Number of teams in unbalanced group |
| `n_balanced` | Number of teams in balanced group |
| `n_pairs` | Total pairwise comparisons made |

---

## Usage

### Basic Usage

```bash
# Run with default threshold (0.300)
uv run python -m src.analysis.league_effect_significance_plot
```

### With Custom Threshold

```bash
# Use threshold 0.250 for G-type classification
uv run python -m src.analysis.league_effect_significance_plot --threshold 0.250
```

### Available Thresholds

- `0.200` - More permissive (more teams classified as unbalanced)
- `0.250`
- `0.300` - Default (matches theoretical methodology)
- `0.350`
- `0.400` - More strict (fewer teams classified as unbalanced)

### Custom Input/Output Paths

```bash
uv run python -m src.analysis.league_effect_significance_plot \
    --input /path/to/strength_schedule_balance.csv \
    --output-dir /path/to/output/
```

---

## Dependencies

The script uses:
- `pandas` - Data manipulation
- `numpy` - Numerical computations
- `scipy.stats` - Mann-Whitney U test
- `matplotlib` - Visualization

All dependencies are included in the project's `pyproject.toml`.

---

## Example Output

After running the script, you'll see summary statistics printed to the console:

```
======================================================================
LEAGUE EFFECT SIZE SUMMARY (Threshold: 0.300)
======================================================================

G- vs G0:
  Leagues analyzed: 21
  Significant (p < 0.05): 3
  Mean Cliff's Delta: -0.042
  Median Cliff's Delta: -0.031
  Range: [-0.183, 0.127]

  Significant leagues:
    - premierleague: δ=-0.156 (worse), p=0.0234
    ...
```

This summary helps quickly identify which leagues show significant effects of schedule imbalance on tournament performance.
