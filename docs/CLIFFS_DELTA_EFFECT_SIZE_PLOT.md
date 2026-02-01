# Cliff's Delta Effect Size Plot

This document explains the methodology and usage of the `cliffs_delta_effect_size.py` script, which creates box plots visualizing the distribution of Cliff's Delta effect sizes across all season-league combinations.

> [!TIP]
> **For AI Agents**: This script follows the batch processing template established in `LEAGUE_EFFECT_SIGNIFICANCE_PLOT.md` with the `--all` flag for running multiple thresholds.

---

## Overview

The plot answers the question: **What is the magnitude of the effect that schedule imbalance has on tournament performance?**

Unlike significance testing (p-values), Cliff's Delta quantifies the **practical significance** of the difference between groups. Each data point represents a single season-league combination, showing how consistently teams with unbalanced schedules finish in different positions compared to balanced teams.

### What This Script Produces

For each threshold configuration, the script generates:
1. **Box plots** showing Cliff's Delta distributions split by time period (All Seasons, Before 2005, 2005+)
2. **CSV summary files** with aggregate statistics

---

## Classification Modes

The script supports two mutually exclusive modes for classifying teams:

### Mode 1: G-Type (Default)

Uses pre-computed `G_type_X.XXX` columns based on fixed Spearman G coefficient thresholds.

**Classification logic:**
- `|G| > threshold` → unbalanced (strong if G > 0, weak if G < 0)
- `|G| ≤ threshold` → balanced

### Mode 2: P-Value

Dynamically classifies teams based on the statistical significance of the Spearman correlation.

**Classification logic:**
- `P_value < threshold` AND `G < 0` → **unbalanced_weak**
- `P_value < threshold` AND `G > 0` → **unbalanced_strong**
- `P_value ≥ threshold` → **balanced**

---

## Methodology

### Cliff's Delta (δ)

Cliff's Delta measures the probability that a randomly selected value from one group is greater than a randomly selected value from another group.

**Formula:**
```
δ = (#(test > control) - #(test < control)) / (n_test × n_control)
```

Where:
- **Test group**: G- (unbalanced_weak) or G+ (unbalanced_strong)
- **Control group**: G0 (balanced)

### Interpretation

| δ Value | Meaning |
|---------|---------|
| +1.0 | Every unbalanced team has worse rank than every balanced team |
| 0.0 | No systematic difference between groups |
| -1.0 | Every unbalanced team has better rank than every balanced team |

**Football context** (lower rank = better position):
- **δ > 0**: Unbalanced teams tend to finish in **worse** positions than balanced teams
- **δ < 0**: Unbalanced teams tend to finish in **better** positions than balanced teams

### Effect Size Thresholds

Using Vargha and Delaney's thresholds:

| |δ| Range | Interpretation |
|------------|----------------|
| < 0.147 | Negligible |
| 0.147 - 0.33 | Small |
| 0.33 - 0.474 | Medium |
| ≥ 0.474 | Large |

### Comparisons Made

For each season-league combination, two comparisons are calculated:
1. **G- vs G0**: Unbalanced weak (harder start) vs Balanced
2. **G+ vs G0**: Unbalanced strong (easier start) vs Balanced

> [!IMPORTANT]
> Comparisons are only made between teams from the **same season** to ensure fair comparison.

---

## Output Structure

Outputs are organized in **threshold-specific subfolders**:

```
data/gold/analysis/cliffs_delta/
├── gtype_0.200/
│   ├── cliffs_delta_gminus_vs_g0.png
│   ├── cliffs_delta_gplus_vs_g0.png
│   └── summary_statistics.csv
├── gtype_0.250/
│   └── ...
├── gtype_0.300/
│   └── ...
├── gtype_0.350/
│   └── ...
├── gtype_0.400/
│   └── ...
└── pvalue_0.1/
    └── ...
```

### Plot Files

Each comparison generates a box plot with:
- **Three boxes**: All Seasons, Before 2005, 2005 and After
- **Jittered points**: Individual season-league combinations
- **Reference lines**: Effect size thresholds (±0.33 small, ±0.474 large)
- **Statistics panel**: Sample sizes, median δ, % positive/negative

### Summary CSV Columns

| Column | Description |
|--------|-------------|
| `comparison` | Which groups are compared (G- vs G0 or G+ vs G0) |
| `threshold_label` | Classification threshold used |
| `n_season_league_combinations` | Total number of data points |
| `mean_n_test` | Average size of the test group |
| `mean_n_control` | Average size of the control group |
| `mean_delta` | Mean Cliff's Delta across all combinations |
| `median_delta` | Median Cliff's Delta |
| `std_delta` | Standard deviation of Cliff's Delta |
| `min_delta` / `max_delta` | Range of values |
| `n_positive_delta` / `pct_positive_delta` | Count/% of seasons with δ > 0 |
| `n_negative_delta` / `pct_negative_delta` | Count/% of seasons with δ < 0 |
| `effect_size_interpretation` | Interpretation of median effect size |
| `n_negligible_effect` | Count of negligible effects |
| `n_small_effect` | Count of small effects |
| `n_medium_effect` | Count of medium effects |
| `n_large_effect` | Count of large effects |

---

## Usage

### Run All Thresholds (Batch Mode)

```bash
python -m src.analysis.cliffs_delta_effect_size --all
```

This runs all thresholds defined in `src/analysis/parameters.py` and generates outputs in threshold-specific subfolders.

### Single Threshold

```bash
# G-type mode (default threshold 0.300)
python -m src.analysis.cliffs_delta_effect_size

# Custom G-type threshold
python -m src.analysis.cliffs_delta_effect_size --mode gtype --threshold 0.250

# P-value mode
python -m src.analysis.cliffs_delta_effect_size --mode pvalue --p-threshold 0.10
```

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--all` | Run ALL thresholds from `parameters.py` | False |
| `--mode` | Classification mode: `gtype` or `pvalue` | `gtype` |
| `--threshold` | G coefficient threshold (gtype mode) | `0.300` |
| `--p-threshold` | P-value threshold (pvalue mode) | `0.10` |
| `--input` | Path to input CSV | Auto |
| `--output-dir` | Base output directory | Auto |

---

## Parameters Configuration

Thresholds are defined in `src/analysis/parameters.py`:

```python
# G-type coefficient thresholds
G_TYPE_THRESHOLDS = ["0.200", "0.250", "0.300", "0.350", "0.400"]

# P-value thresholds
P_VALUE_THRESHOLDS = [0.10]

# Defaults
DEFAULT_G_TYPE_THRESHOLD = "0.300"
DEFAULT_P_VALUE_THRESHOLD = 0.10
```

To add new thresholds, simply edit this file and re-run with `--all`.

---

## Example Output Interpretation

### Scenario: Median δ = 0.15 for G- vs G0

This means:
- The effect is **small** (0.147 ≤ 0.15 < 0.33)
- On average across seasons, teams with harder starts (G-) finish in **slightly worse** positions than balanced teams
- The effect is **positive**, meaning δ > 0, so unbalanced_weak teams have worse ranks

### Scenario: Median δ = -0.05 for G+ vs G0

This means:
- The effect is **negligible** (|-0.05| < 0.147)
- Teams with easier starts (G+) finish in essentially the **same** positions as balanced teams
- Schedule imbalance has no practical significance for this comparison

---

## Relationship to Other Analyses

| Analysis | What it Measures |
|----------|------------------|
| **Mann-Whitney U Test** | Is the difference statistically significant? (p-value) |
| **Cliff's Delta** | How large is the difference? (effect size) |
| **League Effect Significance** | Combines both per-league (scatter plot) |

> [!NOTE]
> A result can be statistically significant (small p-value) but have negligible practical significance (small δ), especially with large sample sizes.

---

## Dependencies

- `pandas`, `numpy`, `scipy.stats`, `matplotlib`, `seaborn`
