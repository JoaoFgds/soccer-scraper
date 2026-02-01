# Stadium Occupancy KDE Plot

This document explains the methodology and usage of the `occupancy_kde_plot.py` script, which creates KDE (Kernel Density Estimation) plots visualizing the distribution of stadium occupancy across schedule balance groups.

> [!TIP]
> **For AI Agents**: This script serves as a reference implementation for batch processing with the `--all` flag. See the [Refactoring Template](#refactoring-template-for-other-scripts) section for guidance on updating other plot scripts.

---

## Overview

The plot answers the question: **Does schedule imbalance have a meaningful effect on stadium attendance/occupancy?**

Each density curve represents the distribution of average stadium occupancy values for teams in that schedule balance group. The X-axis shows occupancy (0 = empty, 1 = full), and the Y-axis shows the density of observations.

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

### 1. Data Sources

| File | Purpose |
|------|---------|
| `strength_schedule_balance.csv` | G coefficient, P_value, and `G_type_X.XXX` columns |
| `occupancy_audit_audience_filled_fb.csv` | Average occupancy for each team-season |

**Join Key:** `(league_name, season_year, team_canonical)`

### 2. Average Occupancy

The `average_occupancy` metric is calculated as the team's mean home game attendance divided by the stadium capacity for that season. Values range from 0 (empty) to 1 (full).

### 3. KDE Density Estimation

The script uses Kernel Density Estimation to create smooth probability density curves for each group. This allows visual comparison of the distributions without relying on histograms.

---

## Interpretation Guide

| Observation | Interpretation |
|-------------|----------------|
| Curves overlap significantly | No meaningful difference in occupancy between groups |
| One curve shifted right | That group tends to have higher stadium occupancy |
| One curve shifted left | That group tends to have lower stadium occupancy |
| Wider curve (more spread) | More variability in occupancy within that group |
| Significant MW p-value | Statistical evidence of a difference from balanced group |

---

## Output Structure

Outputs are organized in **threshold-specific subfolders**:

```
data/gold/analysis/occupancy_kde/
├── gtype_0.200/
│   ├── occupancy_kde_all_groups.png
│   └── occupancy_summary.csv
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

### Summary CSV Schema

| Column | Description |
|--------|-------------|
| `group` | G_type classification (balanced, unbalanced_weak, unbalanced_strong) |
| `n_samples` | Number of team-seasons in the group |
| `mean_occupancy` | Mean average occupancy |
| `median_occupancy` | Median average occupancy |
| `std_occupancy` | Standard deviation |
| `min_occupancy` | Minimum value |
| `max_occupancy` | Maximum value |
| `mw_pvalue_vs_balanced` | Mann-Whitney p-value comparing to balanced group |

---

## Usage

### Run All Thresholds (Batch Mode)

```bash
python -m src.analysis.occupancy_kde_plot --all
```

This runs all thresholds defined in `src/analysis/parameters.py`.

### Single Threshold

```bash
# G-type mode (default threshold 0.300)
python -m src.analysis.occupancy_kde_plot

# Custom G-type threshold
python -m src.analysis.occupancy_kde_plot --mode gtype --threshold 0.250

# P-value mode
python -m src.analysis.occupancy_kde_plot --mode pvalue --p-threshold 0.10
```

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--all` | Run ALL thresholds from `parameters.py` | False |
| `--mode` | Classification mode: `gtype` or `pvalue` | `gtype` |
| `--threshold` | G coefficient threshold (gtype mode) | `0.300` |
| `--p-threshold` | P-value threshold (pvalue mode) | `0.10` |
| `--input-balance` | Path to schedule balance CSV | Auto |
| `--input-occupancy` | Path to occupancy CSV | Auto |
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

To add new thresholds, simply edit this file.

---

## Refactoring Template for Other Scripts

To update other plot scripts with similar batch processing capability:

### 1. Import Parameters

```python
from src.analysis.parameters import (
    G_TYPE_THRESHOLDS,
    P_VALUE_THRESHOLDS,
    DEFAULT_G_TYPE_THRESHOLD,
    DEFAULT_P_VALUE_THRESHOLD,
)
```

### 2. Add CLI Arguments

```python
parser.add_argument("--all", action="store_true", help="Run ALL thresholds")
parser.add_argument("--mode", type=str, default="gtype", choices=["gtype", "pvalue"])
parser.add_argument("--threshold", type=str, default=DEFAULT_G_TYPE_THRESHOLD)
parser.add_argument("--p-threshold", type=float, default=DEFAULT_P_VALUE_THRESHOLD)
```

### 3. Refactor Analysis into Reusable Function

```python
def run_analysis(df, g_type_col, threshold_label, output_dir):
    """Run the full analysis pipeline for a given classification."""
    output_dir.mkdir(parents=True, exist_ok=True)
    # ... existing analysis logic ...
```

### 4. Add Batch Processing Function

```python
def run_all_thresholds(df, output_dir):
    for g_threshold in G_TYPE_THRESHOLDS:
        g_type_col = f"G_type_{g_threshold}"
        run_analysis(df, g_type_col, f"G: {g_threshold}", output_dir / f"gtype_{g_threshold}")
    
    for p_threshold in P_VALUE_THRESHOLDS:
        df_classified = classify_by_pvalue(df, p_threshold)
        run_analysis(df_classified, 'g_type_dynamic', f"p<{p_threshold}", output_dir / f"pvalue_{p_threshold}")
```

### 5. Output Subfolder Convention

Use consistent subfolder naming:
- G-type: `gtype_{threshold}` (e.g., `gtype_0.300`)
- P-value: `pvalue_{threshold}` (e.g., `pvalue_0.1`)

---

## Dependencies

- `pandas`, `numpy`, `scipy.stats`, `matplotlib`, `seaborn`
