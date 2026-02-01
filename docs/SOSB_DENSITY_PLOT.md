# SoSB Density Plot

This document explains the methodology and usage of the `sosb_density_plot.py` script, which creates KDE density plots visualizing the distribution of final league positions by schedule balance type.

> [!TIP]
> **For AI Agents**: This script follows the batch processing template with the `--all` flag for running multiple thresholds.

---

## Overview

The plot answers the question: **Do teams with different schedule balance types finish in different positions?**

Each density curve shows how teams in a particular group (balanced, unbalanced weak, or unbalanced strong) are distributed across final league positions.

### What This Script Produces

For each threshold configuration:
1. **KDE density plots** showing position distributions for each group
2. **CSV summary files** with aggregate statistics
3. **Comparison plot** (when using `--all`) with all G-type thresholds side-by-side

---

## Classification Modes

### Mode 1: G-Type (Default)

Uses pre-computed `G_type_X.XXX` columns based on fixed Spearman G coefficient thresholds.

**Classification:**
- `|G| > threshold` → unbalanced
- `|G| ≤ threshold` → balanced

### Mode 2: P-Value

Dynamically classifies teams based on Spearman correlation significance.

**Classification:**
- `P_value < threshold` AND `G < 0` → **unbalanced_weak**
- `P_value < threshold` AND `G > 0` → **unbalanced_strong**
- `P_value ≥ threshold` → **balanced**

---

## Methodology

### Normalized Rank

Positions are normalized to enable comparison across leagues with different numbers of teams:

```
NormalizedRank = (Position - 1) / (NumTeams - 1)
```

| Value | Meaning |
|-------|---------|
| 0.0 | Champion (1st place) |
| 0.5 | Mid-table |
| 1.0 | Last place |

### Groups Visualized

| Group | Description | Color |
|-------|-------------|-------|
| G0 (Balanced) | Teams with balanced schedules | Green |
| G- (Unbalanced Weak) | Harder start (strong opponents early) | Red |
| G+ (Unbalanced Strong) | Easier start (weak opponents early) | Blue |

### Interpretation

- **Overlapping curves**: No difference between groups
- **G-/G+ shifted right**: Unbalanced teams finish worse
- **G-/G+ shifted left**: Unbalanced teams finish better

---

## Output Structure

```
data/gold/analysis/sosb_density/
├── gtype_0.200/
│   ├── sosb_density_plot.png
│   └── summary_statistics.csv
├── gtype_0.250/
│   └── ...
├── gtype_0.300/
│   └── ...
├── gtype_0.350/
│   └── ...
├── gtype_0.400/
│   └── ...
├── pvalue_0.1/
│   └── ...
└── sosb_density_comparison.png  (multi-panel comparison)
```

### Summary CSV Columns

| Column | Description |
|--------|-------------|
| `g_type` | Group identifier |
| `n_teams` | Number of teams in group |
| `mean_normalized_rank` | Average position (0=champion, 1=last) |
| `median_normalized_rank` | Median position |
| `std_normalized_rank` | Standard deviation |
| `n_top_half` / `pct_top_half` | Count/% finishing in top half |
| `n_bottom_half` / `pct_bottom_half` | Count/% finishing in bottom half |

---

## Usage

### Run All Thresholds (Batch Mode)

```bash
python -m src.analysis.sosb_density_plot --all
```

### Single Threshold

```bash
# G-type mode (default threshold 0.300)
python -m src.analysis.sosb_density_plot

# Custom G-type threshold
python -m src.analysis.sosb_density_plot --mode gtype --threshold 0.250

# P-value mode
python -m src.analysis.sosb_density_plot --mode pvalue --p-threshold 0.10
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

Defined in `src/analysis/parameters.py`:

```python
G_TYPE_THRESHOLDS = ["0.200", "0.250", "0.300", "0.350", "0.400"]
P_VALUE_THRESHOLDS = [0.10]
DEFAULT_G_TYPE_THRESHOLD = "0.300"
```

---

## Dependencies

- `pandas`, `numpy`, `matplotlib`, `seaborn`
