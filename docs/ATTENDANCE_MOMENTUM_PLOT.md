# Attendance Momentum Plot

This document explains the methodology and usage of the `attendance_momentum_plot.py` script, which creates time series plots visualizing how normalized stadium occupancy evolves across rounds within a season.

> [!TIP]
> **For AI Agents**: This script follows the same patterns as `occupancy_kde_plot.py` for batch processing with the `--all` flag. See the [Refactoring Template](#refactoring-template-for-other-scripts) section in that documentation for guidance.

---

## Overview

The plot answers the question: **Does schedule imbalance correlate with different attendance patterns throughout the season?**

Each line on the plot represents a schedule balance group (G-, G0, G+), showing how average normalized occupancy changes from round 1 to the final round. Shaded bands show the 95% confidence interval for each point.

---

## Design Decisions

The following design decisions were made during implementation:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **X-axis definition** | Round number (converted to integer) | More reliable than chronological ordering |
| **Grouping strategy** | Group by rounds across all teams | Not per-team home game sequence |
| **Number of lines** | 3 (G-, G0, G+) | One line per schedule balance group |
| **Confidence interval** | 95% CI around each point | Each point is based on a distribution of occupancies |
| **League/Season aggregation** | Aggregate ALL leagues and seasons | Single combined plot |
| **X-axis range** | Limited to minimum rounds across data | Ensures all data points have comparable sample sizes |
| **Normalized occupancy** | `audience_filled_fb / max_attendance` | Using forward/backward filled attendance |

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
| `data/silver/team_games_valid.csv` | Game-level attendance data with round, home team, and league/season info |
| `data/gold/analysis/mann_whitney/attendance/audit/occupancy_audit_audience_filled_fb.csv` | Max attendance (stadium capacity) per team-season |
| `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance.csv` | G coefficient and group classification per team-season |

**Join Keys:**
- `(league_name, season_year, home_team_canonical)` for games and occupancy
- `(league_name, season_year, home_team_canonical)` for games and balance

### 2. Normalized Occupancy Calculation

For each home game:
```
normalized_occupancy = audience_filled_fb / max_attendance
```

Values are clipped to the [0, 1] range.

### 3. Minimum Rounds Determination

Different leagues have different numbers of rounds (e.g., 20-team league has 38 rounds, 18-team has 34). To ensure fair comparison:

1. Find the maximum round for each (league, season) combination
2. Take the minimum across all league-seasons
3. Filter data to include only rounds ≤ min_rounds

### 4. Per-Round Statistics

For each (round, G_type) combination:
- Collect all normalized_occupancy values from all teams playing at home on that round
- Compute: mean, standard deviation, sample size
- Compute 95% CI using:
  - **Small samples (n < 30)**: `mean ± t(0.975, n-1) * (std / √n)`
  - **Large samples (n ≥ 30)**: `mean ± 1.96 * (std / √n)`

---

## Interpretation Guide

| Observation | Interpretation |
|-------------|----------------|
| Lines stay close together | No meaningful difference in attendance patterns between groups |
| One line consistently higher | That group tends to have higher stadium occupancy throughout the season |
| Diverging lines over time | Schedule balance may influence attendance momentum |
| Converging lines over time | Initial differences diminish as season progresses |
| Wide CI bands | High variability or small sample size for that group-round |
| Non-overlapping CI bands | Statistically significant difference between groups at that round |

---

## Output Structure

Outputs are organized in **threshold-specific subfolders**:

```
data/gold/analysis/attendance_momentum/
├── gtype_0.200/
│   ├── attendance_momentum_plot.png
│   └── round_statistics.csv
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

### round_statistics.csv Schema

| Column | Description |
|--------|-------------|
| `round` | Round number (integer) |
| `g_type` | Group classification (balanced, unbalanced_weak, unbalanced_strong) |
| `n_samples` | Number of observations for this round-group |
| `mean_occupancy` | Mean normalized occupancy |
| `std_occupancy` | Standard deviation |
| `ci_lower` | 95% CI lower bound |
| `ci_upper` | 95% CI upper bound |

---

## Usage

### Run All Thresholds (Batch Mode)

```bash
python -m src.analysis.attendance_momentum_plot --all
```

This runs all thresholds defined in `src/analysis/parameters.py`.

### Single Threshold

```bash
# G-type mode (default threshold 0.300)
python -m src.analysis.attendance_momentum_plot

# Custom G-type threshold
python -m src.analysis.attendance_momentum_plot --mode gtype --threshold 0.250

# P-value mode
python -m src.analysis.attendance_momentum_plot --mode pvalue --p-threshold 0.10
```

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--all` | Run ALL thresholds from `parameters.py` | False |
| `--mode` | Classification mode: `gtype` or `pvalue` | `gtype` |
| `--threshold` | G coefficient threshold (gtype mode) | `0.300` |
| `--p-threshold` | P-value threshold (pvalue mode) | `0.10` |
| `--input-games` | Path to team_games_valid.csv | Auto |
| `--input-occupancy` | Path to occupancy audit CSV | Auto |
| `--input-balance` | Path to schedule balance CSV | Auto |
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

## Technical Notes

### Why Limit to Minimum Rounds?

Different leagues have varying numbers of teams:
- 20-team league → 38 rounds (home + away)
- 18-team league → 34 rounds
- 16-team league → 30 rounds

If we plotted all rounds, later rounds would have progressively fewer data points (only from leagues with more rounds). By limiting to the minimum, all rounds have comparable sample sizes across all leagues.

### Confidence Interval Formula

For sample size `n`, mean `μ`, and standard deviation `σ`:

**Small samples (n < 30):**
```
CI = μ ± t(0.975, df=n-1) × (σ / √n)
```

**Large samples (n ≥ 30):**
```
CI = μ ± 1.96 × (σ / √n)
```

The t-distribution is used for small samples to account for the additional uncertainty in estimating σ.

---

## Dependencies

- `pandas`, `numpy`, `scipy.stats`, `matplotlib`
