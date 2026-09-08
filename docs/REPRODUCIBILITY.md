# Reproducibility

This document describes how to reproduce the historical outputs contributed by
the `TransfermarketPlayersValue` and `PedroTask_20260614` projects.

The imported code is traceable as follows:

- `src/market_value/` preserves the market-value extraction and Bronze-to-Silver
  processing developed in `TransfermarketPlayersValue`.
- `src/consolidate_football_data.py` preserves the team-season consolidation
  developed in `PedroTask_20260614`.
- No code was imported from the old `soccer_scraper` copy because its functional
  content is already present in the main repository.

## Reproduction boundary

Transfermarkt is a live source. Club values, historical coverage, redirects,
and HTML structure can change. Running the scraper again therefore creates a
new data collection; it cannot guarantee the exact historical results.

Exact reproduction uses a frozen input snapshot. That snapshot is intentionally
not tracked in Git and must be published separately as a versioned release
asset. Until the asset and its public download URL are added, the code is
reproducible against the local snapshot but the public repository is not yet a
self-contained reproduction package.

## Environment

The supported environment is Python 3.13 with the dependency versions recorded
in `uv.lock`.

```bash
git clone https://github.com/JoaoFgds/soccer-scraper.git
cd soccer-scraper
uv sync --locked
```

Run the complete code-only test suite without downloading or scraping data:

```bash
uv run --locked python -m unittest discover -s tests -v
```

## Frozen input snapshot

The reproduction archive must restore these files at their repository-relative
paths:

| File | Required columns | Historical rows |
|---|---|---:|
| `data/silver/final_standings_valid_market_ranking.csv` | `league_name`, `season_year`, `team_canonical`, `position_old`, `position`, `total_market_value_euros` | 5,944 |
| `data/gold/analysis/mann_whitney/attendance/audit/occupancy_audit_audience_filled_fb.csv` | `league_name`, `season_year`, `team_canonical`, `mean_attendance`, `max_attendance`, `average_occupancy` | 7,589 |
| `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance_market.csv` | `league_name`, `season_year`, `team_canonical`, `G`, `P_value` | 5,944 |
| `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance_ranking.csv` | `league_name`, `season_year`, `team_canonical`, `G`, `P_value` | 5,944 |

All four tables use the composite key `league_name`, `season_year`, and
`team_canonical`. The consolidation command rejects null, blank, or duplicate
keys instead of producing ambiguous joins.

The frozen standings contract is important: `position_old` is the final league
ranking and `position` is the market-value ranking. The snapshot must not be
replaced with a file that assigns different meanings to those names.

After restoring the snapshot, verify every file before running the code:

```bash
sha256sum -c reproducibility/consolidation-inputs.sha256
```

On macOS, use:

```bash
shasum -a 256 -c reproducibility/consolidation-inputs.sha256
```

## Exact team-season consolidation

Run:

```bash
uv run --locked python -m src.consolidate_football_data
```

The command writes
`data/gold/analysis/team_season_consolidated.csv`. It preserves the 5,944
reference rows and produces 15 columns. Verify it with:

```bash
sha256sum -c reproducibility/consolidation-output.sha256
```

or on macOS:

```bash
shasum -a 256 -c reproducibility/consolidation-output.sha256
```

Two historical naming rules are preserved for result compatibility:

- `avg_attendance` contains the source `average_occupancy` ratio, not a raw
  attendance count.
- `ranking_last_season` and `ranking_market_last_season` contain `-1` when the
  same club and league have no row for exactly the preceding calendar year.

With the verified frozen inputs, the expected output is 5,944 rows by 15
columns and SHA-256
`f4dee57e365c1a5322137cc7efac540143d3bb8ac3dfbd3c8d3f1bdf50162a8d`.

## Market-value collection

To collect one current league-season:

```bash
uv run --locked python -m src.market_value.main \
  --league premierleague \
  --season 2024
```

To collect the historical league/season scope configured in
`src/market_value/config.py`, omit both options. This performs 454 live requests
before retries and writes one CSV per successful league-season under
`data/bronze/market_values/`, plus `scraping_status.csv`.

Consolidate the Bronze files with:

```bash
uv run --locked python -m src.market_value.process_silver
```

The historical local snapshot contains 451 successful league-season files. Its
Silver result has 8,703 rows and the SHA-256 recorded in
`reproducibility/market-values-output.sha256`. Reproducing that exact file
requires publication of the corresponding frozen Bronze directory. A fresh
scrape validates the current pipeline, but is not evidence of historical byte
reproducibility.

## Manual publication still required

Before calling the GitHub repository fully reproducible:

1. Publish the frozen data archive in a stable, versioned location such as a
   GitHub Release or a research-data repository.
2. Add the archive URL, version, SHA-256, and extraction command to this file.
3. Confirm that redistribution of the snapshot is permitted and document its
   provenance and collection date.
4. Clone the public repository into a clean directory, download the archive,
   verify the manifests, and run the tests and consolidation command.
5. Confirm both the output row count and SHA-256 shown above.

Do not publish local virtual environments, logs, ZIP metadata, or generated
plots as source code. These remain ignored by `.gitignore`.
