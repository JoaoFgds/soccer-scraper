# Reproducibility

This document describes how to reproduce the Soccer Scraper processing and
analysis results from a frozen, publicly available data snapshot.

## Reproduction boundary

Transfermarkt is a live source. Club values, historical coverage, redirects,
and HTML structure can change. Running the scraper again therefore creates a
new data collection; it cannot guarantee the exact historical results.

Exact reproduction uses the frozen input snapshot published in the
[`reproducibility-v1` GitHub Release](https://github.com/JoaoFgds/soccer-scraper/releases/tag/reproducibility-v1).
The data remain outside Git history while their version, download location, and
integrity checksums remain stable and public.

The exact analysis boundary begins at the frozen enriched inputs in that
release. The Bronze archives also allow the main processor and market-value
processor to be rerun and verified independently, but the repository does not
currently contain the transformation that joins their Silver outputs into
`final_standings_valid_market_ranking.csv`.

## Environment

The supported environment is Python 3.13 with the dependency versions recorded
in `uv.lock`. The commands require `uv`, `curl`, `unzip`, and a POSIX-compatible
shell. Byte-identical validation was performed on macOS. Linux commands are
provided below; Windows output bytes and checksums have not been validated.

```bash
git clone https://github.com/JoaoFgds/soccer-scraper.git
cd soccer-scraper
uv sync --locked
```

Run the complete code-only test suite without downloading or scraping data:

```bash
uv run --locked python -m unittest discover -s tests -v
```

## Download and restore the release data

Download the four release assets into an ignored working directory:

```bash
mkdir -p downloads/reproducibility-v1
cd downloads/reproducibility-v1

curl --fail --location --remote-name \
  https://github.com/JoaoFgds/soccer-scraper/releases/download/reproducibility-v1/analysis-inputs-v1.zip
curl --fail --location --remote-name \
  https://github.com/JoaoFgds/soccer-scraper/releases/download/reproducibility-v1/market-values-bronze-v1.zip
curl --fail --location --remote-name \
  https://github.com/JoaoFgds/soccer-scraper/releases/download/reproducibility-v1/soccer-scraper-bronze-v1.zip
curl --fail --location --remote-name \
  https://github.com/JoaoFgds/soccer-scraper/releases/download/reproducibility-v1/SHA256SUMS.txt
```

Verify the downloaded archives against both the manifest tracked by Git and the
copy supplied with the release. On Linux:

```bash
sha256sum -c ../../reproducibility/release-assets.sha256
sha256sum -c SHA256SUMS.txt
```

On macOS, use:

```bash
shasum -a 256 -c ../../reproducibility/release-assets.sha256
shasum -a 256 -c SHA256SUMS.txt
```

All three archives must report `OK` in both checks.

Restore the files to the repository paths expected by the pipelines:

```bash
mkdir -p ../../data/bronze/market_values
mkdir -p ../../data/silver
mkdir -p ../../data/gold/analysis/mann_whitney/attendance/audit
mkdir -p ../../data/gold/analysis/spearman_coefficient/metrics

unzip -qo soccer-scraper-bronze-v1.zip -d ../../data -x '__MACOSX/*'
unzip -jqo market-values-bronze-v1.zip 'bronze/*.csv' \
  -d ../../data/bronze/market_values
unzip -jqo analysis-inputs-v1.zip \
  source/final_standings_valid_market_ranking.csv \
  -d ../../data/silver
unzip -jqo analysis-inputs-v1.zip \
  source/occupancy_audit_audience_filled_fb.csv \
  -d ../../data/gold/analysis/mann_whitney/attendance/audit
unzip -jqo analysis-inputs-v1.zip \
  'source/strength_schedule_balance_*.csv' \
  -d ../../data/gold/analysis/spearman_coefficient/metrics

cd ../..
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

To collect one configured league-season from the live website:

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

The published snapshot contains 451 successful league-season files plus the
scraping status report. Its Silver result has 8,703 rows and the SHA-256
recorded in `reproducibility/market-values-output.sha256`. A fresh scrape
validates the current pipeline, but is not evidence of historical byte
reproducibility.

The configured historical scope ends in 2024. The main scraper configuration
also marks every league as already processed, so `python main.py scrape` skips
all leagues unless those flags are deliberately changed. Neither live scraper
is part of the exact reproduction workflow.

## Version 1 interpretation boundary

The v1 output contract preserves one historical column meaning that is
important when interpreting the results:

- `position_old` is the final league position.
- `position` is the market-value ranking.
- In `strength_schedule_balance_market.csv`, the historical `final_position`
  output is populated from `position`. The downstream market-proxy Cliff's delta
  and Mann-Whitney results therefore evaluate market-value rankings, not final
  league positions.

This behavior is retained so the published v1 tables remain byte-reproducible.
Changing the outcome to `position_old` would require recalculated results,
updated checksums, and a new reproducibility release.

## Full validation order

From a clean clone with the release data restored, run:

```bash
uv sync --locked
uv run --locked python -m unittest discover -s tests -v
uv run --locked python main.py process
sha256sum -c reproducibility/processor-outputs.sha256
uv run --locked python -m src.market_value.process_silver
sha256sum -c reproducibility/market-values-output.sha256
uv run --locked python main.py analysis
sha256sum -c reproducibility/analysis-outputs.sha256
sha256sum -c reproducibility/consolidation-inputs.sha256
uv run --locked python -m src.consolidate_football_data
sha256sum -c reproducibility/consolidation-output.sha256
```

On macOS, replace each `sha256sum -c` invocation with `shasum -a 256 -c`.

Do not substitute `python main.py all` for this sequence. Each command above
must exit successfully and every listed file must report `OK`; missing or
unusable processor and analysis inputs cause a non-zero exit status.

The processor manifest covers its six primary Silver and name-mapping CSVs.
The analysis manifest covers the 29 generated statistical CSVs. Schedule JSON
files and PNG figures are regenerated by the analysis command, but are not
byte-hashed: JSON files are supporting traces, while image bytes may differ
across operating systems and font-rendering backends without changing the
underlying statistical results.

The source website is Transfermarkt. The release is a research reproducibility
snapshot and Transfermarkt is not affiliated with this repository.
