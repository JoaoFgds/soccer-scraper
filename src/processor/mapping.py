import logging
import numpy as np
import pandas as pd

from typing import List
from thefuzz import fuzz
from src.utils import paths
from scipy.optimize import linear_sum_assignment

logger = logging.getLogger(__name__)


def _create_optimal_one_to_one_mapping(
    league: str, season: int, canonical_names: List[str], other_names: List[str]
) -> pd.DataFrame:
    """Creates an optimal, one-to-one mapping using the linear assignment problem.

    This method ensures that each canonical name is matched with exactly one
    "other name" to maximize the total similarity score across all pairs. This
    is superior to a simple best-match approach, which could assign multiple
    "other names" to the same canonical name.

    The function constructs a cost matrix where the cost is inversely
    proportional to the string similarity score (`100 - score`). The
    `linear_sum_assignment` algorithm then finds the pairings that minimize the
    total cost, which corresponds to maximizing the total similarity.

    Args:
        league (str): The name of the league, used for context in the output.
        season (int): The season year, used for context in the output.
        canonical_names (List[str]): The definitive list of names (ground truth).
        other_names (List[str]): The list of names to be mapped.

    Returns:
        pd.DataFrame: A DataFrame containing the optimal one-to-one mappings,
            including names, scores, and a confidence flag. Returns an empty
            DataFrame if a valid mapping is not possible.
    """
    if not canonical_names or not other_names:
        return pd.DataFrame()

    # A unique one-to-one assignment requires at least as many "other" names as canonical names.
    if len(canonical_names) > len(other_names):
        logger.error(
            "Mapping error for %s %d: Fewer other_names (%d) than canonical_names (%d).",
            league,
            season,
            len(other_names),
            len(canonical_names),
        )
        return pd.DataFrame()

    score_matrix = np.array(
        [[fuzz.ratio(c, o) for o in other_names] for c in canonical_names]
    )

    cost_matrix = 100 - score_matrix

    row_indices, col_indices = linear_sum_assignment(cost_matrix)

    final_mapping = []
    for r, c in zip(row_indices, col_indices):
        canonical = canonical_names[r]
        other = other_names[c]
        score = score_matrix[r, c]

        final_mapping.append(
            {
                "league_name": league,
                "season_year": int(season),
                "canonical_name": canonical,
                "other_name": other,
                "precision_score": int(score),
                "is_confident_match": score >= 70,
            }
        )

    return pd.DataFrame(final_mapping)


def generate_and_save_name_mappings(
    standings_df: pd.DataFrame, games_df: pd.DataFrame
) -> pd.DataFrame:
    """Generates and saves team name mappings for each league and season.

    This function orchestrates the team name standardization process. It
    prioritizes a manually created mapping file for accuracy and then uses an
    optimal assignment algorithm for any remaining unmapped names.

    The process is as follows:
    1.  Loads a manual override mapping file from the assets directory.
    2.  For each league and season, it identifies canonical names (from the
        standings data) and other names (from the games data).
    3.  Applies the manual mappings first to ensure user-defined corrections.
    4.  Runs the optimal one-to-one mapping on any names not covered by the
        manual file.
    5.  Combines manual and automatic mappings.
    6.  Saves an individual mapping CSV file for the season and aggregates the
        results.
    7.  Saves a final, combined mapping file containing all mappings for all
        leagues and seasons.

    Args:
        standings_df (pd.DataFrame): The complete standings DataFrame, which must
            contain 'league_name', 'season_year', and 'team_sanitized' columns.
            The 'team_sanitized' column serves as the source of canonical names.
        games_df (pd.DataFrame): The complete games DataFrame, containing
            'home_team_sanitized' and 'away_team_sanitized' as the source of
            other names to be mapped.

    Returns:
        pd.DataFrame: A comprehensive DataFrame containing all generated mappings
            from all processed leagues and seasons.
    """
    logger.info("Starting team name mapping process...")
    manual_map_dict = {}
    if paths.MANUAL_NAME_MAPPING_PATH.exists():
        try:
            manual_df = pd.read_csv(paths.MANUAL_NAME_MAPPING_PATH)
            manual_map_dict = dict(
                zip(manual_df["other_name"], manual_df["canonical_name"])
            )
            logger.info("Loaded %d manual name mappings.", len(manual_map_dict))
        except Exception as e:
            logger.error("Failed to load manual mapping file: %s", e)
    else:
        logger.info("No manual mapping file found. Proceeding with automatic mapping.")

    all_mappings = []
    for (league, season), group in standings_df.groupby(["league_name", "season_year"]):
        try:
            canonical_names = sorted(group["team_sanitized"].unique())
            season_games = games_df[
                (games_df["league_name"] == league)
                & (games_df["season_year"] == season)
            ]

            # The 'other_names' must come from the sanitized columns, which are
            # guaranteed to exist and be clean after the pre-processor steps.
            other_names_raw = pd.concat(
                [
                    season_games["home_team_sanitized"],
                    season_games["away_team_sanitized"],
                ]
            ).unique()

            # The original (non-sanitized) names are needed to look up manual overrides.
            original_other_names = pd.concat(
                [season_games["home_team"], season_games["away_team"]]
            ).unique()

            if not canonical_names or len(other_names_raw) == 0:
                logger.warning(
                    "Skipping mapping for %s/%d due to missing names.", league, season
                )
                continue

            # Apply manual mappings first for guaranteed accuracy.
            season_manual_maps = []
            manually_mapped_other = set()
            manually_mapped_canonical = set()

            # Create a temporary lookup from original names to their sanitized versions.
            temp_other_to_sanitized = {
                row["home_team"]: row["home_team_sanitized"]
                for _, row in season_games[
                    ["home_team", "home_team_sanitized"]
                ].iterrows()
            }
            temp_other_to_sanitized.update(
                {
                    row["away_team"]: row["away_team_sanitized"]
                    for _, row in season_games[
                        ["away_team", "away_team_sanitized"]
                    ].iterrows()
                }
            )

            for name in original_other_names:
                if name in manual_map_dict:
                    canonical_name = manual_map_dict[name]
                    if canonical_name in canonical_names:
                        sanitized_other_name = temp_other_to_sanitized.get(name, name)
                        season_manual_maps.append(
                            {
                                "league_name": league,
                                "season_year": season,
                                "canonical_name": canonical_name,
                                "other_name": sanitized_other_name,
                                "precision_score": 101,  # Indicates manual override
                                "is_confident_match": True,
                            }
                        )
                        manually_mapped_other.add(sanitized_other_name)
                        manually_mapped_canonical.add(canonical_name)

            # Exclude already mapped names from the automatic process.
            remaining_other = sorted(
                [o for o in other_names_raw if o not in manually_mapped_other]
            )
            remaining_canonical = sorted(
                [c for c in canonical_names if c not in manually_mapped_canonical]
            )

            automatic_maps_df = pd.DataFrame()
            if remaining_canonical and remaining_other:
                automatic_maps_df = _create_optimal_one_to_one_mapping(
                    league, season, remaining_canonical, remaining_other
                )

            season_manual_maps_df = pd.DataFrame(season_manual_maps)
            final_season_mapping = pd.concat(
                [season_manual_maps_df, automatic_maps_df], ignore_index=True
            )

            if not final_season_mapping.empty:
                output_path = (
                    paths.NAME_MAPPINGS_INDIVIDUAL_DIR
                    / f"{league}_{season}_mapping.csv"
                )
                final_season_mapping.to_csv(output_path, index=False)
                all_mappings.append(final_season_mapping)

        except Exception as e:
            logger.error(
                "Failed to create name mapping for %s/%d: %s",
                league,
                season,
                e,
                exc_info=True,
            )

    if not all_mappings:
        logger.error("No name mappings could be generated across all seasons.")
        return pd.DataFrame()

    combined_mappings = pd.concat(all_mappings, ignore_index=True)
    combined_mappings.to_csv(paths.NAME_MAPPINGS_COMBINED_PATH, index=False)
    logger.info(
        "Saved combined name mappings for %d seasons to %s",
        len(standings_df.groupby(["league_name", "season_year"])),
        paths.NAME_MAPPINGS_COMBINED_PATH,
    )

    return combined_mappings


def apply_name_mappings(
    standings_df: pd.DataFrame, games_df: pd.DataFrame, mappings_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Applies canonical name mappings to the standings and games DataFrames.

    This function standardizes team names across the two main dataframes.
    - In `standings_df`, it renames 'team_sanitized' to 'team_canonical' as
      this is the source of truth.
    - In `games_df`, it merges the `mappings_df` to create new
      'home_team_canonical' and 'away_team_canonical' columns based on the
      original 'home_team' and 'away_team' names.

    Args:
        standings_df (pd.DataFrame): The complete standings DataFrame with a
            'team_sanitized' column.
        games_df (pd.DataFrame): The complete games DataFrame with 'home_team',
            'away_team', 'league_name', and 'season_year' columns.
        mappings_df (pd.DataFrame): The DataFrame of canonical name mappings,
            containing 'other_name' and 'canonical_name' columns.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the enriched
            standings and games DataFrames with standardized canonical team
            name columns.
    """
    if mappings_df.empty:
        logger.warning("Mapping DataFrame is empty. Canonical names may be incomplete.")
        standings_df = standings_df.rename(columns={"team_sanitized": "team_canonical"})
        games_df["home_team_canonical"] = None
        games_df["away_team_canonical"] = None
        return standings_df, games_df

    logger.info("Applying canonical name mappings to standings and games dataframes.")

    # In standings, the sanitized name is already the canonical source of truth.
    standings_enriched = standings_df.rename(
        columns={"team_sanitized": "team_canonical"}
    )

    # In games, map home and away teams using the mapping file.
    map_home = mappings_df.rename(
        columns={
            "other_name": "home_team_sanitized",
            "canonical_name": "home_team_canonical",
        }
    )
    map_away = mappings_df.rename(
        columns={
            "other_name": "away_team_sanitized",
            "canonical_name": "away_team_canonical",
        }
    )

    games_enriched = pd.merge(
        games_df,
        map_home[
            ["league_name", "season_year", "home_team_sanitized", "home_team_canonical"]
        ],
        on=["league_name", "season_year", "home_team_sanitized"],
        how="left",
    )
    games_enriched = pd.merge(
        games_enriched,
        map_away[
            ["league_name", "season_year", "away_team_sanitized", "away_team_canonical"]
        ],
        on=["league_name", "season_year", "away_team_sanitized"],
        how="left",
    )

    games_enriched = games_enriched.rename(
        columns={
            "home_team_canonical_y": "home_team_canonical",
            "away_team_canonical_y": "away_team_canonical",
        }
    )

    desired_order = [
        "id",
        "source_id",
        "standings_id",
        "round",
        "date",
        "time",
        "datetime",
        "home_team",
        "home_team_sanitized",
        "home_team_canonical",
        "away_team",
        "away_team_sanitized",
        "away_team_canonical",
        "formation",
        "coach",
        "coach_sanitized",
        "result",
        "audience",
        "audience_filled_fb",
        "audience_filled_mean",
        "audience_filled_median",
        "league_name",
        "season_year",
        "source_csv_file",
        "standings_csv_file",
    ]

    games_enriched = games_enriched[desired_order]

    return standings_enriched, games_enriched
