import logging
import pandas as pd

from thefuzz import process
from typing import List, Dict
from src.utils import paths


logger = logging.getLogger(__name__)


def create_optimal_name_mapping(
    league_name: str,
    season_year: int,
    canonical_names: List[str],
    other_names: List[str],
) -> pd.DataFrame:
    """Creates a mapping between two lists of names using fuzzy string matching.

    This function iterates through a list of 'other_names' and finds the best
    possible match for each within a 'canonical_names' list (ground truth)
    based on a similarity score calculated by `thefuzz` library.

    Args:
        league_name (str): The name of the league, used for context in the output.
        season_year (int): The season year, used for context in the output.
        canonical_names (List[str]): The definitive list of names to map against.
        other_names (List[str]): The list of names that need to be mapped.

    Returns:
        pd.DataFrame: A DataFrame containing the mappings, including the
            original names, the matched canonical names, and the similarity
            score. Returns an empty DataFrame if either input list is empty.
    """
    mappings: List[Dict] = []

    if not canonical_names or not other_names:
        logger.warning(
            "Mapping skipped for %s/%d due to empty name lists.",
            league_name,
            season_year,
        )
        return pd.DataFrame()

    for other_name in other_names:
        best_match, score = process.extractOne(other_name, canonical_names)

        mappings.append(
            {
                "league_name": league_name,
                "season_year": season_year,
                "canonical_name": best_match,
                "other_name": other_name,
                "precision_score": score,
                "is_confident_match": score >= 70,
            }
        )

    return pd.DataFrame(mappings)


def generate_and_save_name_mappings(
    standings_df: pd.DataFrame, games_df: pd.DataFrame
) -> pd.DataFrame:
    """Generates and saves team name mappings for each league and season.

    This function orchestrates the team name standardization process. It
    prioritizes a manually created mapping file for accuracy and then uses an
    automatic fuzzy-matching algorithm for any remaining unmapped names.

    The process is as follows:
    1.  Loads a manual override mapping file from the assets directory.
    2.  For each league and season, it identifies canonical names (from the
        standings data) and other names (from the games data).
    3.  Applies the manual mappings first to ensure user-defined corrections.
    4.  Runs the automatic fuzzy matching on any names not covered by the
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
            'home_team' and 'away_team' columns, which serve as the source of
            other names to be mapped.

    Returns:
        pd.DataFrame: A comprehensive DataFrame containing all generated mappings
            from all processed leagues and seasons.
    """
    logger.info("Starting team name mapping process...")

    # Load manual mappings from the assets file, if it exists.

    manual_map_dict = {}
    if paths.MANUAL_NAME_MAPPING_PATH.exists():
        try:
            manual_df = pd.read_csv(paths.MANUAL_NAME_MAPPING_PATH)
            manual_map_dict = dict(
                zip(manual_df["other_name"], manual_df["canonical_name"])
            )
            logger.info(
                "Loaded %d manual name mappings from assets.", len(manual_map_dict)
            )
        except Exception as e:
            logger.error("Failed to load manual mapping file: %s", e)
    else:
        logger.info(
            "No manual mapping file found at %s. Proceeding with automatic mapping.",
            paths.MANUAL_NAME_MAPPING_PATH,
        )

    all_mappings = []
    for (league, season), group in standings_df.groupby(["league_name", "season_year"]):
        try:
            canonical_names = sorted(group["team_sanitized"].unique())

            season_games = games_df[
                (games_df["league_name"] == league)
                & (games_df["season_year"] == season)
            ]
            other_names = sorted(
                pd.concat(
                    [season_games["home_team"], season_games["away_team"]]
                ).unique()
            )

            if not canonical_names or not other_names:
                logger.warning(
                    "Skipping mapping for %s/%d due to missing names.", league, season
                )
                continue

            # Apply user-defined manual mappings first for guaranteed accuracy.

            season_manual_maps = []
            manually_mapped_other = set()
            manually_mapped_canonical = set()

            for other_name in other_names:
                if other_name in manual_map_dict:
                    canonical_name = manual_map_dict[other_name]
                    if canonical_name in canonical_names:
                        season_manual_maps.append(
                            {
                                "league_name": league,
                                "season_year": season,
                                "canonical_name": canonical_name,
                                "other_name": other_name,
                                "precision_score": 101,  # Signifies a manual override.
                                "is_confident_match": True,
                            }
                        )
                        manually_mapped_other.add(other_name)
                        manually_mapped_canonical.add(canonical_name)

            # Exclude already mapped names from the automatic process.
            remaining_other = [o for o in other_names if o not in manually_mapped_other]
            remaining_canonical = [
                c for c in canonical_names if c not in manually_mapped_canonical
            ]

            # Run automatic fuzzy matching on the remaining names.
            automatic_maps_df = pd.DataFrame()
            if remaining_canonical and remaining_other:
                automatic_maps_df = create_optimal_name_mapping(
                    league, season, remaining_canonical, remaining_other
                )

            # Combine manual and automatic results for the current season.
            season_manual_maps_df = pd.DataFrame(season_manual_maps)
            final_season_mapping = pd.concat(
                [season_manual_maps_df, automatic_maps_df], ignore_index=True
            )

            output_path = (
                paths.NAME_MAPPINGS_INDIVIDUAL_DIR / f"{league}_{season}_mapping.csv"
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
    """Applies canonical name mappings to standings and games DataFrames.

    This function standardizes team names across the two main dataframes.
    In the standings data, it renames the existing 'team_sanitized' column to
    'team_canonical'. In the games data, it uses the mappings to create new
    'home_team_canonical' and 'away_team_canonical' columns by joining on the
    original team names.

    Args:
        standings_df (pd.DataFrame): The complete standings DataFrame with a
            'team_sanitized' column.
        games_df (pd.DataFrame): The complete games DataFrame with 'home_team'
            and 'away_team' columns.
        mappings_df (pd.DataFrame): The DataFrame of canonical name mappings
            generated by `generate_and_save_name_mappings`.

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

    # In games, map home and away teams using the confirmed mappings.

    map_home = mappings_df.rename(
        columns={"other_name": "home_team", "canonical_name": "home_team_canonical"}
    )
    map_away = mappings_df.rename(
        columns={"other_name": "away_team", "canonical_name": "away_team_canonical"}
    )

    games_enriched = pd.merge(
        games_df,
        map_home[["league_name", "season_year", "home_team", "home_team_canonical"]],
        on=["league_name", "season_year", "home_team"],
        how="left",
    )
    games_enriched = pd.merge(
        games_enriched,
        map_away[["league_name", "season_year", "away_team", "away_team_canonical"]],
        on=["league_name", "season_year", "away_team"],
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
