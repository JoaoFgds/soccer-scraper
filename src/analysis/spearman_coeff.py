import json
import logging
import numpy as np
import pandas as pd

from src.utils import paths
from scipy.stats import spearmanr
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy and pandas data types.

    This class extends the default JSON encoder to provide serialization for
    common data types used in NumPy and pandas, such as integers, floats,
    arrays, and missing values, which are not natively supported by JSON.
    """

    def default(self, obj: Any) -> Any:
        """Serializes NumPy types into native Python types for JSON compatibility.

        This method is called for any object that is not a primitive type. It
        checks if the object is a NumPy integer, float, or array, or a pandas
        NA value, and converts it to a JSON-serializable format.

        Args:
            obj (Any): The object to serialize.

        Returns:
            Any: The JSON-serializable representation of the object.
        """
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if pd.isna(obj):
            return None
        return super().default(obj)


def _get_first_round_opponents(
    team_canonical: str, season_games_df: pd.DataFrame
) -> List[str]:
    """Retrieves the ordered list of unique opponents a team faced in the first round.

    The function identifies all of a team's matches within a given season, sorts
    them chronologically, and extracts the opponent from each match. It then
    returns a list of unique opponent names, preserving the order of their
    first encounter. This effectively represents the team's schedule for the
    first half of a double round-robin season.

    Args:
        team_canonical (str): The canonical name of the team.
        season_games_df (pd.DataFrame): A DataFrame containing all games for the
            specific league and season.

    Returns:
        List[str]: An ordered list of unique opponent canonical names, based on
            the sequence of their first encounter with the specified team.
    """
    team_games = season_games_df[
        (season_games_df["home_team_canonical"] == team_canonical)
        | (season_games_df["away_team_canonical"] == team_canonical)
    ].copy()

    team_games["datetime"] = pd.to_datetime(team_games["datetime"], errors="coerce")
    team_games.sort_values(by="datetime", ascending=True, inplace=True)

    opponents_in_order = []
    for _, row in team_games.iterrows():
        opponent = (
            row["away_team_canonical"]
            if row["home_team_canonical"] == team_canonical
            else row["home_team_canonical"]
        )
        if opponent != team_canonical:
            opponents_in_order.append(opponent)

    return list(dict.fromkeys(opponents_in_order))


def _calculate_g_coefficient(
    r_list: List[int], s_list: List[int], team_name: str
) -> Tuple[float | None, str | None]:
    """Calculates Spearman's G coefficient and classifies the schedule type.

    The G coefficient is the Spearman's rank correlation between an ideal
    schedule (`r_list`, opponents ranked from best to worst) and the team's
    actual schedule (`s_list`, the ranks of opponents in chronological order).
    The result is classified as 'unbalanced_strong', 'unbalanced_weak', or
    'balanced' based on predefined correlation thresholds.

    Args:
        r_list (List[int]): The list of all possible opponent ranks, sorted.
        s_list (List[int]): The list of the team's actual opponent ranks in the
            order they were played.
        team_name (str): The canonical name of the team, used for logging purposes.

    Returns:
        Tuple[float | None, str | None]: A tuple containing the calculated G
            coefficient and its string classification (e.g., 'balanced').
            Returns (None, None) if the calculation is not possible.
    """
    if len(r_list) != len(s_list) or len(s_list) <= 1:
        return None, None

    g, _ = spearmanr(r_list, s_list)
    g = float(g) if not pd.isna(g) else None

    if g is None:
        logger.warning("Spearman correlation returned NaN for %s.", team_name)
        return None, None

    if g > 0.3:
        g_type = "unbalanced_strong"
    elif g < -0.3:
        g_type = "unbalanced_weak"
    else:
        g_type = "balanced"

    return g, g_type


def calculate_strength_schedule_balance() -> pd.DataFrame:
    """Calculates schedule balance for all teams using Spearman's G coefficient.

    This is the main analysis function. It measures the correlation between the
    final rank of a team's opponents and the chronological order in which they
    were played during the first half of the season. A high positive
    correlation ('unbalanced_strong') suggests a team played weaker opponents
    first and stronger ones later. A high negative correlation
    ('unbalanced_weak') suggests the opposite.

    The function relies on the validated data from the silver layer. It outputs
    a summary CSV file with the G coefficients and detailed JSON files
    containing the raw rank arrays used for each calculation.

    Returns:
        pd.DataFrame: A DataFrame with the schedule balance analysis results for
            each team in each valid season. Returns an empty DataFrame if a
            critical error occurs, such as a missing input file.
    """
    logger.info("Starting Strength of Schedule Balance calculation.")
    try:
        games_df = pd.read_csv(paths.GAMES_VALID_PATH)
        standings_df = pd.read_csv(paths.STANDINGS_VALID_PATH)
        logger.info("Successfully loaded validated games and standings files.")
    except FileNotFoundError as e:
        logger.error("Input file not found: %s. Aborting analysis.", e, exc_info=True)
        return pd.DataFrame()

    results: List[Dict[str, Any]] = []
    all_json_data: List[Dict[str, Any]] = []

    for (league, season), season_standings in standings_df.groupby(
        ["league_name", "season_year"]
    ):
        logger.info("Processing league: %s, Season: %d", league, season)
        season_games = games_df[
            (games_df["league_name"] == league) & (games_df["season_year"] == season)
        ]
        if season_games.empty:
            logger.warning("No game data for %s %d. Skipping.", league, season)
            continue

        position_map = season_standings.set_index("team_canonical")[
            "position"
        ].to_dict()

        all_positions = sorted(
            [int(p) for p in season_standings["position"].dropna().unique()]
        )
        season_json_data = []

        for _, team_row in season_standings.iterrows():
            team_canonical = team_row["team_canonical"]
            final_pos = team_row["position"]

            if pd.isna(final_pos):
                logger.warning(
                    "Skipping %s in %s %d due to missing final position.",
                    team_canonical,
                    league,
                    season,
                )
                continue

            final_pos = int(final_pos)

            r_list = [p for p in all_positions if p != final_pos]

            opponents_canonical = _get_first_round_opponents(
                team_canonical, season_games
            )
            s_list = [
                int(pos)
                for opp in opponents_canonical
                if (pos := position_map.get(opp)) is not None
            ]

            g, g_type = _calculate_g_coefficient(r_list, s_list, team_canonical)

            if g is not None:
                results.append(
                    {
                        "standings_id": team_row["source_id"],
                        "league_name": league,
                        "season_year": int(season),
                        "team_canonical": team_canonical,
                        "final_position": final_pos,
                        "R_array": r_list,
                        "S_array": s_list,
                        "G": g,
                        "G_rounded": round(g, 4),
                        "G_type": g_type,
                    }
                )
                season_json_data.append(
                    {
                        "league_name": league,
                        "season_year": int(season),
                        "canonical_name": team_canonical,
                        "R_list": r_list,
                        "S_list_names": opponents_canonical,
                        "S_list_classif": s_list,
                    }
                )
            else:
                logger.warning(
                    "Skipping G-coeff for %s (%s %d). R_len=%d, S_len=%d.",
                    team_canonical,
                    league,
                    season,
                    len(r_list),
                    len(s_list),
                )

        if season_json_data:
            all_json_data.extend(season_json_data)
            json_file = (
                paths.SCHEDULES_DATA_INDIVIDUAL_DIR
                / f"schedule_data_{league}_{season}.json"
            )
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(
                    season_json_data, f, indent=2, cls=NumpyEncoder, ensure_ascii=False
                )

    if not results:
        logger.warning("No results were generated across all seasons.")
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    result_df.to_csv(paths.SPEARMAN_BALANCE_PATH, index=False)
    logger.info("Saved schedule balance results to %s.", paths.SPEARMAN_BALANCE_PATH)

    with open(paths.SCHEDULES_DATA_COMBINED_PATH, "w", encoding="utf-8") as f:
        json.dump(all_json_data, f, indent=2, cls=NumpyEncoder, ensure_ascii=False)
    logger.info(
        "Saved combined schedule JSON data to %s.", paths.SCHEDULES_DATA_COMBINED_PATH
    )

    return result_df
