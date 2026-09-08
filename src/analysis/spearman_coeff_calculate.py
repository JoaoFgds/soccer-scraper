import json
import logging
import numpy as np
import pandas as pd

from src.utils import paths
from scipy.stats import spearmanr
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

SIGNIFICANCE_LEVEL = 0.05
CORRELATION_THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4]


class NumpyEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle NumPy and pandas data types.

    This class extends the default JSON encoder to provide serialization for
    common data types used in NumPy and pandas, such as integers, floats,
    arrays, and missing values (pd.NA), which are not natively supported by
    the standard 'json' library.
    """

    def default(self, obj: Any) -> Any:
        """
        Serializes NumPy/pandas types into native Python types.

        This method is called by the JSON encoder for any object that is not
        a primitive type. It checks if the object is a NumPy integer, float,
        or array, or a pandas NA value, and converts it to a
        JSON-serializable format (int, float, list, or None).

        Args:
            obj: The object to serialize.

        Returns:
            The JSON-serializable representation of the object.
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
    """
    Retrieves the ordered list of unique opponents a team faced in the first round.

    The function identifies all of a team's matches within a given season, sorts
    them chronologically by datetime, and extracts the opponent from each match.
    It then returns a list of unique opponent names, preserving the order of their
    first encounter. This effectively represents the team's schedule for the
    first half of a double round-robin season.

    Args:
        team_canonical: The canonical name of the team whose schedule
            is being analyzed.
        season_games_df: A DataFrame containing all games for the
            specific league and season.

    Returns:
        An ordered list of unique opponent canonical names, based on
        the sequence of their first encounter with the specified team.
    """
    team_games = season_games_df[
        (season_games_df["home_team_canonical"] == team_canonical)
        | (season_games_df["away_team_canonical"] == team_canonical)
    ].copy()

    team_games["datetime"] = pd.to_datetime(
        team_games["datetime"], errors="coerce", format="%Y-%m-%d %H:%M:%S"
    )

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


def _classify_g_type(
    g: float,
    p_value: float,
    correlation_threshold: float,
    significance_level: float,
) -> str:
    """
    Classifies a G-coefficient based on its value, p-value, and thresholds.

    Note: The p-value and significance_level are passed but currently
    commented out in the logic, per the original code structure. The
    classification is based purely on the G-value magnitude relative
    to the correlation_threshold.

    Args:
        g: The Spearman's rank correlation coefficient.
        p_value: The p-value associated with the G-coefficient.
        correlation_threshold: The minimum absolute 'g' value to be
            classified as 'unbalanced'.
        significance_level: The maximum p-value (alpha) for a
            correlation to be considered statistically significant.

    Returns:
        The classification string: 'unbalanced_strong', 'unbalanced_weak',
        or 'balanced'.
    """
    is_significant = p_value < significance_level

    if g > correlation_threshold:
        return "unbalanced_strong"
    elif g < -correlation_threshold:
        return "unbalanced_weak"
    else:
        return "balanced"


def _calculate_schedule_balance(
    games_df: pd.DataFrame,
    standings_df: pd.DataFrame,
    strength_column: str,
    final_position_column: str,
    write_schedule_data: bool = False,
) -> pd.DataFrame:
    """
    Calculates schedule balance using one standings strength proxy.

    `strength_column` determines the ranks used in R_array and S_array.
    `final_position_column` remains the team's observed final position in the
    output, so a market-value analysis does not replace the outcome column.

    Returns:
        A DataFrame containing the calculated balance metrics.
    """
    results: List[Dict[str, Any]] = []

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
            strength_column
        ].to_dict()

        all_positions = sorted(
            [int(p) for p in season_standings[strength_column]]
        )

        season_json_data = []

        for _, team_row in season_standings.iterrows():
            team_canonical = team_row["team_canonical"]
            final_pos = team_row[final_position_column]
            strength_pos = team_row[strength_column]

            if pd.isna(final_pos) or pd.isna(strength_pos):
                logger.warning(
                    "Skipping %s in %s %d due to missing position data.",
                    team_canonical,
                    league,
                    season,
                )
                continue

            final_pos = int(final_pos)
            strength_pos = int(strength_pos)
            r_list = all_positions.copy()

            try:
                r_list.remove(strength_pos)
            except ValueError:
                pass

            opponents_canonical = _get_first_round_opponents(
                team_canonical, season_games
            )
            s_list = [
                int(pos)
                for opp in opponents_canonical
                if (pos := position_map.get(opp)) is not None
            ]

            if len(r_list) != len(s_list) or len(s_list) <= 1:
                logger.warning(
                    "Skipping G-coeff for %s (%s %d). R_len=%d, S_len=%d.",
                    team_canonical,
                    league,
                    season,
                    len(r_list),
                    len(s_list),
                )
                continue

            g, p_value = spearmanr(r_list, s_list)
            g = float(g) if not pd.isna(g) else None

            if g is None or pd.isna(p_value):
                logger.warning(
                    "Spearman correlation returned NaN for %s (%s %d).",
                    team_canonical,
                    league,
                    season,
                )
                continue

            is_significant = p_value <= SIGNIFICANCE_LEVEL

            result_row = {
                "standings_id": team_row["source_id"],
                "league_name": league,
                "season_year": int(season),
                "team_canonical": team_canonical,
                "final_position": final_pos,
                "R_array": r_list,
                "S_array": s_list,
                "G": g,
                "P_value": p_value,
                "is_significant": is_significant,
            }

            for thresh in CORRELATION_THRESHOLDS:
                g_type = _classify_g_type(g, p_value, thresh, SIGNIFICANCE_LEVEL)
                key_name = f"G_type_{thresh:.3f}"
                result_row[key_name] = g_type

            results.append(result_row)

            if write_schedule_data:
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

        if write_schedule_data and season_json_data:
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

    return pd.DataFrame(results)


def calculate_strength_schedule_balance() -> pd.DataFrame:
    """Generate final-ranking and market-value schedule-balance results."""
    logger.info("Starting Strength of Schedule Balance calculation.")
    try:
        games_df = pd.read_csv(paths.GAMES_VALID_PATH)
        ranking_standings_df = pd.read_csv(
                    paths.STANDINGS_VALID_RANKING_PATH
                )
        market_standings_df = pd.read_csv(
            paths.STANDINGS_VALID_MARKET_RANKING_PATH
        )
        logger.info("Successfully loaded validated games and standings files.")
    except FileNotFoundError as e:
        logger.error("Input file not found: %s. Aborting analysis.", e, exc_info=True)
        return pd.DataFrame()

    ranking_result_df = _calculate_schedule_balance(
        games_df,
        ranking_standings_df,
        strength_column="position",
        final_position_column="position",
        write_schedule_data=True,
    )
    if ranking_result_df.empty:
        return ranking_result_df

    ranking_result_df.to_csv(paths.SPEARMAN_BALANCE_RANKING_PATH, index=False)
    ranking_result_df.to_csv(paths.SPEARMAN_BALANCE_PATH, index=False)
    logger.info(
        "Saved final-ranking schedule balance results to %s.",
        paths.SPEARMAN_BALANCE_RANKING_PATH,
    )

    # computing market value rank before passing to SSB computation
    market_standings_df["market_value_rank"] = (
    market_standings_df
    .groupby(["league_name", "season_year"])["total_market_value_euros"]
    .rank(method="first", ascending=False)
    .astype("Int64")
)

    market_result_df = _calculate_schedule_balance(
        games_df,
        market_standings_df,
        strength_column="market_value_rank",
        final_position_column="position",
    )
    if not market_result_df.empty:
        market_result_df.to_csv(paths.SPEARMAN_BALANCE_MARKET_PATH, index=False)
        logger.info(
            "Saved market-value schedule balance results to %s.",
            paths.SPEARMAN_BALANCE_MARKET_PATH,
        )

    # Preserve the legacy combined artifact behavior for existing consumers.
    with open(paths.SCHEDULES_DATA_COMBINED_PATH, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2, cls=NumpyEncoder, ensure_ascii=False)
    logger.info(
        "Saved combined schedule JSON data to %s.", paths.SCHEDULES_DATA_COMBINED_PATH
    )

    return ranking_result_df
