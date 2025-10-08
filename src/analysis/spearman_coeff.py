import json
import logging
import numpy as np
import pandas as pd

from fuzzywuzzy import fuzz
from scipy.stats import spearmanr
from typing import Dict, List, Any
from scipy.optimize import linear_sum_assignment

from src.utils import paths

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle NumPy and Pandas data types.

    This encoder extends the default JSONEncoder to provide serialization for
    data types commonly found in scientific computing libraries like NumPy and
    Pandas, which are not natively supported by the standard `json` module.

    It handles:
    - NumPy integers (e.g., np.int64)
    - NumPy floats (e.g., np.float64)
    - NumPy arrays (np.ndarray)
    - Pandas missing values (pd.NA, np.nan)

    Usage:
        json.dump(my_numpy_data, file, cls=NumpyEncoder)
    """

    def default(self, obj: Any) -> Any:
        """
        Convert non-standard types to JSON-serializable formats.

        This method is called by the JSONEncoder for any object that it
        doesn't know how to serialize. For all other types, it defers to
        the parent class's default implementation.

        Args:
            obj (Any): The object to be encoded.

        Returns:
            Any: A JSON-serializable representation of the object.
        """
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        return super().default(obj)


def _sanitize_for_json(data: Any) -> Any:
    """
    Recursively sanitizes a data structure to ensure it is JSON serializable.

    This function traverses nested dictionaries and lists, converting specific
    NumPy and Pandas data types into their native Python equivalents that are
    compatible with the standard `json` library.

    Key conversions performed:
    - NumPy integers (e.g., np.int64) are converted to standard `int`.
    - NumPy floats (e.g., np.float64) are converted to standard `float`.
    - Pandas NA values (pd.NA) and NumPy NaN values (np.nan) are
      converted to `None`, which serializes to JSON `null`.

    Args:
        data (Any): The input data structure (e.g., dict, list, scalar)
                    to be sanitized.

    Returns:
        Any: A new data structure containing only JSON-serializable types.
    """
    if isinstance(data, dict):
        return {key: _sanitize_for_json(value) for key, value in data.items()}

    elif isinstance(data, list):
        return [_sanitize_for_json(item) for item in data]

    elif isinstance(data, np.integer):
        return int(data)

    elif isinstance(data, np.floating):
        if np.isnan(data):
            return None
        return float(data)

    elif pd.isna(data):
        return None

    return data


def _validate_outputs_paths():
    """
    Ensures that all required output directories exist.

    This function checks for the presence of several directories defined in the
    'paths' module. If any of these directories do not exist, it creates them
    recursively. This is a setup utility to prevent errors when other parts
    of the application attempt to write output files.
    """
    logger.info("Validating and creating output directories if necessary.")

    # Create directories for name mappings data
    paths.NAME_MAPPINGS_COMBINED.mkdir(parents=True, exist_ok=True)
    paths.NAME_MAPPINGS_INDIVIDUAL.mkdir(parents=True, exist_ok=True)

    # Create directories for schedules data
    paths.SCHEDULES_DATA_COMBINED.mkdir(parents=True, exist_ok=True)
    paths.SCHEDULES_DATA_INDIVIDUAL.mkdir(parents=True, exist_ok=True)

    # Create directory for analysis results
    paths.SPEARMAN_COEFFICIENT.mkdir(parents=True, exist_ok=True)

    logger.info("All output directories are available.")


def _create_optimal_name_mapping(
    league: str,
    season: int,
    canonical_names: List[str],
    other_names: List[str],
) -> pd.DataFrame:
    """
    Creates a guaranteed, optimal, one-to-one mapping between canonical and other
    names by solving the linear assignment problem.

    This function ensures that every canonical name is matched with a unique
    'other name' in a way that maximizes the total similarity score across all pairs.

    Args:
        league: The name of the league.
        season: The season year.
        canonical_names: A list of standard names. Each must have a match.
        other_names: A list of names to be mapped. Must have at least as many
                     elements as canonical_names.

    Returns:
        A pandas DataFrame containing the optimal one-to-one mappings.

    Raises:
        ValueError: If there are more canonical names than other names, making
                    a unique assignment for each canonical name impossible.
    """
    num_canonical = len(canonical_names)
    num_other = len(other_names)

    if num_canonical > num_other:
        raise ValueError(
            f"Cannot create a unique mapping for every canonical name. "
            f"Found {num_canonical} canonical names but only {num_other} other names."
        )

    # Step 1: Create a score matrix (profit matrix)
    # Rows: canonical_names, Columns: other_names
    score_matrix = np.zeros((num_canonical, num_other))
    for i, canonical in enumerate(canonical_names):
        for j, other in enumerate(other_names):
            score_matrix[i, j] = fuzz.ratio(canonical, other)

    # Step 2: Solve the assignment problem.
    # The function linear_sum_assignment finds the minimum cost, so we use the
    # negative of our score matrix to maximize the total score.
    row_indices, col_indices = linear_sum_assignment(-score_matrix)

    # Step 3: Build the final mapping from the optimal assignments.
    final_mapping: List[Dict[str, Any]] = []
    for r, c in zip(row_indices, col_indices):
        canonical_name = canonical_names[r]
        other_name = other_names[c]
        score = score_matrix[r, c]

        final_mapping.append(
            {
                "league_name": league,
                "season_year": int(season),
                "canonical_name": canonical_name,
                "other_name": other_name,
                "precision_score": int(score),
                "is_score_greater_70": int(score) >= 70,
            }
        )

    return pd.DataFrame(final_mapping)


def _get_team_schedule(
    team_canonical: str, season_games: pd.DataFrame, name_mapping: pd.DataFrame
) -> List[str]:
    """
    Retrieves the chronologically ordered list of unique opponents for a team.

    This function processes a DataFrame of season games to determine the schedule
    of the first round-robin ("primeiro turno"). It identifies all unique
    opponents a team faced during the season and returns them ordered by the
    date of their first encounter.

    Args:
        team_canonical (str): The official, standardized name of the team to analyze.
        season_games (pd.DataFrame): A DataFrame containing all games for the season.
            Expected columns: 'home_team_sanitized', 'away_team_sanitized', and a
            date/datetime column named 'datetime'.
        name_mapping (pd.DataFrame): A DataFrame used for standardizing team names.
            Expected columns: 'other_name', 'canonical_name'.

    Returns:
        List[str]: A list of unique canonical opponent names, sorted by the date
                   of their first encounter with the team.
    """
    # Create a copy to avoid modifying the original DataFrame.
    season_games_mapped = season_games.copy()

    # --- 1. Standardize Team Names (No changes here) ---
    name_map_dict = dict(
        zip(name_mapping["other_name"], name_mapping["canonical_name"])
    )
    season_games_mapped["away_team_canonical"] = season_games_mapped[
        "away_team_sanitized"
    ].map(name_map_dict)
    season_games_mapped["away_team_canonical"] = season_games_mapped[
        "away_team_canonical"
    ].fillna(season_games_mapped["away_team_sanitized"])

    # --- 2. Filter for All of the Team's Games ---
    # Select all games where the team was either home or away.
    team_games = season_games_mapped[
        (season_games_mapped["home_team_sanitized"] == team_canonical)
        | (season_games_mapped["away_team_canonical"] == team_canonical)
    ].copy()

    # --- 3. Sort Games Chronologically by Datetime ---
    # Ensure the 'datetime' column is in the correct format and sort by it.
    # This is the new primary sorting method.
    team_games["datetime"] = pd.to_datetime(team_games["datetime"])
    team_games = team_games.sort_values(by="datetime", ascending=True)

    # --- 4. Extract Unique Opponents while Preserving Order ---
    # First, get a list of all opponents in chronological order (with duplicates).
    opponents_in_order = []
    for _, row in team_games.iterrows():
        if row["home_team_sanitized"] == team_canonical:
            opponent = row["away_team_canonical"]
        else:
            opponent = row["home_team_sanitized"]

        # A safety check to ensure we don't add the team itself to the list.
        if opponent != team_canonical:
            opponents_in_order.append(opponent)

    # Now, create a unique list of opponents that preserves the order of the first encounter.
    # The dict.fromkeys() method is a highly efficient way to do this in Python 3.7+.
    unique_opponents = list(dict.fromkeys(opponents_in_order))

    return unique_opponents


def calculate_strength_schedule_balance() -> pd.DataFrame:
    """
    Calculates and analyzes the strength of schedule balance for soccer leagues.

    This function quantifies how balanced a team's schedule was during the first
    half of a season using the Spearman's rank correlation coefficient, denoted
    as 'G'. The coefficient measures the relationship between a team's final
    rank and the final ranks of its opponents from the first 19 rounds.

    The interpretation of the 'G' coefficient is as follows:
    - G > 0.3 (unbalanced_strong): The team tended to play more games against teams
      that finished in lower positions (an easier schedule).
    - G < -0.3 (unbalanced_weak): The team tended to play more games against teams
      that finished in higher positions (a harder schedule).
    - -0.3 <= G <= 0.3 (balanced): The schedule was relatively balanced, with no
      significant correlation between opponent strength and game order.

    The process involves several key steps:
    1.  Loading mid-season game data and final season standings.
    2.  Iterating through each unique league and season.
    3.  Creating an optimal name mapping to reconcile team names between different
        data sources for that season.
    4.  For each team, constructing two arrays: 'R' (the rank of all possible
        opponents) and 'S' (the actual ranks of their opponents).
    5.  Calculating the 'G' coefficient by applying Spearman's correlation to the
        'R' and 'S' arrays.
    6.  Saving intermediate artifacts (individual name mappings, JSON schedule data)
        and final combined outputs, including a CSV with the G-coefficients for all teams.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated schedule balance
                      for each team, or an empty DataFrame if a critical error occurs.
    """

    _validate_outputs_paths()

    logger.info("Starting Strength of Schedule Balance calculation process.")
    logger.info("Loading input data: mid-season games and final standings.")

    try:
        team_games_df = pd.read_csv(paths.TEAM_GAMES_VALID)
        final_standings_df = pd.read_csv(paths.FINAL_STANDINGS_VALID)
        logger.info("Successfully loaded all required input files.")
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {e}. Cannot proceed.", exc_info=True)
        return pd.DataFrame()

    # ---------------------------------------------------------------------

    # Process each league and season combination separately.

    results = []
    all_mappings = []
    all_json_data = []

    for (league, season), season_standings in final_standings_df.groupby(
        ["league_name", "season_year"]
    ):

        logger.info(f"Processing league: {league}, Season: {season}")

        season_games = team_games_df[
            (team_games_df["league_name"] == league)
            & (team_games_df["season_year"] == season)
        ].copy()

        if season_games.empty:
            logger.warning(f"No game data found for {league} {season}. Skipping.")
            continue

        # ---------------------------------------------------------------------

        # Create and save an optimal name mapping for the current season.

        away_names = sorted(season_games["away_team_sanitized"].unique())
        canonical_names = sorted(season_games["home_team_sanitized"].unique())

        mapping_df = _create_optimal_name_mapping(
            league, season, canonical_names, away_names
        )

        mapping_file = (
            paths.NAME_MAPPINGS_INDIVIDUAL / f"name_mapping_{league}_{season}.csv"
        )
        mapping_df.to_csv(mapping_file, index=False)
        all_mappings.append(mapping_df)

        logger.debug(f"Individual name mapping saved to {mapping_file}")

        # ---------------------------------------------------------------------

        position_map = season_standings.set_index("team_sanitized")[
            "position"
        ].to_dict()

        all_positions = sorted(
            [int(p) for p in season_standings["position"].dropna().unique()]
        )

        season_json = []

        # Iterate through each team in the season to calculate its G coefficient.

        for _, team_row in season_standings.iterrows():

            team_canonical = team_row["team_sanitized"]
            final_pos = team_row["position"]

            if pd.isna(final_pos):
                logger.warning(
                    f"Skipping {team_canonical} in {league} {season} due to missing final position."
                )
                continue

            final_pos = int(final_pos)

            # Create R LIST

            r_list = [p for p in all_positions if p != final_pos]

            # Create S LIST

            opponents_canonical = _get_team_schedule(
                team_canonical, season_games, mapping_df
            )

            s_list_names = opponents_canonical

            s_list_classif = [
                int(pos)
                for opp in opponents_canonical
                if (pos := position_map.get(opp)) is not None
                and not pd.isna(pos)
                and int(pos) != final_pos
            ]

            # Calculate Spearman's correlation

            if len(r_list) == len(s_list_classif) and len(s_list_classif) > 1:

                g, _ = spearmanr(r_list, s_list_classif)
                g = float(g) if not pd.isna(g) else None

                if g is None:
                    logger.warning(
                        f"Spearman correlation returned NaN for {team_canonical}."
                    )
                    continue

                team_json = {
                    "league_name": league,
                    "season_year": int(season),
                    "canonical_name": team_canonical,
                    "R_list": r_list,
                    "S_list_names": s_list_names,
                    "S_list_classif": s_list_classif,
                }

                season_json.append(_sanitize_for_json(team_json))

                # Classify coefficient

                if g > 0.3:
                    g_type = "unbalanced_strong"
                elif g < -0.3:
                    g_type = "unbalanced_weak"
                else:
                    g_type = "balanced"

                logger.debug(f"Calculated G={g:.2f} ({g_type}) for {team_canonical}.")

                results.append(
                    {
                        "standings_id": team_row["source_id"],
                        "league_name": league,
                        "season_year": int(season),
                        "team_sanitized": team_canonical,
                        "final_position": final_pos,
                        "R_array": r_list,
                        "S_array": s_list_classif,
                        "G": g,
                        "G_rounded": round(g, 4),
                        "G_type": g_type,
                    }
                )

            else:
                logger.warning(
                    f"Skipping G-coeff for {team_canonical} ({league} {season}). "
                    f"Reason: Mismatched or insufficient data. R_len={len(r_list)}, S_len={len(s_list_classif)}."
                )

        # ---------------------------------------------------------------------

        all_json_data.extend(season_json)

        json_file = (
            paths.SCHEDULES_DATA_INDIVIDUAL / f"schedule_data_{league}_{season}.json"
        )

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(season_json, f, indent=2, cls=NumpyEncoder, ensure_ascii=False)

        logger.debug("Individual season schedule JSON saved to %s", json_file)

        # ---------------------------------------------------------------------

    logger.info(
        "Finished processing all leagues and seasons. Now saving combined files."
    )

    if all_mappings:

        combined_mapping = pd.concat(all_mappings, ignore_index=True)
        mapping_out = paths.NAME_MAPPINGS_COMBINED / "name_mappings_combined.csv"
        combined_mapping.to_csv(mapping_out, index=False)
        logger.info("Successfully saved combined name mappings to %s", mapping_out)

    # ---------------------------------------------------------------------

    sanitized_json_data = _sanitize_for_json(all_json_data)
    json_out = paths.SCHEDULES_DATA_COMBINED / "schedule_data_combined.json"

    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(
            sanitized_json_data, f, indent=2, cls=NumpyEncoder, ensure_ascii=False
        )

    logger.info("Successfully saved combined schedule JSON data to %s", json_out)

    # ---------------------------------------------------------------------

    if results:

        result_df = pd.DataFrame(results)
        final_out = paths.SPEARMAN_COEFFICIENT / "strength_schedule_balance.csv"
        result_df.to_csv(final_out, index=False)

        logger.info(
            "Successfully saved final schedule balance results to %s", final_out
        )
        logger.info(
            f"Strength of Schedule Balance calculation finished. Generated {len(result_df)} results."
        )

        return result_df

    else:
        logger.warning("No results were generated. Process finished with no output.")
        return pd.DataFrame()
