import json
import logging
import numpy as np
import pandas as pd

from src.utils import paths
from scipy.stats import spearmanr
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

SIGNIFICANCE_LEVEL = 0.05
CORRELATION_THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4]


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
    """Classifica um coeficiente G baseado em seu valor, p-value e limiares.

    Args:
        g: O coeficiente de correlação de Spearman.
        p_value: O p-value associado ao coeficiente g.
        correlation_threshold: O valor 'g' mínimo (absoluto) para
            classificar como 'unbalanced'.
        significance_level: O p-value (alfa) máximo para que a correlação
            seja considerada estatisticamente significativa.

    Returns:
        A string de classificação: 'unbalanced_strong', 'unbalanced_weak',
        ou 'balanced'.
    """
    is_significant = p_value < significance_level

    # if not is_significant:
    #     return "balanced"

    if g > correlation_threshold:
        return "unbalanced_strong"
    elif g < -correlation_threshold:
        return "unbalanced_weak"
    else:
        return "balanced"


def calculate_strength_schedule_balance() -> pd.DataFrame:
    """Calculates schedule balance for all teams using Spearman's G coefficient.

    (Docstring original omitida para brevidade)
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

        all_positions = sorted([int(p) for p in season_standings["position"]])

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
            r_list = all_positions.copy()

            try:
                r_list.remove(final_pos)
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

            # 1. Validação de tamanho ANTES de calcular
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

            # 2. Calcular G e P-value UMA VEZ
            g, p_value = spearmanr(r_list, s_list)
            g = float(g) if not pd.isna(g) else None

            # 3. Lidar com cálculo inválido (NaN)
            if g is None or pd.isna(p_value):
                logger.warning(
                    "Spearman correlation returned NaN for %s (%s %d).",
                    team_canonical,
                    league,
                    season,
                )
                continue

            # 4. Calcular significância (com base no seu requisito)
            is_significant = p_value <= SIGNIFICANCE_LEVEL

            # 5. Criar o dicionário de resultados base
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

            # 6. Calcular G_type para cada threshold e adicionar ao dicionário
            for thresh in CORRELATION_THRESHOLDS:
                g_type = _classify_g_type(g, p_value, thresh, SIGNIFICANCE_LEVEL)
                key_name = f"G_type_{thresh:.3f}"
                result_row[key_name] = g_type

            # 7. Adicionar o dicionário completo aos resultados
            results.append(result_row)

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

        if season_json_data:
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
