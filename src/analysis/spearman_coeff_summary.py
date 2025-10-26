import logging
import pandas as pd
from src.utils import paths
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def _calculate_group_stats(group_df: pd.DataFrame, g_type_cols: List[str]) -> pd.Series:
    """
    Função auxiliar para calcular estatísticas de sumarização para um
    determinado grupo de dados.
    """
    if group_df.empty:
        return pd.Series(dtype="float64")

    stats: Dict[str, Any] = {}

    stats["total_tests"] = len(group_df)
    stats["significant_tests"] = group_df["is_significant"].sum()

    for col in g_type_cols:
        counts = group_df[col].value_counts()

        stats[f"{col}_balanced"] = counts.get("balanced", 0)
        stats[f"{col}_unbalanced_strong"] = counts.get("unbalanced_strong", 0)
        stats[f"{col}_unbalanced_weak"] = counts.get("unbalanced_weak", 0)

    return pd.Series(stats)


def create_g_type_summary():
    """
    Agrega os resultados do G-coefficient (em múltiplos limiares)
    em uma tabela de sumarização.

    (Docstring original omitida para brevidade)
    """
    logger.info("Starting creation of G-type summary.")
    try:
        df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        logger.info("Loaded '%s' with %d rows.", paths.SPEARMAN_BALANCE_PATH, len(df))
    except FileNotFoundError:
        logger.error(
            "Input file not found: %s. Cannot create summary.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    if df.empty:
        logger.warning(
            "Input file '%s' is empty. No summary will be created.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    g_type_cols = sorted([col for col in df.columns if col.startswith("G_type_")])

    if not g_type_cols:
        logger.error(
            "No 'G_type_' columns found in '%s'. Aborting summary.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    logger.info(
        "Found %d G-type columns to summarize: %s", len(g_type_cols), g_type_cols
    )

    # Nível 3: Agrupado por liga e temporada
    agg_season = (
        df.groupby(["league_name", "season_year"])
        .apply(
            _calculate_group_stats, g_type_cols=g_type_cols, include_groups=False
        )  # <-- CORREÇÃO 1
        .reset_index()
    )

    # Nível 2: Agrupado por liga (todas as temporadas)
    agg_league = (
        df.groupby("league_name")
        .apply(
            _calculate_group_stats, g_type_cols=g_type_cols, include_groups=False
        )  # <-- CORREÇÃO 2
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"

    # Nível 1: Geral (todas as ligas, todas as temporadas)
    agg_all_series = _calculate_group_stats(df, g_type_cols)
    agg_all = agg_all_series.to_frame().T
    agg_all["league_name"] = "all_leagues"
    agg_all["season_year"] = "all_seasons"

    # Garante que os tipos de dados numéricos sejam mantidos após a transposição
    for col in agg_all.columns:
        if col not in ["league_name", "season_year"]:
            # --- CORREÇÃO 3 ---
            try:
                agg_all[col] = pd.to_numeric(agg_all[col])
            except ValueError:
                # Imita 'errors="ignore"': se não for numérico, mantém o original.
                pass
            # --- FIM DA CORREÇÃO ---

    # Combinar tudo
    summary_df = pd.concat([agg_season, agg_league, agg_all], ignore_index=True)

    # Reordenar colunas
    id_cols = ["league_name", "season_year", "total_tests", "significant_tests"]

    metric_cols = []
    for col_name in g_type_cols:
        metric_cols.extend(
            [
                f"{col_name}_balanced",
                f"{col_name}_unbalanced_strong",
                f"{col_name}_unbalanced_weak",
            ]
        )

    final_columns = id_cols + metric_cols
    summary_df = summary_df.reindex(columns=final_columns)

    # Ordenar e salvar
    summary_df.sort_values(by=["league_name", "season_year"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)

    int_cols = [
        col for col in summary_df.columns if col not in ["league_name", "season_year"]
    ]
    summary_df[int_cols] = summary_df[int_cols].astype(int)

    summary_df.to_csv(paths.SPEARMAN_SUMMARY_PATH, index=False)
    logger.info(
        "Summary table with %d rows saved to '%s'.",
        len(summary_df),
        paths.SPEARMAN_SUMMARY_PATH,
    )
