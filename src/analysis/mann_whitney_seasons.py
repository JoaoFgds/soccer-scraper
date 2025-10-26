import logging
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from src.utils import paths
from itertools import combinations
from scipy.stats import mannwhitneyu

# Importação condicional para type hinting
if TYPE_CHECKING:
    from pandas import DataFrame

logger = logging.getLogger(__name__)


# Mapeamento e ordem dos grupos

GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]

# Pares de comparação para o teste

COMPARISON_PAIRS = [("G-", "G0"), ("G-", "G+"), ("G0", "G+")]
P_VALUE_COLS = [f"{g1}_vs_{g2}" for g1, g2 in COMPARISON_PAIRS]


# Constante para focar a análise em um único limiar
G_TYPE_COLUMN_TO_ANALYZE = "G_type_0.300"


def _run_tests_on_subset(data: pd.DataFrame) -> Optional["DataFrame"]:
    """Executa testes Mann-Whitney U pareados em um subconjunto de dados."""
    groups = {
        name: group_data["final_position"]
        for name, group_data in data.groupby("group_name")
    }
    if len(groups) < 2:
        return None

    p_values = {}
    for g1, g2 in COMPARISON_PAIRS:
        col_name = f"{g1}_vs_{g2}"
        if g1 in groups and g2 in groups:
            if groups[g1].empty or groups[g2].empty:
                p_values[col_name] = None
            else:
                _, p_value = mannwhitneyu(
                    groups[g1], groups[g2], alternative="two-sided"
                )
                p_values[col_name] = p_value
        else:
            p_values[col_name] = None

    p_values_df = pd.DataFrame([p_values])
    return p_values_df.astype(float)


def _calculate_significance_stats(group: pd.DataFrame) -> pd.Series:
    """
    (NOVA FUNÇÃO) Calcula as estatísticas de significância para um
    determinado grupo de p-values (de um melted_df).
    """
    if group.empty:
        return pd.Series(dtype="float64")

    stats = {}
    total_tests = len(group)

    # Cria uma máscara booleana para p < 0.05
    significant_mask = group["p_value"] < 0.05
    total_significant = significant_mask.sum()

    stats["total_tests"] = total_tests
    stats["significant_tests"] = total_significant

    # Calcula a porcentagem
    if total_tests > 0:
        stats["significant_percentage"] = total_significant / total_tests
    else:
        stats["significant_percentage"] = 0.0

    # --- Breakdown por tipo de comparação ---
    stats["significant_G-_vs_G0"] = significant_mask[
        group["comparison"] == "G-_vs_G0"
    ].sum()
    stats["significant_G-_vs_G+"] = significant_mask[
        group["comparison"] == "G-_vs_G+"
    ].sum()
    stats["significant_G0_vs_G+"] = significant_mask[
        group["comparison"] == "G0_vs_G+"
    ].sum()

    return pd.Series(stats)


def _analyze_and_log_results(results_df: pd.DataFrame) -> None:
    """
    (MODIFICADO) Analisa o DataFrame de p-values, salva o novo CSV de
    contagens em 3 níveis e loga as respostas para as perguntas de negócio.

    Primeiro, filtra quaisquer linhas com p-values nulos.
    """
    logger.info(
        "--- Iniciando Análise Agregada dos P-Values (para %s) ---",
        G_TYPE_COLUMN_TO_ANALYZE,
    )

    original_row_count = len(results_df)
    # Filtra o DataFrame para manter apenas linhas onde TODOS os p-values
    # são não-nulos.
    analysis_df = results_df.dropna(subset=P_VALUE_COLS)
    dropped_rows = original_row_count - len(analysis_df)

    if dropped_rows > 0:
        logger.info(
            f"Removidas {dropped_rows} linhas (temporadas) com p-values nulos "
            f"(testes incompletos) antes da análise. "
            f"{len(analysis_df)} temporadas completas restantes."
        )

    if analysis_df.empty:
        logger.warning(
            "Nenhuma linha com conjunto completo de p-values "
            "encontrada para analisar."
        )
        return
    # --- FIM DA MODIFICAÇÃO ---

    # "Derrete" o DataFrame (agora filtrado) para ter p-values em uma única coluna
    melted_df = analysis_df.melt(
        id_vars=["league_name", "season_year"],
        value_vars=P_VALUE_COLS,
        var_name="comparison",
        value_name="p_value",
    )
    # Nenhuma dropna() é necessária aqui, pois já foi feito.

    # --- 1. Calcular agregações nos 3 níveis ---

    # Nível 3: (league, season)
    agg_season = (
        melted_df.groupby(["league_name", "season_year"])
        .apply(_calculate_significance_stats, include_groups=False)
        .reset_index()
    )

    # Nível 2: (league)
    agg_league = (
        melted_df.groupby("league_name")
        .apply(_calculate_significance_stats, include_groups=False)
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"

    # Nível 1: (overall)
    agg_all_series = _calculate_significance_stats(melted_df)
    agg_all = agg_all_series.to_frame().T
    agg_all["league_name"] = "all_leagues"
    agg_all["season_year"] = "all_seasons"

    # Corrige tipos de dados após a transposição
    for col in agg_all.columns:
        if col not in ["league_name", "season_year"]:
            try:
                agg_all[col] = pd.to_numeric(agg_all[col])
            except ValueError:
                pass

    # --- 2. Combinar e Salvar o NOVO CSV de contagens ---

    summary_df = pd.concat([agg_season, agg_league, agg_all], ignore_index=True)

    id_cols = ["league_name", "season_year"]
    metric_cols = [
        "total_tests",
        "significant_tests",
        "significant_percentage",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]

    summary_df = summary_df.reindex(columns=id_cols + metric_cols)
    summary_df.sort_values(by=["league_name", "season_year"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)

    int_cols = [
        "total_tests",
        "significant_tests",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df[int_cols] = summary_df[int_cols].fillna(0).astype(int)

    summary_df.to_csv(paths.OVERALL_MANN_WHITNEY_PATH, index=False, float_format="%.4f")
    logger.info(
        "CSV de sumarização de significância salvo em: %s",
        paths.OVERALL_MANN_WHITNEY_PATH,
    )

    # --- 3. Logar as respostas (usando o summary_df recém-criado) ---

    overall_stats = summary_df[
        (summary_df["league_name"] == "all_leagues")
        & (summary_df["season_year"] == "all_seasons")
    ]

    if not overall_stats.empty:
        stats = overall_stats.iloc[0]
        logger.info(
            f"[Análise Geral (Limiar {G_TYPE_COLUMN_TO_ANALYZE})] "
            f"Percentual de testes significativos (p < 0.05): "
            f"{stats['significant_percentage'] * 100:.2f}% "
            f"({stats['significant_tests']} de {stats['total_tests']} testes)"
        )

    league_summary = (
        summary_df[
            (summary_df["season_year"] == "all_seasons")
            & (summary_df["league_name"] != "all_leagues")
        ]
        .set_index("league_name")["significant_percentage"]
        .sort_values(ascending=False)
    )

    if not league_summary.empty:
        logger.info(
            f"[Análise por Liga (Limiar {G_TYPE_COLUMN_TO_ANALYZE})] "
            f"Liga com MAIOR efeito (mais testes significativos): "
            f"{league_summary.index[0]} ({league_summary.iloc[0] * 100:.2f}%)"
        )
        logger.info(
            f"[Análise por Liga (Limiar {G_TYPE_COLUMN_TO_ANALYZE})] "
            f"Liga com MENOR efeito (menos testes significativos): "
            f"{league_summary.index[-1]} ({league_summary.iloc[-1] * 100:.2f}%)"
        )

        league_summary_log = (league_summary * 100).to_string(float_format="%.2f%%")
        logger.info(
            "Sumário completo da taxa de significância por liga (para %s):\n%s",
            G_TYPE_COLUMN_TO_ANALYZE,
            league_summary_log,
        )

    logger.info("--- Análise Agregada Concluída ---")


def run_statistical_analysis() -> None:
    """
    Orquestra a análise estatística do balanço da tabela nos ranks finais
    focando em um único limiar (G_type_0.300).
    """
    logger.info("Iniciando análise de significância estatística (Mann-Whitney U).")

    try:
        df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        logger.info(
            "Carregado '%s' com %d linhas.", paths.SPEARMAN_BALANCE_PATH, len(df)
        )
    except FileNotFoundError:
        logger.error(
            "Arquivo de entrada não encontrado: %s. Abortando análise.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    if G_TYPE_COLUMN_TO_ANALYZE not in df.columns:
        logger.error(
            "Coluna de análise '%s' não encontrada em '%s'. Abortando.",
            G_TYPE_COLUMN_TO_ANALYZE,
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    all_season_results: List["DataFrame"] = []

    logger.info("--- Processando limiar único: %s ---", G_TYPE_COLUMN_TO_ANALYZE)

    # 1. Prepara o DataFrame para este limiar
    df_thresh = df[
        ["league_name", "season_year", "final_position", G_TYPE_COLUMN_TO_ANALYZE]
    ].copy()
    df_thresh["group_name"] = df_thresh[G_TYPE_COLUMN_TO_ANALYZE].map(GROUP_MAP)
    df_thresh.dropna(subset=["final_position", "group_name"], inplace=True)

    if df_thresh.empty:
        logger.warning(
            "Sem dados válidos para '%s'. Encerrando análise.", G_TYPE_COLUMN_TO_ANALYZE
        )
        return

    # 2. Executa testes na granularidade (liga, temporada)
    for (league, season), season_df in df_thresh.groupby(
        ["league_name", "season_year"]
    ):
        if (p_season := _run_tests_on_subset(season_df)) is not None:
            p_season["league_name"] = league
            p_season["season_year"] = season
            all_season_results.append(p_season)

    if not all_season_results:
        logger.warning(
            "Análise concluída, mas nenhum resultado estatístico foi gerado."
        )
        return

    # Correção do Erro: Use pd.concat para unir uma lista de DataFrames
    results_df = pd.concat(all_season_results, ignore_index=True)

    # Reordena colunas para clareza
    id_cols = ["league_name", "season_year"]
    results_df = results_df.reindex(columns=id_cols + P_VALUE_COLS)

    # Salva o CSV de P-VALUES por temporada
    results_df.to_csv(
        paths.PER_SEASON_MANN_WHITNEY_PATH, index=False, float_format="%.4f"
    )
    logger.info(
        "Resultados (p-values) para '%s' salvos em: %s",
        G_TYPE_COLUMN_TO_ANALYZE,
        paths.PER_SEASON_MANN_WHITNEY_PATH,
    )

    # Executa a análise, salva o CSV de contagens e loga os resultados
    _analyze_and_log_results(results_df)

    logger.info("Análise estatística completa.")


if __name__ == "__main__":
    run_statistical_analysis()
