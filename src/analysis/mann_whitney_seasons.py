# File: src/analysis/statistical_analyzer.py
"""
Performs statistical significance tests on the schedule balance groups,
at three levels: overall, per-league, and per-season.
"""
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from itertools import combinations
from pathlib import Path
from scipy.stats import mannwhitneyu
from src.utils import paths

logger = logging.getLogger(__name__)

# Mapeamento Global de G_type para os nomes dos grupos
GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]


def _generate_boxplot(df: pd.DataFrame, output_path: Path, title: str):
    """Gera e salva um boxplot customizado da distribuição de classificações."""
    if df.empty or df["group_name"].nunique() < 1:
        logger.warning(
            f"A geração do boxplot para '{title}' foi ignorada devido à falta de dados."
        )
        return

    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    sns.boxplot(
        x="group_name",
        y="final_position",
        data=df,
        order=GROUP_ORDER,
        palette="viridis",
        hue="group_name",
        legend=False,
    )

    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Group", fontsize=12)
    plt.ylabel("Team Final Rank", fontsize=12)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path)
    logger.info(f"Boxplot salvo em: {output_path.name}")
    plt.close()


def _run_tests_on_subset(data: pd.DataFrame) -> pd.DataFrame | None:
    """Executa testes Mann-Whitney U em um subconjunto de dados."""
    groups = {
        name: group_data["final_position"]
        for name, group_data in data.groupby("group_name")
    }

    if len(groups) < 2:
        logger.debug(
            f"Testes ignorados; encontrados {len(groups)} grupos, são necessários pelo menos 2 para comparação."
        )
        return None

    p_values = {}
    for g1, g2 in combinations(GROUP_ORDER, 2):
        if g1 in groups and g2 in groups:
            _, p_value = mannwhitneyu(groups[g1], groups[g2], alternative="two-sided")
            p_values[f"{g1}_vs_{g2}"] = p_value

    return pd.DataFrame([p_values])


def run_statistical_analysis():
    """
    Orquestra a análise estatística geral, por liga e por época.
    """
    logger.info("Iniciando análise de significância estatística (Mann-Whitney U).")

    try:
        input_file = paths.SPEARMAN_COEFFICIENT / "strength_schedule_balance.csv"
        df = pd.read_csv(input_file)
        df["group_name"] = df["G_type"].map(GROUP_MAP)
        df = df.dropna(subset=["final_position", "group_name"])
        logger.info(
            f"Arquivo '{input_file.name}' carregado com {len(df)} registros válidos."
        )
    except FileNotFoundError:
        logger.error(
            f"Arquivo de entrada não encontrado: {input_file}. Análise abortada."
        )
        return

    # --- NÍVEL 1: Análise Geral (todos os dados) ---
    logger.info("--- Executando Análise Estatística Geral ---")
    overall_p_values = _run_tests_on_subset(df)
    if overall_p_values is not None:
        p_values_path = (
            paths.STATISTICAL_TESTS_DIR / "overall_mann_whitney_p_values.csv"
        )
        overall_p_values.to_csv(p_values_path, index=False, float_format="%.4f")
        logger.info(f"P-values gerais salvos em: {p_values_path.name}")
        logger.info(f"Resultados Gerais:\n{overall_p_values.to_string(index=False)}\n")

    boxplot_path = paths.PLOTS_DIR / "overall_final_rank_distribution.png"
    _generate_boxplot(df, boxplot_path, "Overall Team Final Rank Distribution by Group")

    # --- NÍVEL 2: Análise por Liga (agregando todas as épocas) ---
    logger.info("--- Executando Análise Estatística Por Liga ---")
    per_league_results = []
    for league_name, league_df in df.groupby("league_name"):
        logger.info(f"Analisando Liga: {league_name}")

        p_values_league = _run_tests_on_subset(league_df)
        if p_values_league is not None:
            p_values_league["league_name"] = league_name
            per_league_results.append(p_values_league)

        plot_path_league = paths.PLOTS_DIR / f"rank_dist_{league_name}_all_seasons.png"
        title = f"Final Rank Distribution for {league_name.replace('_', ' ').title()} (All Seasons)"
        _generate_boxplot(league_df, plot_path_league, title)

    if per_league_results:
        combined_league_df = pd.concat(per_league_results, ignore_index=True)
        cols = ["league_name"] + [
            c for c in combined_league_df.columns if c != "league_name"
        ]
        combined_league_df = combined_league_df[cols]
        per_league_path = (
            paths.STATISTICAL_TESTS_DIR / "per_league_mann_whitney_p_values.csv"
        )
        combined_league_df.to_csv(per_league_path, index=False, float_format="%.4f")
        logger.info(
            f"Resultados de p-values por liga salvos em: {per_league_path.name}"
        )

    # --- NÍVEL 3: Análise por Época (nível mais granular) ---
    logger.info("--- Executando Análise Estatística Por Época ---")
    per_season_results = []
    for (league, season), season_df in df.groupby(["league_name", "season_year"]):
        season_key = f"{league}_{season}"
        logger.info(f"Analisando: {season_key}")

        p_values_season = _run_tests_on_subset(season_df)
        if p_values_season is not None:
            p_values_season["league_name"] = league
            p_values_season["season_year"] = season
            per_season_results.append(p_values_season)

        plot_path_season = paths.PLOTS_DIR / f"rank_dist_{season_key}.png"
        title = (
            f"Final Rank Distribution for {league.replace('_', ' ').title()} {season}"
        )
        _generate_boxplot(season_df, plot_path_season, title)

    if per_season_results:
        combined_df = pd.concat(per_season_results, ignore_index=True)
        cols = ["league_name", "season_year"] + [
            c for c in combined_df.columns if c not in ["league_name", "season_year"]
        ]
        combined_df = combined_df[cols]
        per_season_path = (
            paths.STATISTICAL_TESTS_DIR / "per_season_mann_whitney_p_values.csv"
        )
        combined_df.to_csv(per_season_path, index=False, float_format="%.4f")
        logger.info(
            f"Resultados de p-values por época salvos em: {per_season_path.name}"
        )

    logger.info("Análise estatística completa.")
