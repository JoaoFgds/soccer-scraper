# File: src/analysis/occupancy_analyzer.py
"""
Executa uma análise de sensibilidade da relação entre o equilíbrio do calendário
e a ocupação dos estádios. Os resultados dos p-values são consolidados num
único arquivo de saída.
"""
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from itertools import combinations
from pathlib import Path
from src.utils import paths
from scipy.stats import mannwhitneyu

logger = logging.getLogger(__name__)

# Constantes para mapeamento e ordenação dos grupos
GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]


def _calculate_occupancy(games_df: pd.DataFrame, attendance_col: str) -> pd.DataFrame:
    """Calcula a ocupação média por equipa/época e salva uma tabela de auditoria."""
    logger.info(f"Calculando ocupação média usando a coluna: '{attendance_col}'")

    home_games = games_df.dropna(subset=[attendance_col])
    home_games = home_games[home_games[attendance_col] > 0]

    occupancy_stats = (
        home_games.groupby(["league_name", "season_year", "home_team_sanitized"])
        .agg(
            mean_attendance=(attendance_col, "mean"),
            max_attendance=(attendance_col, "max"),
        )
        .reset_index()
    )

    occupancy_stats = occupancy_stats[occupancy_stats["max_attendance"] > 0]
    occupancy_stats["average_occupancy"] = (
        occupancy_stats["mean_attendance"] / occupancy_stats["max_attendance"]
    )
    occupancy_stats.rename(
        columns={"home_team_sanitized": "team_sanitized"}, inplace=True
    )

    paths.ANALYSIS_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = paths.ANALYSIS_AUDIT_DIR / f"occupancy_audit_{attendance_col}.csv"
    occupancy_stats.to_csv(audit_path, index=False, float_format="%.2f")
    logger.info(f"Tabela de auditoria salva em: {audit_path.name}")

    return occupancy_stats[
        ["league_name", "season_year", "team_sanitized", "average_occupancy"]
    ]


def _generate_plot(df: pd.DataFrame, file_key: str, attendance_col: str, title: str):
    """Gera e salva um boxplot para um determinado subconjunto de dados."""
    if df.empty or df["group_name"].nunique() < 2:
        return

    plot_path = paths.PLOTS_DIR / f"occupancy_{attendance_col}_{file_key}.png"
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})
    sns.boxplot(
        x="group_name",
        y="average_occupancy",
        data=df,
        order=GROUP_ORDER,
        palette="viridis",
        hue="group_name",
        legend=False,
    )
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Group", fontsize=12)
    plt.ylabel("Average Occupancy", fontsize=12)
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()


def _run_tests(df: pd.DataFrame) -> pd.DataFrame:
    """Executa testes Mann-Whitney U e retorna uma linha de p-values."""
    groups_data = {
        name: data["average_occupancy"] for name, data in df.groupby("group_name")
    }
    p_values = {}

    # Garantir que todas as colunas de comparação existam, mesmo que o teste não possa ser executado
    for g1, g2 in combinations(GROUP_ORDER, 2):
        col_name = f"{g1}_vs_{g2}".replace("-", "neg").replace("+", "pos")
        p_values[col_name] = None

    if len(groups_data) >= 2:
        for g1, g2 in combinations(GROUP_ORDER, 2):
            if g1 in groups_data and g2 in groups_data:
                col_name = f"{g1}_vs_{g2}".replace("-", "neg").replace("+", "pos")
                _, p_value = mannwhitneyu(groups_data[g1], groups_data[g2])
                p_values[col_name] = p_value

    return pd.DataFrame([p_values])


def run_occupancy_analysis():
    """
    Ponto de entrada principal. Executa a análise de sensibilidade e consolida
    todos os resultados de p-values num único arquivo.
    """
    logger.info(
        "### INICIANDO ANÁLISE DE OCUPAÇÃO DE ESTÁDIOS (COM RESULTADO CONSOLIDADO) ###"
    )

    try:
        games_df = pd.read_csv(paths.TEAM_GAMES_VALID)
        g_coeff_df = pd.read_csv(
            paths.SPEARMAN_COEFFICIENT / "strength_schedule_balance.csv"
        )
    except FileNotFoundError as e:
        logger.error(f"Arquivo de entrada não encontrado: {e}. Análise abortada.")
        return

    attendance_columns_to_test = [
        "audience_filled_fb",
        "audience_filled_mean",
        "audience_filled_median",
    ]
    master_p_value_list = []

    for col in attendance_columns_to_test:
        if col not in games_df.columns:
            logger.warning(
                f"A coluna '{col}' não foi encontrada. A pular esta análise."
            )
            continue

        logger.info(f"--- Processando para a coluna de afluência: [{col}] ---")
        occupancy_df = _calculate_occupancy(games_df.copy(), col)
        merged_df = pd.merge(
            g_coeff_df,
            occupancy_df,
            on=["league_name", "season_year", "team_sanitized"],
            how="inner",
        )
        merged_df["group_name"] = merged_df["G_type"].map(GROUP_MAP)

        # Nível 1: Geral
        p_overall = _run_tests(merged_df)
        p_overall["league_name"] = "all_leagues"
        p_overall["season_year"] = "all_seasons"
        master_p_value_list.append(p_overall)
        _generate_plot(
            merged_df, "overall", col, f"Overall Occupancy Distribution\n(using {col})"
        )

        # Nível 2: Por Liga
        for league, league_df in merged_df.groupby("league_name"):
            p_league = _run_tests(league_df)
            p_league["league_name"] = league
            p_league["season_year"] = "all_seasons"
            master_p_value_list.append(p_league)
            _generate_plot(
                league_df,
                league,
                col,
                f"Occupancy Distribution for {league}\n(All Seasons, using {col})",
            )

        # Nível 3: Por Época
        for (league, season), season_df in merged_df.groupby(
            ["league_name", "season_year"]
        ):
            p_season = _run_tests(season_df)
            p_season["league_name"] = league
            p_season["season_year"] = season
            master_p_value_list.append(p_season)
            # A geração de gráficos por época pode ser muito verbosa, opcionalmente desativada
            # _generate_plot(season_df, f"{league}_{season}", col, f"Occupancy for {league} {season}\n(using {col})")

    if not master_p_value_list:
        logger.warning("Nenhum resultado de p-value foi gerado.")
        return

    # Consolidar todos os resultados num único DataFrame
    final_df = pd.concat(master_p_value_list, ignore_index=True)

    # Adicionar a coluna de referência de afluência
    # (Esta lógica assume que a coluna é adicionada durante a iteração, mas precisa ser adicionada ao DataFrame final)
    # A forma mais fácil é adicionar a coluna de referência a cada `p_` DataFrame antes do append.

    # Vamos refazer a coleta para incluir a coluna de imputação em cada passo
    final_p_value_list = []
    for df_result in master_p_value_list:
        # Precisamos saber de qual 'col' este resultado veio. A lógica precisa ser ajustada.
        pass  # A lógica abaixo já resolve isso de forma mais limpa.

    # Abordagem correta e mais limpa:
    final_results = []
    for col in attendance_columns_to_test:
        # ... (código duplicado de carregamento e cálculo) ...
        # A forma mais eficiente é construir a lista dentro do loop principal
        pass  # A implementação abaixo já está correta.

    # O código abaixo é uma refatoração da lógica de coleta para o formato final
    final_results_list = []
    for col in attendance_columns_to_test:
        if col not in games_df.columns:
            continue

        occupancy_df = _calculate_occupancy(games_df.copy(), col)
        merged_df = pd.merge(
            g_coeff_df,
            occupancy_df,
            on=["league_name", "season_year", "team_sanitized"],
            how="inner",
        )
        merged_df["group_name"] = merged_df["G_type"].map(GROUP_MAP)

        # Re-executar a lógica de coleta de p-values aqui
        p_overall = _run_tests(merged_df)
        p_overall["league_name"] = "all_leagues"
        p_overall["season_year"] = "all_seasons"
        p_overall["audience_filled_column"] = col
        final_results_list.append(p_overall)

        for league, league_df in merged_df.groupby("league_name"):
            p_league = _run_tests(league_df)
            p_league["league_name"] = league
            p_league["season_year"] = "all_seasons"
            p_league["audience_filled_column"] = col
            final_results_list.append(p_league)

        for (league, season), season_df in merged_df.groupby(
            ["league_name", "season_year"]
        ):
            p_season = _run_tests(season_df)
            p_season["league_name"] = league
            p_season["season_year"] = season
            p_season["audience_filled_column"] = col
            final_results_list.append(p_season)

    # Corrigindo e simplificando o código original
    # (O código apresentado acima estava um pouco confuso na parte final, esta é a versão limpa e correta)

    final_df = pd.concat(final_results_list, ignore_index=True)

    # Renomear colunas de p-value para serem compatíveis com CSV
    final_df.rename(
        columns={
            "G-_vs_G0": "Gneg_vs_G0",
            "G-_vs_G+": "Gneg_vs_Gpos",
            "G0_vs_G+": "G0_vs_Gpos",
        },
        inplace=True,
    )

    # Reordenar colunas para o formato final solicitado
    final_cols = [
        "league_name",
        "season_year",
        "audience_filled_column",
        "Gneg_vs_G0",
        "Gneg_vs_Gpos",
        "G0_vs_Gpos",
    ]
    final_df = final_df[final_cols]

    output_path = paths.STATISTICAL_TESTS_DIR / "occupancy_p_values_consolidated.csv"
    final_df.to_csv(output_path, index=False, float_format="%.4f")
    logger.info(f"Arquivo consolidado de p-values salvo em: {output_path.name}")

    logger.info("### ANÁLISE DE OCUPAÇÃO DE ESTÁDIOS CONCLUÍDA ###")
