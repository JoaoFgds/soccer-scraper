import logging
import pandas as pd
import seaborn as sns  # Re-importar
import matplotlib.pyplot as plt  # Re-importar
import matplotlib.ticker as mtick  # Re-importar

from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING
from itertools import combinations

from src.utils import paths
from scipy.stats import mannwhitneyu

if TYPE_CHECKING:
    from pandas import DataFrame

logger = logging.getLogger(__name__)


# Mapeamento e ordem dos grupos
GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]
# Pares de comparação para o teste
COMPARISON_PAIRS = [("G-", "G0"), ("G-", "G+"), ("G0", "G+")]
P_VALUE_COLS = [f"{g1}_vs_{g2}" for g1, g2 in COMPARISON_PAIRS]

# Constantes para focar a análise
G_TYPE_COLUMN_TO_ANALYZE = "G_type_0.300"
ATTENDANCE_COLUMN = "audience_filled_fb"


def _calculate_occupancy(games_df: pd.DataFrame, attendance_col: str) -> "DataFrame":
    """Calcula a ocupação média do estádio para cada time-temporada."""
    logger.info("Calculando ocupação média usando a coluna: '%s'", attendance_col)
    home_games = games_df.dropna(subset=[attendance_col]).copy()
    home_games = home_games[home_games[attendance_col] > 0]

    occupancy_stats = (
        home_games.groupby(["league_name", "season_year", "home_team_canonical"])
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
        columns={"home_team_canonical": "team_canonical"}, inplace=True
    )

    paths.MANN_WHITNEY_ATT_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = (
        paths.MANN_WHITNEY_ATT_AUDIT_DIR / f"occupancy_audit_{attendance_col}.csv"
    )
    occupancy_stats.to_csv(audit_path, index=False, float_format="%.2f")
    logger.info("Tabela de auditoria de ocupação salva em: %s", audit_path)

    return occupancy_stats[
        ["league_name", "season_year", "team_canonical", "average_occupancy"]
    ]


def _run_tests_on_subset(data: pd.DataFrame) -> Optional["DataFrame"]:
    """
    Executa testes Mann-Whitney U pareados em um subconjunto de dados,
    comparando a métrica 'average_occupancy'.
    """
    groups = {
        name: group_data["average_occupancy"]
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
    """Calcula as estatísticas de significância para um grupo de p-values."""
    if group.empty:
        return pd.Series(dtype="float64")

    stats = {}
    total_tests = len(group)
    significant_mask = group["p_value"] < 0.05
    total_significant = significant_mask.sum()

    stats["total_comparisons"] = total_tests
    stats["significant_comparisons"] = total_significant

    if total_tests > 0:
        stats["significant_percentage"] = total_significant / total_tests
    else:
        stats["significant_percentage"] = 0.0

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


# --- INÍCIO DAS FUNÇÕES DE PLOTAGEM ---


def _generate_occupancy_boxplot(
    df: pd.DataFrame, output_path: Path, title: str
) -> None:
    """(GRÁFICO 1) Gera o boxplot de 3 grupos para ocupação."""
    if df.empty or df["group_name"].nunique() < 2:
        logger.warning(...)
        return

    logger.info("Gerando boxplot de ocupação: %s", title)
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
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Ocupação Média do Estádio", fontsize=12)
    plt.ylim(0, 1.05)
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Boxplot de ocupação salvo em: %s", output_path)


# --- ADIÇÃO DA FUNÇÃO VIOLIN PLOT PARA OCUPAÇÃO ---
def _plot_violin_occupancy_G_vs_Gplus(
    df: pd.DataFrame, output_path: Path, title: str
) -> None:
    """
    (NOVO GRÁFICO SUGErIDO PARA OCUPAÇÃO)
    Gera um gráfico de violino comparando apenas G- e G+ para
    mostrar a forma completa da distribuição da ocupação média.
    """
    logger.info("Gerando violin plot de ocupação (G- vs G+): %s", title)

    # 1. Filtra os dados
    df_plot = df[df["group_name"].isin(["G-", "G+"])].copy()

    if df_plot.empty or df_plot["group_name"].nunique() < 2:
        logger.warning(
            "Pulando geração do violin plot de ocupação para '%s' (dados insuficientes).",
            title,
        )
        return

    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    # 2. Define paleta
    palette = {"G-": "#91cf60", "G+": "#d01c8b"}  # Verde e Magenta

    # 3. Cria o gráfico de violino
    sns.violinplot(
        x="group_name",
        y="average_occupancy",  # Mudança aqui
        data=df_plot,
        order=["G-", "G+"],
        palette=palette,
        hue="group_name",
        legend=False,
        inner="quartile",
        cut=0,
    )

    # 4. Ajusta eixo Y
    plt.ylim(0, 1.05)
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Ocupação Média do Estádio", fontsize=12)  # Mudança aqui
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Violin plot de ocupação salvo em: %s", output_path)


# --- FIM DA ADIÇÃO ---


def _plot_significance_by_league(summary_df: pd.DataFrame, output_path: Path) -> None:
    """(GRÁFICO 2) Gera um gráfico de barras respondendo "Qual liga tem maior efeito?"."""
    logger.info("Gerando gráfico de resumo de significância da ocupação por liga...")

    df_plot = summary_df[
        (summary_df["season_year"] == "all_seasons")
        & (summary_df["league_name"] != "all_leagues")
    ].sort_values(by="significant_percentage", ascending=False)

    if df_plot.empty:
        logger.warning("Sem dados agregados de liga para plotar resumo de ocupação.")
        return

    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")

    ax = sns.barplot(
        data=df_plot,
        x="significant_percentage",
        y="league_name",
        palette="coolwarm",
        hue="league_name",
        legend=False,
    )

    ax.set_title(
        f"Taxa de Significância (p < 0.05) na Ocupação por Liga\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Taxa de Significância (Comparações com p < 0.05)", fontsize=12)
    ax.set_ylabel("Liga", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlim(0, 1)

    for container in ax.containers:
        labels = [f"{v*100:.1f}%" if v > 0 else "" for v in container.datavalues]
        ax.bar_label(container, labels=labels, padding=5)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(
        "Gráfico de significância da ocupação por liga salvo em: %s", output_path
    )


def _plot_pvalue_distribution(melted_df: pd.DataFrame, output_path: Path) -> None:
    """(GRÁFICO 3) Gera um stripplot mostrando a distribuição de todos os p-values da ocupação."""
    logger.info("Gerando gráfico de distribuição de p-values da ocupação...")
    plt.figure(figsize=(14, 8))
    sns.set_style("whitegrid")

    ax = sns.stripplot(
        data=melted_df,
        x="comparison",
        y="p_value",
        hue="comparison",
        legend=False,
        jitter=0.2,
        alpha=0.6,
        palette="muted",
    )

    ax.axhline(
        0.05, ls="--", color="red", linewidth=2, label="Nível de Significância (0.05)"
    )

    ax.set_title(
        f"Distribuição P-Values (Ocupação por Liga/Temporada)\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Comparação de Grupos", fontsize=12)
    ax.set_ylabel("P-Value (Teste Mann-Whitney U)", fontsize=12)
    ax.set_ylim(-0.02, 1.02)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles=handles, labels=labels, loc="upper right")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(
        "Gráfico de distribuição de p-values da ocupação salvo em: %s", output_path
    )


# --- FIM DAS FUNÇÕES DE PLOTAGEM ---


def _analyze_occupancy_results(
    results_df: pd.DataFrame, plot_dir: Path, df_thresh: pd.DataFrame
) -> None:
    """
    Analisa p-values da ocupação, salva CSV de contagens, gera plots de resumo
    e loga as respostas para as perguntas de negócio.
    """
    logger.info("--- Iniciando Análise Agregada dos P-Values (Ocupação) ---")

    original_row_count = len(results_df)
    analysis_df = results_df.dropna(subset=P_VALUE_COLS)
    dropped_rows = original_row_count - len(analysis_df)

    if dropped_rows > 0:
        logger.info(f"Removidas {dropped_rows} linhas...")

    if analysis_df.empty:
        logger.warning("Nenhuma linha com conjunto completo de p-values encontrada...")
        # Tentar gerar pelo menos o boxplot geral
        plot_path_box = (
            plot_dir / f"occupancy_dist_overall_{G_TYPE_COLUMN_TO_ANALYZE}.png"
        )
        title_box = f"Distribuição Geral da Ocupação Média\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
        _generate_occupancy_boxplot(df_thresh, plot_path_box, title_box)
        # Tentar gerar o violin plot
        plot_path_violin = (
            plot_dir
            / f"occupancy_violin_Gminus_vs_Gplus_{G_TYPE_COLUMN_TO_ANALYZE}.png"
        )
        title_violin = f"Comparação Distribuição Ocupação (G- vs G+)\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
        _plot_violin_occupancy_G_vs_Gplus(df_thresh, plot_path_violin, title_violin)
        return

    melted_df = analysis_df.melt(
        id_vars=["league_name", "season_year"],
        value_vars=P_VALUE_COLS,
        var_name="comparison",
        value_name="p_value",
    )

    # --- 1. Calcular agregações nos 3 níveis ---
    agg_season = (
        melted_df.groupby(["league_name", "season_year"])
        .apply(_calculate_significance_stats, include_groups=False)
        .reset_index()
    )
    agg_league = (
        melted_df.groupby("league_name")
        .apply(_calculate_significance_stats, include_groups=False)
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"
    agg_all_series = _calculate_significance_stats(melted_df)
    agg_all = agg_all_series.to_frame().T
    agg_all["league_name"] = "all_leagues"
    agg_all["season_year"] = "all_seasons"

    for col in agg_all.columns:
        if col not in ["league_name", "season_year"]:
            try:
                agg_all[col] = pd.to_numeric(agg_all[col])
            except ValueError:
                pass

    # --- 2. Combinar e Salvar o CSV de contagens ---
    summary_df = pd.concat([agg_season, agg_league, agg_all], ignore_index=True)
    id_cols = ["league_name", "season_year"]
    metric_cols = [
        "total_comparisons",
        "significant_comparisons",
        "significant_percentage",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df = summary_df.reindex(columns=id_cols + metric_cols)
    summary_df.sort_values(by=["league_name", "season_year"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)
    int_cols = [
        "total_comparisons",
        "significant_comparisons",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df[int_cols] = summary_df[int_cols].fillna(0).astype(int)

    paths.MANN_WHITNEY_ATT_METRICS_DIR.mkdir(parents=True, exist_ok=True)
    output_path_csv = (
        paths.MANN_WHITNEY_ATT_METRICS_DIR
        / "per_season_mann_whitney_p_values_summary.csv"
    )
    summary_df.to_csv(output_path_csv, index=False, float_format="%.4f")
    logger.info(
        "CSV de sumarização de significância da ocupação salvo em: %s", output_path_csv
    )

    # --- 3. Gerar Gráficos de Resumo ---
    # Boxplot e Violin são gerados antes, na função principal
    plot_path_league_summary = plot_dir / "summary_occupancy_significance_by_league.png"
    _plot_significance_by_league(summary_df, plot_path_league_summary)
    plot_path_pvalue_dist = plot_dir / "summary_occupancy_pvalue_distribution.png"
    _plot_pvalue_distribution(melted_df, plot_path_pvalue_dist)

    # --- 4. Logar as respostas ---
    overall_stats = summary_df[
        (summary_df["league_name"] == "all_leagues")
        & (summary_df["season_year"] == "all_seasons")
    ]
    if not overall_stats.empty:
        stats = overall_stats.iloc[0]
        logger.info(
            f"[Análise Geral Ocupação (...)] Percentual de comparações significativas (...): {stats['significant_percentage'] * 100:.2f}% (...)"
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
            f"[Análise por Liga Ocupação (...)] Liga com MAIOR efeito (...): {league_summary.index[0]} ({league_summary.iloc[0] * 100:.2f}%)"
        )
        logger.info(
            f"[Análise por Liga Ocupação (...)] Liga com MENOR efeito (...): {league_summary.index[-1]} ({league_summary.iloc[-1] * 100:.2f}%)"
        )
        league_summary_log = (league_summary * 100).to_string(float_format="%.2f%%")
        logger.info(
            "Sumário completo da taxa de significância da ocupação por liga:\n%s",
            league_summary_log,
        )

    logger.info("--- Análise Agregada de Ocupação Concluída ---")


def run_occupancy_analysis() -> None:
    """
    Orquestra a análise de sensibilidade da ocupação do estádio.
    """
    logger.info("--- Iniciando Análise de Ocupação do Estádio (Mann-Whitney U) ---")

    plot_dir = paths.MANN_WHITNEY_ATT_PLOTS_DIR
    plot_dir.mkdir(parents=True, exist_ok=True)

    try:
        games_df = pd.read_csv(paths.GAMES_VALID_PATH)
        g_coeff_df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
    except FileNotFoundError as e:
        logger.error("...")
        return

    # Validações...
    if ATTENDANCE_COLUMN not in games_df.columns:
        logger.error("...")
        return
    if G_TYPE_COLUMN_TO_ANALYZE not in g_coeff_df.columns:
        logger.error("...")
        return

    logger.info("Processando com a coluna de público: [%s]", ATTENDANCE_COLUMN)
    logger.info("Processando com o limiar G_type: [%s]", G_TYPE_COLUMN_TO_ANALYZE)

    # 1. Calcular Métrica
    occupancy_df = _calculate_occupancy(games_df, ATTENDANCE_COLUMN)

    # 2. Mesclar dados
    merged_df = pd.merge(
        g_coeff_df,
        occupancy_df,
        on=["league_name", "season_year", "team_canonical"],
        how="inner",
    )

    # 3. Mapear grupos e limpar dados
    merged_df["group_name"] = merged_df[G_TYPE_COLUMN_TO_ANALYZE].map(GROUP_MAP)
    merged_df.dropna(subset=["average_occupancy", "group_name"], inplace=True)

    if merged_df.empty:
        logger.warning("Não há dados válidos após a fusão e limpeza. Abortando.")
        return

    # --- GERA OS GRÁFICOS INICIAIS (BOXPLOT E VIOLINO) ---
    plot_path_box = plot_dir / f"occupancy_dist_overall_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    title_box = (
        f"Distribuição Geral da Ocupação Média\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    )
    _generate_occupancy_boxplot(merged_df, plot_path_box, title_box)

    plot_path_violin = (
        plot_dir / f"occupancy_violin_Gminus_vs_Gplus_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    )
    title_violin = f"Comparação Distribuição Ocupação (G- vs G+)\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    _plot_violin_occupancy_G_vs_Gplus(merged_df, plot_path_violin, title_violin)

    all_p_value_results: List["DataFrame"] = []

    # 4. Loop Nível 3 (liga, temporada)
    for (league, season), season_df in merged_df.groupby(
        ["league_name", "season_year"]
    ):
        if (p_season := _run_tests_on_subset(season_df)) is not None:
            p_season["league_name"] = league
            p_season["season_year"] = season
            all_p_value_results.append(p_season)

    if not all_p_value_results:
        logger.warning("Nenhum resultado de p-value foi gerado.")
        logger.info(
            "--- Análise de Ocupação do Estádio Concluída (sem testes estatísticos) ---"
        )
        return

    # 5. Concatenar Resultados
    final_df = pd.concat(all_p_value_results, ignore_index=True)
    final_cols = ["league_name", "season_year"] + P_VALUE_COLS
    final_df = final_df.reindex(columns=final_cols)

    # 6. Salvar P-Values Brutos
    final_df.to_csv(paths.OCCUPANCY_P_VALUES_PATH, index=False, float_format="%.4f")
    logger.info(
        "P-values da análise de ocupação salvos em: %s", paths.OCCUPANCY_P_VALUES_PATH
    )

    # --- 7. Executa a análise, salva CSV de contagens e gera gráficos ---
    _analyze_occupancy_results(final_df, plot_dir, merged_df)

    logger.info("--- Análise de Ocupação do Estádio Concluída ---")


if __name__ == "__main__":
    run_occupancy_analysis()
