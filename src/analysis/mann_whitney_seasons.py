import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

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
    """Calcula as estatísticas de significância para um grupo de p-values."""
    if group.empty:
        return pd.Series(dtype="float64")

    stats = {}
    total_tests = len(group)
    significant_mask = group["p_value"] < 0.05
    total_significant = significant_mask.sum()

    stats["total_tests"] = total_tests
    stats["significant_tests"] = total_significant

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


def _generate_boxplot(df: pd.DataFrame, output_path: Path, title: str) -> None:
    """(GRÁFICO 1) Gera o boxplot de 3 grupos."""
    if df.empty or df["group_name"].nunique() < 2:
        logger.warning(
            "Pulando geração do boxplot para '%s' (dados insuficientes).", title
        )
        return

    logger.info("Gerando boxplot: %s", title)
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

    plt.gca().invert_yaxis()
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Posição Final no Torneio (Rank)", fontsize=12)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Boxplot salvo em: %s", output_path)


def _plot_violin_G_vs_Gplus(df: pd.DataFrame, output_path: Path, title: str) -> None:
    """
    (NOVO GRÁFICO SUGErIDO)
    Gera um gráfico de violino comparando apenas G- e G+ para
    mostrar a forma completa da distribuição.
    """
    logger.info("Gerando violin plot (G- vs G+): %s", title)

    # 1. Filtra os dados para incluir apenas os dois grupos extremos
    df_plot = df[df["group_name"].isin(["G-", "G+"])].copy()

    if df_plot.empty or df_plot["group_name"].nunique() < 2:
        logger.warning(
            "Pulando geração do violin plot para '%s' (dados insuficientes).", title
        )
        return

    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    # 2. Define uma paleta de cores para os grupos
    palette = {"G-": "#91cf60", "G+": "#d01c8b"}  # Verde e Magenta

    # 3. Cria o gráfico de violino
    sns.violinplot(
        x="group_name",
        y="final_position",
        data=df_plot,
        order=["G-", "G+"],  # Garante a ordem
        palette=palette,
        hue="group_name",  # Para evitar o FutureWarning
        legend=False,
        inner="quartile",  # Mostra as linhas de quartil dentro do violino
        cut=0,  # Limita o violino ao range dos dados
    )

    # 4. Inverte o eixo Y
    plt.gca().invert_yaxis()

    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Posição Final no Torneio (Rank)", fontsize=12)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Violin plot salvo em: %s", output_path)


def _plot_significance_by_league(summary_df: pd.DataFrame, output_path: Path) -> None:
    """(GRÁFICO 2) Gera um gráfico de barras respondendo "Qual liga tem maior efeito?"."""
    logger.info("Gerando gráfico de resumo de significância por liga...")

    df_plot = summary_df[
        (summary_df["season_year"] == "all_seasons")
        & (summary_df["league_name"] != "all_leagues")
    ].sort_values(by="significant_percentage", ascending=False)

    if df_plot.empty:
        logger.warning("Sem dados agregados de liga para plotar resumo.")
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
        f"Taxa de Significância (p < 0.05) por Liga\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Taxa de Significância (Testes com p < 0.05)", fontsize=12)
    ax.set_ylabel("Liga", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlim(0, 1)

    for container in ax.containers:
        labels = [f"{v*100:.1f}%" if v > 0 else "" for v in container.datavalues]
        ax.bar_label(container, labels=labels, padding=5)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Gráfico de significância por liga salvo em: %s", output_path)


def _plot_pvalue_distribution(melted_df: pd.DataFrame, output_path: Path) -> None:
    """(GRÁFICO 3) Gera um stripplot mostrando a distribuição de todos os p-values."""
    logger.info("Gerando gráfico de distribuição de p-values...")
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
        f"Distribuição de Todos P-Values (Testes por Liga/Temporada)\n"
        f"(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
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
    logger.info("Gráfico de distribuição de p-values salvo em: %s", output_path)


# --- FIM DAS FUNÇÕES DE PLOTAGEM ---


def _analyze_and_log_results(results_df: pd.DataFrame, plot_dir: Path) -> None:
    """
    Analisa p-values, salva CSV de contagens, gera plots de resumo
    e loga as respostas para as perguntas de negócio.
    """
    logger.info(
        "--- Iniciando Análise Agregada dos P-Values (para %s) ---",
        G_TYPE_COLUMN_TO_ANALYZE,
    )

    original_row_count = len(results_df)
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

    # --- 3. Gerar Gráficos de Resumo ---
    plot_path_league_summary = plot_dir / "summary_significance_by_league.png"
    _plot_significance_by_league(summary_df, plot_path_league_summary)

    plot_path_pvalue_dist = plot_dir / "summary_pvalue_distribution.png"
    _plot_pvalue_distribution(melted_df, plot_path_pvalue_dist)

    # --- 4. Logar as respostas ---
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


def run_seasons_analysis() -> None:
    """
    Orquestra a análise estatística do balanço da tabela nos ranks finais
    focando em um único limiar (G_type_0.300).
    """
    logger.info("Iniciando análise de significância estatística (Mann-Whitney U).")

    # Define e cria o diretório de plots
    plot_dir = paths.MANN_WHITNEY_PLOTS_DIR
    plot_dir.mkdir(parents=True, exist_ok=True)

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

    # --- GERA O GRÁFICO 1 (BOXPLOT) ---
    plot_path_box = plot_dir / f"rank_dist_overall_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    title = f"Distribuição Geral do Rank Final (Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    _generate_boxplot(df_thresh, plot_path_box, title)

    # --- GERA O NOVO GRÁFICO DE VIOLINO ---
    plot_path_violin = (
        plot_dir / f"violin_Gminus_vs_Gplus_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    )
    title_violin = (
        f"Comparação da Distribuição de Ranks (G- vs G+)\n"
        f"(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    )
    _plot_violin_G_vs_Gplus(df_thresh, plot_path_violin, title_violin)

    # --- Executa testes na granularidade (liga, temporada) ---
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

    id_cols = ["league_name", "season_year"]
    results_df = results_df.reindex(columns=id_cols + P_VALUE_COLS)

    results_df.to_csv(
        paths.PER_SEASON_MANN_WHITNEY_PATH, index=False, float_format="%.4f"
    )
    logger.info(
        "Resultados (p-values) para '%s' salvos em: %s",
        G_TYPE_COLUMN_TO_ANALYZE,
        paths.PER_SEASON_MANN_WHITNEY_PATH,
    )

    # Executa a análise, salva o CSV de contagens e loga os resultados
    _analyze_and_log_results(results_df, plot_dir)

    logger.info("Análise estatística completa.")


if __name__ == "__main__":
    run_statistical_analysis()
