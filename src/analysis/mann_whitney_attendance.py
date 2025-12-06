import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from pathlib import Path
from src.utils import paths
from scipy.stats import mannwhitneyu
from typing import Optional, List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pandas import DataFrame

logger = logging.getLogger(__name__)

# --- Constantes ---
GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]
COMPARISON_PAIRS = [("G-", "G0"), ("G-", "G+"), ("G0", "G+")]
P_VALUE_COLS = [f"{g1}_vs_{g2}" for g1, g2 in COMPARISON_PAIRS]
ATTENDANCE_COLUMN = "audience_filled_fb"

# Lista de colunas para análise iterativa
G_TYPE_COLUMNS = [
    "G_type_0.200",
    "G_type_0.250",
    "G_type_0.300",
    "G_type_0.350",
    "G_type_0.400",
]


def _calculate_occupancy(games_df: pd.DataFrame, attendance_col: str) -> "DataFrame":
    """
    Calcula a ocupação média do estádio para cada combinação de time-temporada.
    Lógica original preservada: Filtra jogos em casa, calcula média/max,
    computa ocupação e salva CSV de auditoria.
    """
    logger.info("Calculando ocupação média usando coluna: '%s'", attendance_col)
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

    # Salvamento da Auditoria (Lógica Original Mantida)
    paths.MANN_WHITNEY_ATT_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = (
        paths.MANN_WHITNEY_ATT_AUDIT_DIR / f"occupancy_audit_{attendance_col}.csv"
    )
    occupancy_stats.to_csv(audit_path, index=False, float_format="%.2f")
    logger.info("Tabela de auditoria de ocupação salva em: %s", audit_path)

    return occupancy_stats[
        ["league_name", "season_year", "team_canonical", "average_occupancy"]
    ]


def _calculate_stats_and_tests(
    df_subset: pd.DataFrame, context_name: str, context_type: str
) -> Dict[str, Any]:
    """
    Calcula metadados e testes Mann-Whitney para um subconjunto de dados.
    Métrica analisada: average_occupancy.
    """
    unique_seasons = df_subset["season_year"].nunique()
    total_samples = len(df_subset)

    counts = df_subset["group_name"].value_counts().to_dict()
    count_g_minus = counts.get("G-", 0)
    count_g0 = counts.get("G0", 0)
    count_g_plus = counts.get("G+", 0)

    result = {
        "analysis_scope": context_type,
        "scope_name": context_name,
        "n_seasons": unique_seasons,
        "total_samples": total_samples,
        "count_G-": count_g_minus,
        "count_G0": count_g0,
        "count_G+": count_g_plus,
    }

    groups = {
        name: group_data["average_occupancy"]
        for name, group_data in df_subset.groupby("group_name")
    }

    for g1, g2 in COMPARISON_PAIRS:
        col_name = f"{g1}_vs_{g2}"
        if (g1 in groups and not groups[g1].empty) and (
            g2 in groups and not groups[g2].empty
        ):
            try:
                stat, p_value = mannwhitneyu(
                    groups[g1], groups[g2], alternative="two-sided"
                )
                result[col_name] = float(p_value)
            except ValueError:
                result[col_name] = None
        else:
            result[col_name] = None

    return result


def _plot_occupancy_distribution(
    df: pd.DataFrame, title: str, output_path: Path
) -> None:
    """
    Gera um Boxplot da distribuição de Ocupação Média por Grupo (G-, G0, G+).
    Eixo Y formatado em porcentagem (0% a 100%).
    """
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    if df.empty:
        logger.warning("Dados vazios para plotagem: %s", title)
        plt.close()
        return

    ax = sns.boxplot(
        data=df,
        x="group_name",
        y="average_occupancy",
        hue="group_name",
        legend=False,
        order=GROUP_ORDER,
        palette="viridis",
        width=0.6,
        linewidth=1.5,
    )

    plt.title(title, fontsize=14, pad=15)
    plt.xlabel("Grupo de Tabela (Schedule Strength)", fontsize=12)
    plt.ylabel("Ocupação Média do Estádio", fontsize=12)

    # Formatação Eixo Y (Percentual 0.0 - 1.0)
    plt.ylim(0, 1.05)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()


def run_occupancy_analysis() -> None:
    """
    Orquestra a análise de ocupação iterando sobre múltiplas colunas G_type.
    """
    logger.info("--- Iniciando Análise Iterativa de Ocupação (Mann-Whitney) ---")

    # 1. Carregamento dos Dados Brutos
    try:
        games_df = pd.read_csv(paths.GAMES_VALID_PATH)
        g_coeff_df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        logger.info("Arquivos carregados com sucesso.")
    except FileNotFoundError as e:
        logger.error("Arquivo obrigatório não encontrado: %s", e)
        return

    if ATTENDANCE_COLUMN not in games_df.columns:
        logger.error("Coluna '%s' não encontrada em games_df.", ATTENDANCE_COLUMN)
        return

    # 2. Cálculo da Ocupação (Independente do G-Type)
    # Isso gera o DataFrame com [league, season, team, average_occupancy]
    occupancy_df = _calculate_occupancy(games_df, ATTENDANCE_COLUMN)

    # Definição do Diretório Raiz de Saída: mann_whitney/attendance/
    # Assume que MANN_WHITNEY_ATT_PLOTS_DIR aponta para .../mann_whitney/attendance/plots
    attendance_output_root = paths.MANN_WHITNEY_ATT_PLOTS_DIR.parent
    logger.info("Diretório base de saída definido para: %s", attendance_output_root)

    # 3. Loop Principal: Itera sobre cada coluna de G_type
    for g_col in G_TYPE_COLUMNS:
        logger.info("\n=== Processando Variável: %s ===", g_col)

        if g_col not in g_coeff_df.columns:
            logger.warning(
                "Coluna '%s' não encontrada no dataset Spearman. Pulando.", g_col
            )
            continue

        # Configuração de Diretórios Específicos
        # Estrutura: mann_whitney/attendance/G_type_X.XXX/
        current_base_dir = attendance_output_root / g_col
        current_boxplots_dir = current_base_dir / "boxplots"

        current_base_dir.mkdir(parents=True, exist_ok=True)
        current_boxplots_dir.mkdir(parents=True, exist_ok=True)

        # Merge Ocupação + Dados de Spearman (G-Type atual)
        merged_df = pd.merge(
            g_coeff_df,
            occupancy_df,
            on=["league_name", "season_year", "team_canonical"],
            how="inner",
        )

        # Mapeamento e Limpeza
        merged_df["group_name"] = merged_df[g_col].map(GROUP_MAP)
        merged_df.dropna(subset=["average_occupancy", "group_name"], inplace=True)

        if merged_df.empty:
            logger.warning("Sem dados válidos para %s após merge. Pulando.", g_col)
            continue

        results_list = []

        # ---------------------------------------------------------
        # ANÁLISE 1: Agrupamento por Liga
        # ---------------------------------------------------------
        for league_name, league_df in merged_df.groupby("league_name"):
            # A. Estatística
            stats = _calculate_stats_and_tests(
                df_subset=league_df, context_name=league_name, context_type="League"
            )
            results_list.append(stats)

            # B. Visualização (Boxplot da Liga)
            safe_filename = league_name.replace(" ", "_").lower()
            plot_path = current_boxplots_dir / f"occupancy_boxplot_{safe_filename}.png"
            plot_title = f"Ocupação Média - {league_name} ({g_col})"

            _plot_occupancy_distribution(league_df, plot_title, plot_path)

        # ---------------------------------------------------------
        # ANÁLISE 2: Global
        # ---------------------------------------------------------
        # A. Estatística
        global_stats = _calculate_stats_and_tests(
            df_subset=merged_df,
            context_name="Global (All Leagues)",
            context_type="Global",
        )
        results_list.append(global_stats)

        # B. Visualização (Boxplot Global)
        plot_path_global = current_boxplots_dir / "occupancy_boxplot_global.png"
        _plot_occupancy_distribution(
            merged_df, f"Distribuição Global de Ocupação ({g_col})", plot_path_global
        )

        # 4. Consolidação e Salvamento
        results_df = pd.DataFrame(results_list)
        cols_order = [
            "analysis_scope",
            "scope_name",
            "n_seasons",
            "total_samples",
            "count_G-",
            "count_G0",
            "count_G+",
            "G-_vs_G0",
            "G-_vs_G+",
            "G0_vs_G+",
        ]
        results_df = results_df.reindex(columns=cols_order)

        output_csv = current_base_dir / "mann_whitney_attendance_results.csv"
        results_df.to_csv(output_csv, index=False, float_format="%.6f")

        logger.info("Resultados de ocupação salvos para %s em: %s", g_col, output_csv)

    logger.info("\n--- Análise Iterativa de Ocupação Concluída ---")


if __name__ == "__main__":
    run_occupancy_analysis()
