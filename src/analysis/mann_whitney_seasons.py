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

# Lista de colunas para análise iterativa
G_TYPE_COLUMNS = [
    "G_type_0.200",
    "G_type_0.250",
    "G_type_0.300",
    "G_type_0.350",
    "G_type_0.400",
]


def _calculate_stats_and_tests(
    df_subset: pd.DataFrame, context_name: str, context_type: str
) -> Dict[str, Any]:
    """
    Calcula metadados e testes Mann-Whitney para um subconjunto de dados.
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
        name: group_data["final_position"]
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


def _plot_rank_distribution(df: pd.DataFrame, title: str, output_path: Path) -> None:
    """
    Gera um Boxplot da distribuição de Ranks Finais por Grupo (G-, G0, G+).
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
        y="final_position",
        hue="group_name",
        legend=False,
        order=GROUP_ORDER,
        palette="viridis",
        width=0.6,
        linewidth=1.5,
    )

    plt.title(title, fontsize=14, pad=15)
    plt.xlabel("Grupo de Tabela (Schedule Strength)", fontsize=12)
    plt.ylabel("Posição Final (Rank)", fontsize=12)

    ax.yaxis.set_major_locator(mtick.MaxNLocator(integer=True))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()


def run_seasons_analysis() -> None:
    """
    Orquestra a análise estatística iterando sobre múltiplas colunas G_type.
    """
    logger.info("--- Iniciando Análise Iterativa (Mann-Whitney & Boxplots) ---")

    try:
        df_full = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        logger.info("Arquivo carregado. Total linhas: %d", len(df_full))
    except FileNotFoundError:
        logger.error("Arquivo não encontrado: %s", paths.SPEARMAN_BALANCE_PATH)
        return

    # Ajuste de Caminho: Sobe um nível para sair de '.../seasons/plots' para '.../seasons/'
    # Assume que MANN_WHITNEY_PLOTS_DIR termina em 'plots' ou similar.
    seasons_output_root = paths.MANN_WHITNEY_PLOTS_DIR.parent
    logger.info("Diretório base de saída definido para: %s", seasons_output_root)

    # Loop Principal: Itera sobre cada coluna de G_type definida
    for g_col in G_TYPE_COLUMNS:
        logger.info("\n=== Processando Variável: %s ===", g_col)

        if g_col not in df_full.columns:
            logger.warning("Coluna '%s' não encontrada no dataset. Pulando.", g_col)
            continue

        # 1. Configuração de Diretórios Específicos
        # Estrutura final: mann_whitney/seasons/G_type_X.XXX/
        current_base_dir = seasons_output_root / g_col
        current_boxplots_dir = current_base_dir / "boxplots"

        current_base_dir.mkdir(parents=True, exist_ok=True)
        current_boxplots_dir.mkdir(parents=True, exist_ok=True)

        # 2. Preparação dos Dados para a Coluna Atual
        df_clean = df_full.copy()
        df_clean["group_name"] = df_clean[g_col].map(GROUP_MAP)
        df_clean.dropna(
            subset=["final_position", "group_name", "league_name"], inplace=True
        )

        if df_clean.empty:
            logger.warning("Sem dados válidos para %s após limpeza. Pulando.", g_col)
            continue

        results_list = []

        # ---------------------------------------------------------
        # ANÁLISE 1: Agrupamento por Liga
        # ---------------------------------------------------------
        for league_name, league_df in df_clean.groupby("league_name"):
            # A. Estatística
            stats = _calculate_stats_and_tests(
                df_subset=league_df, context_name=league_name, context_type="League"
            )
            results_list.append(stats)

            # B. Visualização (Boxplot da Liga) -> Salvo em /boxplots
            safe_filename = league_name.replace(" ", "_").lower()
            plot_path = current_boxplots_dir / f"boxplot_{safe_filename}.png"
            plot_title = f"Rank Dist - {league_name} ({g_col})"

            _plot_rank_distribution(league_df, plot_title, plot_path)

        # ---------------------------------------------------------
        # ANÁLISE 2: Global
        # ---------------------------------------------------------
        # A. Estatística
        global_stats = _calculate_stats_and_tests(
            df_subset=df_clean,
            context_name="Global (All Leagues)",
            context_type="Global",
        )
        results_list.append(global_stats)

        # B. Visualização (Boxplot Global) -> Salvo em /boxplots (ALTERADO)
        plot_path_global = current_boxplots_dir / "boxplot_global_distribution.png"
        _plot_rank_distribution(
            df_clean, f"Distribuição Global de Ranks ({g_col})", plot_path_global
        )

        # 3. Consolidação e Salvamento dos Resultados da Variável Atual
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

        # CSV Salvo na raiz da variável: mann_whitney/seasons/G_type_X.XXX/
        output_csv = current_base_dir / "mann_whitney_aggregated_results.csv"
        results_df.to_csv(output_csv, index=False, float_format="%.6f")

        logger.info("Resultados salvos para %s em: %s", g_col, output_csv)

    logger.info("\n--- Análise Iterativa Concluída ---")


if __name__ == "__main__":
    run_seasons_analysis()
