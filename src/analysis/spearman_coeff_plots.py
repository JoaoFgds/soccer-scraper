import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from pathlib import Path
from typing import List, Dict, Any, Set
import re
from src.utils import paths

# Configuração do Logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Paleta de cores padrão para os tipos
TYPE_COLORS = {
    "balanced": "#1f77b4",  # Azul
    "unbalanced_strong": "#d62728",  # Vermelho
    "unbalanced_weak": "#2ca02c",  # Verde
}
TYPE_NAMES_MAP = {
    "balanced": "Balanceada",
    "unbalanced_strong": "Forte",
    "unbalanced_weak": "Fraca",
}
# Define os tipos fixos que procuramos
PLOT_TYPES = ["balanced", "unbalanced_strong", "unbalanced_weak"]

# Constante para o nome da categoria geral
OVERALL_LABEL = "Overall"


def _plot_stacked_distribution(
    summary_df: pd.DataFrame, threshold: float, output_path: Path
):
    """
    (Gráfico 1) Gera um gráfico de barras 100% empilhadas para um
    único limiar, comparando a distribuição por liga e incluindo 'Overall'.
    """
    logger.info(
        "Gerando gráfico de distribuição (Barras Empilhadas 100%%) "
        "para o limiar %.3f...",
        threshold,
    )

    # 1. Filtrar dados agregados por liga
    df_plot = summary_df[(summary_df["season_year"] == "all_seasons")].copy()

    if df_plot.empty:
        logger.warning("Não há dados 'all_seasons' para plotar a distribuição.")
        return

    # Renomear 'all_leagues' para 'Overall' para o gráfico
    df_plot["league_name"] = df_plot["league_name"].replace(
        {"all_leagues": OVERALL_LABEL}
    )

    # 2. Preparar colunas e calcular proporções (MODO ROBUSTO)
    base_col_prefix = f"G_type_{threshold:.3f}_"

    count_cols = []
    plot_types_found = []  # Tipos que realmente existem no DF

    # Itera sobre os tipos fixos e verifica se a coluna existe
    for plot_type in PLOT_TYPES:
        col_name = f"{base_col_prefix}{plot_type}"
        if col_name in df_plot.columns:
            count_cols.append(col_name)
            plot_types_found.append(plot_type)
        else:
            df_plot[col_name] = 0
            count_cols.append(col_name)
            plot_types_found.append(plot_type)
            logger.debug("Coluna '%s' não encontrada. Adicionando com zeros.", col_name)

    if not count_cols:
        logger.warning(
            "Nenhuma coluna de contagem válida encontrada para o limiar %.3f. "
            "Pulando gráfico.",
            threshold,
        )
        return

    df_plot["total_tests_safe"] = df_plot["total_tests"].replace(0, 1)

    prop_cols = {}
    for col in count_cols:
        prop_col_name = f"prop_{col}"
        df_plot[prop_col_name] = df_plot[col] / df_plot["total_tests_safe"]
        prop_cols[col] = prop_col_name

    # 3. Preparar para plotagem
    df_to_plot = df_plot[["league_name"] + list(prop_cols.values())].set_index(
        "league_name"
    )

    # Ordenar para que 'Overall (Geral)' apareça por último
    if OVERALL_LABEL in df_to_plot.index:
        other_leagues = sorted(
            [idx for idx in df_to_plot.index if idx != OVERALL_LABEL]
        )
        df_to_plot = df_to_plot.reindex(other_leagues + [OVERALL_LABEL])
    else:
        df_to_plot.sort_index(inplace=True)

    df_to_plot.columns = [TYPE_NAMES_MAP.get(pt, pt) for pt in plot_types_found]
    plot_colors = [TYPE_COLORS.get(pt) for pt in plot_types_found]

    # 4. Plotar
    sns.set_theme(style="whitegrid")
    ax = df_to_plot.plot(
        kind="bar", stacked=True, figsize=(12, 7), color=plot_colors, width=0.8
    )

    ax.set_title(
        f"Distribuição de Tipos de Tabela por Liga (Limiar G = {threshold:.3f})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Liga", fontsize=12)
    ax.set_ylabel("Proporção de Tabelas", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(0, 1)

    plt.xticks(rotation=45, ha="right")

    ax.legend(
        title="Tipo de Tabela",
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        borderaxespad=0.0,
    )

    plt.tight_layout(rect=[0, 0, 0.85, 1])
    plt.savefig(output_path)
    plt.close()
    logger.info("Gráfico salvo em: %s", output_path)


def _plot_threshold_sensitivity(
    summary_df: pd.DataFrame, thresholds: List[float], output_path: Path
):
    """
    (Gráfico 2) Gera um gráfico de linha mostrando a sensibilidade da
    classificação, destacando 'Overall' e com legenda clara.
    """
    logger.info("Gerando gráfico de sensibilidade ao limiar (Linhas)...")

    df_filtered = summary_df[(summary_df["season_year"] == "all_seasons")].copy()

    if df_filtered.empty:
        logger.warning("Não há dados 'all_seasons' para plotar a sensibilidade.")
        return

    df_filtered["league_name"] = df_filtered["league_name"].replace(
        {"all_leagues": OVERALL_LABEL}
    )

    id_vars = ["league_name", "total_tests"]
    value_vars = [col for col in df_filtered.columns if col.startswith("G_type_")]
    df_melted = df_filtered.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="g_type_full",
        value_name="count",
    )

    df_melted["total_tests_safe"] = df_melted["total_tests"].replace(0, 1)
    df_melted["percentage"] = df_melted["count"] / df_melted["total_tests_safe"]

    regex_pattern = r"G_type_(\d+\.\d+)_(balanced|unbalanced_strong|unbalanced_weak)$"
    extract_df = df_melted["g_type_full"].str.extract(regex_pattern)

    df_melted["threshold"] = pd.to_numeric(extract_df[0])
    # A coluna 'g_type' terá os nomes em Português
    df_melted["g_type"] = extract_df[1].map(TYPE_NAMES_MAP)

    df_plot = df_melted.dropna(subset=["threshold", "g_type", "percentage"])

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(14, 8))

    # --- INÍCIO DA CORREÇÃO DO VALUEERROR ---

    # Crie uma paleta que mapeie os nomes em Português (do hue) para as cores
    mapped_palette = {
        portuguese_name: TYPE_COLORS[english_key]
        for english_key, portuguese_name in TYPE_NAMES_MAP.items()
        if english_key in TYPE_COLORS
    }

    # --- FIM DA CORREÇÃO ---

    df_leagues = df_plot[df_plot["league_name"] != OVERALL_LABEL]
    df_overall = df_plot[df_plot["league_name"] == OVERALL_LABEL]

    # Plotar as ligas individuais (cores por tipo, estilos por liga)
    ax = sns.lineplot(
        data=df_leagues,
        x="threshold",
        y="percentage",
        hue="g_type",  # Cores para os tipos (Balanceada, Desbalanceada...)
        style="league_name",  # Estilos para as ligas
        markers=True,
        markersize=7,
        linewidth=2,
        palette=mapped_palette,  # Use a paleta mapeada correta
        dashes=False,
    )

    # Plotar 'Overall (Geral)' por cima
    if not df_overall.empty:
        sns.lineplot(
            data=df_overall,
            ax=ax,
            x="threshold",
            y="percentage",
            hue="g_type",
            style="league_name",
            markers=True,
            markersize=10,
            linewidth=4,
            palette=mapped_palette,  # Use a paleta mapeada correta aqui também
            dashes=False,
            legend=False,  # Não crie entradas de legenda duplicadas
        )

    ax.set_title(
        "Análise de Sensibilidade: Proporção de Tipos de Tabela vs. Limiar (G)",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Limiar de Correlação (G)", fontsize=12)
    ax.set_ylabel("Proporção de Tabelas", fontsize=12)

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xticks(thresholds)
    ax.set_ylim(0, 1)

    # Reorganizar a legenda para maior clareza
    try:
        handles, labels = ax.get_legend_handles_labels()

        idx_gtype_start = labels.index("g_type")
        idx_league_name_start = -1
        for i, label in enumerate(labels[idx_gtype_start + 1 :]):
            if label == "league_name":
                idx_league_name_start = idx_gtype_start + 1 + i
                break

        if idx_league_name_start == -1:  # Not found
            raise ValueError("Formato de legenda simples.")

        g_type_handles = handles[idx_gtype_start + 1 : idx_league_name_start]
        g_type_labels = labels[idx_gtype_start + 1 : idx_league_name_start]

        league_name_handles = handles[idx_league_name_start + 1 :]
        league_name_labels = labels[idx_league_name_start + 1 :]

        legend1 = ax.legend(
            g_type_handles,
            g_type_labels,
            title="Tipo de Tabela",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            borderaxespad=0.0,
        )

        ax.add_artist(legend1)
        legend2 = ax.legend(
            league_name_handles,
            league_name_labels,
            title="Liga",
            bbox_to_anchor=(1.02, 0.65),
            loc="upper left",
            borderaxespad=0.0,
        )
    except ValueError:
        # Fallback se a divisão da legenda falhar
        ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0.0)

    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(output_path)
    plt.close()
    logger.info("Gráfico salvo em: %s", output_path)


def _plot_significance_rate(summary_df: pd.DataFrame, output_path: Path):
    """
    (Gráfico 3) Gera um gráfico de barras horizontais mostrando a taxa
    de significância, destacando 'Overall' no topo.
    """
    logger.info("Gerando gráfico de taxa de significância (Barras)...")

    df_plot = summary_df[(summary_df["season_year"] == "all_seasons")].copy()

    if df_plot.empty:
        logger.warning("Não há dados 'all_seasons' para plotar a significância.")
        return

    df_plot["league_name"] = df_plot["league_name"].replace(
        {"all_leagues": OVERALL_LABEL}
    )

    df_plot["total_tests_safe"] = df_plot["total_tests"].replace(0, 1)
    df_plot["significance_rate"] = (
        df_plot["significant_tests"] / df_plot["total_tests_safe"]
    )

    df_leagues = df_plot[df_plot["league_name"] != OVERALL_LABEL].sort_values(
        "significance_rate", ascending=False
    )
    df_overall = df_plot[df_plot["league_name"] == OVERALL_LABEL]

    df_plot = pd.concat([df_overall, df_leagues], ignore_index=True)

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    palette = {lg: "#aaaaaa" for lg in df_plot["league_name"] if lg != OVERALL_LABEL}
    palette[OVERALL_LABEL] = "#3498db"

    ax = sns.barplot(
        data=df_plot,
        x="significance_rate",
        y="league_name",
        palette=palette,
        hue="league_name",
        legend=False,
    )

    ax.set_title(
        "Taxa de Significância (p <= 0.05) por Liga (Todas as Temporadas)",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Taxa de Significância", fontsize=12)
    ax.set_ylabel("Liga", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlim(0, 1)

    for container in ax.containers:
        labels = [f"{v*100:.1f}%" if v > 0 else "" for v in container.datavalues]

        ax.bar_label(container, labels=labels, padding=5)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Gráfico salvo em: %s", output_path)


def generate_all_visualizations():
    """
    Função principal para orquestrar a geração de todos os gráficos
    de análise da sumarização.
    """
    logger.info("Iniciando geração de visualizações da análise...")

    summary_path = paths.SPEARMAN_SUMMARY_PATH
    output_dir = paths.SPEARMAN_COEFFICIENT_PLOTS

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error("Falha ao criar diretório de plots: %s", e)
        return

    try:
        df = pd.read_csv(summary_path)
        logger.info("Carregado '%s' com %d linhas.", summary_path, len(df))
    except FileNotFoundError:
        logger.error(
            "Arquivo de sumarização não encontrado: %s. Abortando.", summary_path
        )
        return
    except pd.errors.EmptyDataError:
        logger.error("Arquivo de sumarização '%s' está vazio. Abortando.", summary_path)
        return

    g_type_cols = [col for col in df.columns if col.startswith("G_type_")]

    thresholds_str = set()
    for col in g_type_cols:
        match = re.search(r"G_type_(\d+\.\d+)_", col)
        if match:
            thresholds_str.add(match.group(1))

    try:
        thresholds = sorted([float(t) for t in thresholds_str])
    except (ValueError, IndexError) as e:
        logger.error(
            "Falha ao parsear limiares das colunas: %s. Colunas: %s", e, g_type_cols
        )
        return

    if not thresholds:
        logger.error("Nenhum limiar (threshold) detectado nas colunas. Abortando.")
        return

    logger.info("Limiares detectados para análise: %s", thresholds)

    for t in thresholds:
        fname = output_dir / f"1_plot_distribuicao_thresh_{t:.3f}.png"
        _plot_stacked_distribution(df, t, fname)

    fname_sens = output_dir / "2_plot_sensibilidade_limiar.png"
    _plot_threshold_sensitivity(df, thresholds, fname_sens)

    fname_sig = output_dir / "3_plot_taxa_significancia.png"
    _plot_significance_rate(df, fname_sig)

    logger.info("Geração de visualizações concluída.")


# ---
# ### Gráfico 1: Distribuição de Tipos de Tabela por Liga (Gráfico de Barras Empilhadas 100%)

# * **Objetivo:** Este gráfico responde: "Qual a proporção de tabelas balanceadas, desbalanceadas-fortes e desbalanceadas-fracas em cada liga?" (Para um limiar G específico, ex: 0.200).

# * **Como Ler:**
#     * **Eixo X:** Cada barra é uma entidade (ex: "ligaportugal2", "ligue2", ou "Overall (Geral)").
#     * **Eixo Y:** Proporção de 0% a 100%.
#     * **Cores:** Cada cor representa um tipo de tabela (Azul=Balanceada, Vermelho=Forte, Verde=Fraca).

# * **Interpretação:**
#     * **Comparação Rápida:** Você pode ver instantaneamente se uma liga (ex: "ligaportugal2") tem uma proporção maior de tabelas "Balanceadas" (azul) do que outra (ex: "ligue2").
#     * **Benchmark:** A barra "Overall (Geral)" é a média de todo o seu conjunto de dados. Use-a como benchmark. Se uma liga tem muito mais vermelho ("Forte") do que o "Overall", ela se destaca como tendo mais tabelas que favorecem times mais fortes no final.

# ---
# ### Gráfico 2: Análise de Sensibilidade (Gráfico de Linhas)

# * **Objetivo:** Este gráfico é o mais complexo, mas muito poderoso. Ele responde: "Como as minhas conclusões mudam se eu for mais rigoroso sobre o que considero 'desbalanceado'?"

# * **Como Ler:**
#     * **Eixo X (Limiar G):** Esta é a sua "régua de rigor".
#         * `0.20` (Esquerda): É um limiar "frouxo". Qualquer correlação acima de 0.20 já é contada como "Forte".
#         * `0.40` (Direita): É um limiar "restrito". Uma correlação de 0.35 *não* é "Forte"; ela é contada como "Balanceada".
#     * **Eixo Y (Proporção):** A porcentagem de tabelas que se encaixam em cada categoria.
#     * **Legenda "Tipo de Tabela" (Cores):**
#         * **Azul:** Proporção de tabelas "Balanceadas".
#         * **Vermelho:** Proporção de tabelas "Desbalanceadas (Forte)".
#         * **Verde:** Proporção de tabelas "Desbalanceadas (Fraca)".
#     * **Legenda "Liga" (Marcadores):**
#         * **Círculo (o):** "ligaportugal2".
#         * **Cruz (x):** "ligue2".

# * **Interpretação (Usando o Exemplo da Imagem):**

#     1.  **Por que as linhas "Balanceada" (Azul) SOBEM?**
#         * Quando o limiar G é baixo (ex: 0.20), muitas tabelas caem nas categorias "Forte" ou "Fraca".
#         * À medida que você AUMENTA o limiar (move-se para a direita, ex: 0.40), você está sendo mais rigoroso. Tabelas que *eram* "Forte" (ex: com G=0.25) agora são consideradas "Balanceadas" (porque 0.25 < 0.40).
#         * **Conclusão:** A linha azul subir significa que a categoria "Balanceada" está "absorvendo" as tabelas que deixam de ser "Fortes" ou "Fracas" à medida que o rigor aumenta. No seu gráfico, com G=0.40, mais de 90% de todas as tabelas são consideradas "Balanceadas".

#     2.  **Por que as linhas "Desbalanceada" (Vermelha e Verde) CAEM?**
#         * Pela razão oposta. Com G=0.20, ~21% das tabelas são "Fortes" (linha vermelha).
#         * Quando você muda o limiar para G=0.40, apenas tabelas com correlação *acima* de 0.40 contam. Isso é muito mais raro, então a proporção de tabelas "Fortes" cai para apenas ~3-4%.

#     3.  **Comparando Ligas (Marcador 'o' vs. 'x'):**
#         * No seu gráfico, as linhas de "ligaportugal2" (círculo) e "ligue2" (cruz) estão quase perfeitamente sobrepostas.
#         * **Conclusão:** Isso significa que ambas as ligas se comportam de forma estatisticamente *idêntica* em relação a esta análise. Não há diferença notável entre elas.

#     4.  **Onde está o "Overall (Geral)"?**
#         * No gráfico antigo (o confuso), o "Overall" era a linha preta grossa. O gráfico novo (o corrigido) parece não estar mostrando essa linha (provavelmente um artefato da legenda ou da plotagem).
#         * **Como interpretar (quando estiver lá):** A linha "Overall" (geralmente plotada em preto e mais grossa) é a média de todas as ligas. Se a linha de uma liga (ex: "ligaportugal2") estiver muito acima ou abaixo da linha "Overall", significa que ela se comporta de forma diferente da média.

# ---
# ### Gráfico 3: Taxa de Significância (Gráfico de Barras Horizontais)

# * **Objetivo:** Este é o gráfico de "verificação de sanidade". Ele responde: "A minha análise é estatisticamente válida ou estou apenas medindo ruído?"

# * **Como Ler:**
#     * **Eixo Y:** As ligas e o "Overall (Geral)".
#     * **Eixo X:** A "Taxa de Significância" (0% a 100%).
#     * **Barras:** Mostram a porcentagem de testes (times) em cada liga que tiveram um `p-value <= 0.05`.

# * **Interpretação:**
#     * Este gráfico é o seu "filtro da verdade".
#     * Se a barra "Overall (Geral)" (azul) mostra 30%, isso significa que 30% de todas as correlações calculadas são estatisticamente significativas, enquanto 70% são provavelmente devidas ao acaso.
#     * **REGRA CRUCIAL:** Você só deve levar a sério as conclusões do Gráfico 1 (Distribuição) para ligas que têm uma *Taxa de Significância* (Gráfico 3) alta.
#     * **Exemplo:** Se o Gráfico 1 mostra que a "Liga X" tem 50% de tabelas "Desbalanceadas (Forte)", mas o Gráfico 3 mostra que a "Liga X" tem apenas 10% de Taxa de Significância, você *não pode* concluir que a Liga X é desbalanceada. Você deve concluir que a maioria desses 50% são resultados aleatórios (ruído).
