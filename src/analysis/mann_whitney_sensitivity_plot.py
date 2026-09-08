import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import logging
from pathlib import Path
from src.utils import paths

# Configuração
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_sensitivity_data(base_path: Path, label: str) -> pd.DataFrame:
    """Carrega o CSV de sensibilidade e prepara para plotagem."""
    csv_path = base_path / "sensitivity_analysis_summary.csv"
    if not csv_path.exists():
        logger.warning(f"Arquivo não encontrado: {csv_path}")
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    # Extrai o valor numérico do threshold (ex: "G_type_0.200" -> 0.200)
    df["threshold_value"] = (
        df["g_type_threshold"].str.extract(r"(\d+\.\d+)").astype(float)
    )
    df["metric_type"] = label
    return df


def plot_sensitivity_curve(df_all: pd.DataFrame, output_dir: Path):
    """
    Gera o Gráfico 1: Curva de Sensibilidade (% de Significância)
    Compara 'Rank Final' vs 'Attendance' no mesmo gráfico.
    """
    plt.figure(figsize=(10, 6))
    sns.set_style("whitegrid")

    # Plot de Linha com Marcadores
    sns.lineplot(
        data=df_all,
        x="threshold_value",
        y="percentage_significant",
        hue="metric_type",
        style="metric_type",
        markers=True,
        dashes=False,
        linewidth=2.5,
        markersize=9,
    )

    plt.title(
        "Sensibilidade do Teste Mann-Whitney por Limiar de G-Type", fontsize=15, pad=15
    )
    plt.xlabel("Limiar de Desequilíbrio (G-Type Threshold)", fontsize=12)
    plt.ylabel("% de Testes Significativos (p < 0.05)", fontsize=12)
    plt.ylim(0, df_all["percentage_significant"].max() * 1.2)  # Margem de 20% no topo

    # Anotação dos valores nos pontos
    for line in plt.gca().lines:
        x_data = line.get_xdata()
        y_data = line.get_ydata()
        for x, y in zip(x_data, y_data):
            plt.text(
                x, y + 0.5, f"{y:.1f}%", ha="center", fontsize=9, fontweight="bold"
            )

    plt.legend(title="Métrica Analisada")
    plt.tight_layout()

    out_path = output_dir / "sensitivity_curve_comparison.png"
    plt.savefig(out_path, dpi=300)
    plt.close()
    logger.info(f"Curva de sensibilidade salva: {out_path}")


def plot_stacked_breakdown(df_metric: pd.DataFrame, metric_name: str, output_dir: Path):
    """
    Gera o Gráfico 2: Barras Empilhadas (Breakdown por Par Comparativo)
    """
    if df_metric.empty:
        return

    # Preparar dados para formato longo (necessário para stack do seaborn/pandas)
    cols_pairs = ["sig_G-_vs_G0", "sig_G-_vs_G+", "sig_G0_vs_G+"]
    df_melted = df_metric.melt(
        id_vars=["threshold_value"],
        value_vars=cols_pairs,
        var_name="pair",
        value_name="count",
    )

    # Mapear nomes mais bonitos para a legenda
    pair_labels = {
        "sig_G-_vs_G0": "G- vs G0",
        "sig_G-_vs_G+": "G- vs G+ (Extremos)",
        "sig_G0_vs_G+": "G0 vs G+",
    }
    df_melted["pair"] = df_melted["pair"].map(pair_labels)

    plt.figure(figsize=(10, 6))
    sns.set_style("whitegrid")

    # Usar Pandas plot kind='bar' com stacked=True é mais fácil para empilhamento simples que o Seaborn
    # Pivotar de volta para usar o plot do pandas
    df_plot = df_melted.pivot(index="threshold_value", columns="pair", values="count")

    ax = df_plot.plot(
        kind="bar", stacked=True, colormap="viridis", figsize=(10, 6), rot=0
    )

    plt.title(f"Composição da Significância: {metric_name}", fontsize=15, pad=15)
    plt.xlabel("Limiar (G-Type Threshold)", fontsize=12)
    plt.ylabel("Qtd. de Testes Significativos", fontsize=12)
    plt.legend(title="Par Comparativo", bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()
    out_path = (
        output_dir
        / f"sensitivity_breakdown_{metric_name.lower().replace(' ', '_')}.png"
    )
    plt.savefig(out_path, dpi=300)
    plt.close()
    logger.info(f"Breakdown plot salvo: {out_path}")


def run_plotting():
    # 1. Definir os diretórios de entrada (Onde estão os CSVs de resultado)
    # paths.MANN_WHITNEY_PLOTS_DIR -> .../mann_whitney/seasons/plots
    # .parent -> .../mann_whitney/seasons
    seasons_base = paths.MANN_WHITNEY_PLOTS_DIR.parent

    # paths.MANN_WHITNEY_ATT_PLOTS_DIR -> .../mann_whitney/attendance/plots
    # .parent -> .../mann_whitney/attendance
    attendance_base = paths.MANN_WHITNEY_ATT_PLOTS_DIR.parent

    # 2. Definir o diretório de saída (Target: .../mann_whitney/reports)
    # Pegamos o pai de 'seasons_base' que é a pasta raiz 'mann_whitney'
    mann_whitney_root = seasons_base.parent
    plots_output_dir = mann_whitney_root / "reports"

    # Cria o diretório se não existir
    plots_output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Diretório de saída definido para: {plots_output_dir}")

    # 3. Carregar Dados
    df_seasons = load_sensitivity_data(seasons_base, "Classificação Final")
    df_attendance = load_sensitivity_data(attendance_base, "Attendance (Ocupação)")

    if df_seasons.empty and df_attendance.empty:
        logger.error("Nenhum dado encontrado para plotar.")
        return

    # 4. Gerar Curva Comparativa (Linha)
    df_combined = pd.concat([df_seasons, df_attendance], ignore_index=True)
    if not df_combined.empty:
        plot_sensitivity_curve(df_combined, plots_output_dir)

    # 5. Gerar Breakdowns (Barras Empilhadas) Individuais
    if not df_seasons.empty:
        plot_stacked_breakdown(df_seasons, "Classificação Final", plots_output_dir)

    if not df_attendance.empty:
        plot_stacked_breakdown(df_attendance, "Attendance", plots_output_dir)


if __name__ == "__main__":
    run_plotting()
