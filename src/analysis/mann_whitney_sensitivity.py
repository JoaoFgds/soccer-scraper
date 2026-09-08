import pandas as pd
import logging
from pathlib import Path
from src.utils import paths

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configurações de Análise
G_TYPE_FOLDERS = [
    "G_type_0.200",
    "G_type_0.250",
    "G_type_0.300",
    "G_type_0.350",
    "G_type_0.400",
]

COMPARISON_COLS = ["G-_vs_G0", "G-_vs_G+", "G0_vs_G+"]
SIGNIFICANCE_THRESHOLD = 0.05


def analyze_directory(base_dir: Path, result_filename: str, analysis_name: str) -> None:
    """
    Percorre as pastas de G_type dentro de base_dir, lê os CSVs de resultados
    e compila um relatório de sensibilidade estatística.
    """
    logger.info(f"--- Iniciando Análise de Sensibilidade: {analysis_name} ---")

    if not base_dir.exists():
        logger.error(f"Diretório base não encontrado: {base_dir}")
        return

    summary_rows = []

    for folder_name in G_TYPE_FOLDERS:
        # Caminho completo para o arquivo CSV de resultados daquele limiar
        # Ex: .../seasons/G_type_0.300/mann_whitney_aggregated_results.csv
        file_path = base_dir / folder_name / result_filename

        if not file_path.exists():
            logger.warning(f"Arquivo não encontrado para {folder_name}: {file_path}")
            continue

        try:
            df = pd.read_csv(file_path)

            # Validação básica de colunas
            if not all(col in df.columns for col in COMPARISON_COLS):
                logger.warning(f"Colunas ausentes em {file_path}. Pulando.")
                continue

            # 1. Total de Testes (Linhas * 3 Comparações)
            # Consideramos todas as linhas (Ligas individuais + Global)
            n_rows = len(df)
            total_tests = n_rows * 3

            # 2. Contagem de Significativos (p < 0.05)
            # Cria uma máscara booleana de todo o dataframe para as colunas de interesse
            sig_mask = df[COMPARISON_COLS] < SIGNIFICANCE_THRESHOLD

            total_significant = sig_mask.sum().sum()

            # 3. Quebra por par comparativo
            sig_G_minus_G0 = sig_mask["G-_vs_G0"].sum()
            sig_G_minus_Gplus = sig_mask["G-_vs_G+"].sum()
            sig_G0_Gplus = sig_mask["G0_vs_G+"].sum()

            # Cálculo de porcentagem
            pct_significant = (
                (total_significant / total_tests) if total_tests > 0 else 0.0
            )

            summary_rows.append(
                {
                    "g_type_threshold": folder_name,
                    "total_rows_analyzed": n_rows,
                    "total_tests_performed": total_tests,
                    "total_significant_tests": total_significant,
                    "percentage_significant": round(pct_significant * 100, 2),
                    "sig_G-_vs_G0": sig_G_minus_G0,
                    "sig_G-_vs_G+": sig_G_minus_Gplus,
                    "sig_G0_vs_G+": sig_G0_Gplus,
                }
            )

            logger.info(
                f"Processado {folder_name}: {total_significant}/{total_tests} significativos."
            )

        except Exception as e:
            logger.error(f"Erro ao processar {file_path}: {e}")

    # Salvar Relatório Consolidado
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)

        # Output na raiz da análise (ex: mann_whitney/seasons/sensitivity_summary.csv)
        output_path = base_dir / "sensitivity_analysis_summary.csv"
        summary_df.to_csv(output_path, index=False)

        logger.info(
            f"Relatório de Sensibilidade salvo com sucesso em:\n -> {output_path}"
        )
        logger.info(
            f"\nResumo da Análise ({analysis_name}):\n{summary_df.to_string(index=False)}"
        )
    else:
        logger.warning(f"Nenhum dado processado para {analysis_name}.")


def run_sensitivity_analysis():
    # 1. Análise para SEASONS (Classificação Final)
    # Caminho base: .../mann_whitney/seasons
    # Nome do arquivo alvo: mann_whitney_aggregated_results.csv
    # Nota: Usamos .parent pois seus paths apontam para 'plots', queremos a raiz 'seasons'
    seasons_base = paths.MANN_WHITNEY_PLOTS_DIR.parent
    analyze_directory(
        base_dir=seasons_base,
        result_filename="mann_whitney_aggregated_results.csv",
        analysis_name="Seasons (Rank Final)",
    )

    print("-" * 50)

    # 2. Análise para ATTENDANCE (Ocupação)
    # Caminho base: .../mann_whitney/attendance
    # Nome do arquivo alvo: mann_whitney_attendance_results.csv
    attendance_base = paths.MANN_WHITNEY_ATT_PLOTS_DIR.parent
    analyze_directory(
        base_dir=attendance_base,
        result_filename="mann_whitney_attendance_results.csv",
        analysis_name="Attendance (Ocupação)",
    )


if __name__ == "__main__":
    run_sensitivity_analysis()
