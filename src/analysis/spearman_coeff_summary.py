# File: src/analysis/spearman_coeff_summary.py
"""
Generates summary tables from analysis results.
"""
import logging
import pandas as pd
from src.utils import paths

logger = logging.getLogger(__name__)


def create_g_type_summary():
    """
    Lê o arquivo strength_schedule_balance.csv e cria uma tabela de resumo.

    A tabela de resumo agrupa os dados por liga, época e G_type, calculando
    o número de amostras e o valor médio de 'G' para vários níveis de agregação.
    O resultado é salvo em um novo arquivo CSV.
    """
    logger.info("Iniciando a criação do resumo de G_type.")

    try:
        input_file = paths.SPEARMAN_COEFFICIENT / "strength_schedule_balance.csv"
        output_file = paths.SPEARMAN_COEFFICIENT / "strength_schedule_summary.csv"

        df = pd.read_csv(input_file)
        logger.info(
            f"Arquivo '{input_file.name}' carregado com sucesso com {len(df)} linhas."
        )

    except FileNotFoundError:
        logger.error(
            f"Arquivo de entrada não encontrado: {input_file}. Não é possível criar o resumo."
        )
        return

    # 1. Agrupamento por league_name, season_year e G_type
    agg_season = (
        df.groupby(["league_name", "season_year", "G_type"])
        .agg(n_samples=("G", "count"), G_avg=("G", "mean"))
        .reset_index()
    )

    # 2. Agrupamento por league_name (englobando todos os anos)
    agg_league = (
        df.groupby(["league_name", "G_type"])
        .agg(n_samples=("G", "count"), G_avg=("G", "mean"))
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"

    # 3. Agrupamento geral (todas as ligas e épocas)
    agg_all = (
        df.groupby("G_type")
        .agg(n_samples=("G", "count"), G_avg=("G", "mean"))
        .reset_index()
    )
    agg_all["league_name"] = "all_leagues"
    agg_all["season_year"] = "all_seasons"

    # 4. Concatenar os três DataFrames de agregação
    summary_df = pd.concat([agg_season, agg_league, agg_all], ignore_index=True)

    # 5. Reordenar colunas para o formato final desejado
    final_columns = ["league_name", "season_year", "G_type", "n_samples", "G_avg"]
    summary_df = summary_df[final_columns]

    # 6. Ordenar os resultados para melhor visualização
    summary_df = summary_df.sort_values(
        by=["league_name", "season_year", "G_type"]
    ).reset_index(drop=True)

    # 7. Salvar a tabela de resumo em um novo arquivo CSV
    summary_df.to_csv(output_file, index=False, float_format="%.4f")
    logger.info(
        f"Tabela de resumo com {len(summary_df)} linhas salva em '{output_file.name}'."
    )
