# File: src/pre_processor/main.py
"""
Orchestrates the entire data pre-processing workflow.
"""
import logging
import pandas as pd
from src.pre_processor import config
from src.pre_processor import processors

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    config.configure_logging()

    # Step 1: Process standings data
    unified_standings_df = processors.process_standings_data()

    if not unified_standings_df.empty:
        # Step 2: Process schedules data and get attendance summary
        unified_schedules_df, attendance_summary_df = processors.process_schedules_data(
            unified_standings_df
        )

        # Step 3: Merge standings with attendance summary to get final processed standings table
        final_standings_df = pd.merge(
            unified_standings_df,
            attendance_summary_df,
            on=["league_name", "season_year"],
            how="left",
        )

        # Fill NaN for seasons that were not processed
        final_standings_df["is_valid_attendance"] = (
            final_standings_df["is_valid_attendance"].fillna(False).astype(bool)
        )
        final_standings_df["num_total_games"] = (
            final_standings_df["num_total_games"].fillna(0).astype(int)
        )
        final_standings_df["num_null_attendance_games"] = (
            final_standings_df["num_null_attendance_games"].fillna(0).astype(int)
        )
        final_standings_df["pct_null_attendance_games"] = final_standings_df[
            "pct_null_attendance_games"
        ].fillna(100.0)
        final_standings_df["attendance_total_games"] = final_standings_df[
            "attendance_total_games"
        ].fillna(0)
        final_standings_df["attendance_total_games_filled"] = final_standings_df[
            "attendance_total_games_filled"
        ].fillna(0)

        # Save the final processed tables

        ordered_list = [
            "id",
            "league_name",
            "season_year",
            "team",
            "team_sanitized",
            "position",
            "played",
            "won",
            "drawn",
            "lost",
            "goal_difference",
            "goal_ratio",
            "points",
            "num_total_teams",
            "num_total_games",
            "num_null_attendance_games",
            "pct_null_attendance_games",
            "attendance_total_games",
            "attendance_total_games_filled",
            "is_valid_attendance",
            "is_double_rounded",
            "is_valid_url",
            "team_url",
            "csv_file_name",
        ]

        final_standings_df = final_standings_df.drop(columns=["csv_file_name_y"])
        final_standings_df = final_standings_df.rename(
            columns={"csv_file_name_x": "csv_file_name"}
        )
        final_standings_df = final_standings_df[ordered_list]
        final_standings_df.to_csv(config.STANDINGS_CONCAT_CSV, index=False)
        attendance_summary_df.to_csv(config.ATTENDANCE_CONCAT_CSV, index=False)

        logger.info(
            f"Final standings table saved to: {config.STANDINGS_CONCAT_CSV.relative_to(config.BASE_DIR)}"
        )

        ordered_list = [
            "id",
            "league_name",
            "season_year",
            "round",
            "date",
            "time",
            "datetime_str",
            "datetime",
            "home_team",
            "home_team_sanitized",
            "away_team",
            "away_team_sanitized",
            "formation",
            "coach",
            "result",
            "audience",
            "audience_filled",
            "csv_file_name",
            "match_link",
        ]

        unified_schedules_df = unified_schedules_df[ordered_list]
        unified_schedules_df.to_csv(config.SCHEDULES_CONCAT_CSV, index=False)
        logger.info(
            f"Final schedules table saved to: {config.SCHEDULES_CONCAT_CSV.relative_to(config.BASE_DIR)}"
        )

        # Step 4: Generate the final JSON with combined validity flags
        processors.generate_final_json(final_standings_df)

    logger.info("Data pre-processing workflow complete.")
