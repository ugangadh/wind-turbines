from datetime import timedelta

import pandas as pd
from src.config import AppConfig
from src.database.persistence import DatabaseManager


def trigger_summary_stats_creation():
    db_manager = DatabaseManager(AppConfig.DATABASE_URI)

    # Find the min and max value of timestamp in the cleaned readings
    cleaned_readings_min_time, cleaned_readings_max_time = db_manager.fetch_min_max_cleaned_readings_timestamp(
        AppConfig.PIPELINE_VERSION)

    print(f"Min timestamp : {cleaned_readings_min_time} \nMax timestamp : {cleaned_readings_max_time}")

    if not cleaned_readings_min_time:
        print(f"Not running stats as no cleaned readings available for Pipeline version : {AppConfig.PIPELINE_VERSION}")
        return

    initial_date = cleaned_readings_min_time.replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        # For the current pipeline version & stats version get the latest entry in the table
        stats_control = db_manager.fetch_latest_statistics_control(AppConfig.PIPELINE_VERSION, AppConfig.STATS_VERSION)

        new_from_date = initial_date
        if stats_control:
            new_from_date = stats_control.to_date

        # We only want to run the stats if data is available for the new window
        if new_from_date > cleaned_readings_max_time:
            break

        new_to_date = new_from_date + timedelta(days=AppConfig.DURATION_IN_DAYS)

        # Fetch a dataframe of rows to do the stats
        df = db_manager.fetch_cleaned_readings(AppConfig.PIPELINE_VERSION, new_from_date, new_to_date)

        stats_df = calculate_stats(df)

        # store the stats along with an entry into the statistic_control table for this period
        db_manager.store_stats(AppConfig.PIPELINE_VERSION, AppConfig.STATS_VERSION, new_from_date, new_to_date,
                               stats_df)

        print(
            f"Created stats for duration from {new_from_date} to {new_to_date} "
            f"for Pipeline version {AppConfig.PIPELINE_VERSION} and Stats version {AppConfig.STATS_VERSION}")


def calculate_stats(df):
    df = df.astype({
        "timestamp": "datetime64[ns]",
        "turbine_id": "int64",
        "wind_speed": "float64",
        "wind_direction": "int64",
        "power_output": "float64",
    })

    # Calculate min, max, mean & std for each turbine
    stats = df.groupby('turbine_id').agg(
        min_power=("power_output", "min"),
        max_power=("power_output", "max"),
        average=("power_output", "mean"),
        std_deviation=("power_output", "std")
    ).reset_index()

    # Join stats with the cleaned_readings data frame
    df = pd.merge(df, stats, on='turbine_id')

    # Calculate z-scores for each power output reading
    df['z-score'] = (df['power_output'] - df['average']) / df['std_deviation']
    df['has_anomaly'] = abs((df['power_output'] - df['average']) / df['std_deviation']) > 2

    # Reduce it back to final stats with the anomaly identification column
    final_stats = df.groupby('turbine_id').agg(
        min_power=("min_power", agg_first_value),
        max_power=("max_power", agg_first_value),
        average=("average", agg_first_value),
        has_anomaly_reading=("has_anomaly", agg_any_true)
    ).reset_index()

    final_stats = final_stats.round(2)

    return final_stats


def agg_first_value(series):
    return series.iloc[0] if not series.empty else None


def agg_any_true(series):
    return any(series)


if __name__ == "__main__":
    trigger_summary_stats_creation()
