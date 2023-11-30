import os
import pandas as pd
from src.config import AppConfig
from src.database.persistence import DatabaseManager


def clean_data(df):
    # Convert 'timestamp' column to datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    # Drop rows with invalid timestamp
    df = df.dropna(subset=['timestamp'])

    # Convert numeric columns
    numeric_columns = ['turbine_id', 'wind_speed', 'wind_direction', 'power_output']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    # Drop rows with invalid turbine id
    df = df.dropna(subset=['turbine_id'], how='any', axis=0)
    df['turbine_id'] = df['turbine_id'].astype(int)

    # Impute any missing data in wind_speed, wind_direction & power_output
    # If data is missing, because this is time series data, it makes sense to use the same value for the
    # previous reading for the same turbine
    # For eg: If wind_direction is missing for turbine 5 at 09:00:00, then it makes sense to impute the missing
    # field with value from the next reading for turbine 5 at 10:00:00
    # For this to work the data for the same turbines
    df = df.sort_values(by='turbine_id')
    df = df.bfill()

    # Drop any remaining rows that are unfilled
    df = df.dropna()

    df = df.astype({
        "timestamp": "datetime64[ns]",
        "turbine_id": "int64",
        "wind_speed": "float64",
        "wind_direction": "int64",
        "power_output": "float64",
    })

    # Validate 'wind_speed', wind_direction & power_output values within the specified range(which can be set &
    # retrieved from config)
    valid_wind_speed = (df['wind_speed'] >= 0.00) & (df['wind_speed'] <= 100.00)
    valid_wind_direction = (df['wind_direction'] >= 0) & (df['wind_direction'] <= 360)
    valid_power_output = (df['power_output'] >= 0.00) & (df['power_output'] <= 50.00)

    # Filter the DataFrame to include only rows with valid 'wind_speed'
    df = df[valid_wind_speed & valid_wind_direction & valid_power_output]

    return df


def do_etl(db_manager, input_file_name, pipeline_version, file_path, chunk_size=5):
    while True:
        load_control_latest = db_manager.fetch_latest_load_control(input_file_name, pipeline_version)

        start_row = 1
        if load_control_latest:
            start_row = load_control_latest.last_loaded_line_number + 1

        df = pd.read_csv(file_path, skiprows=list(range(1, start_row)), nrows=chunk_size)
        total_rows = len(df)

        if total_rows < 1:
            break

        print(df.head())
        print(len(df))

        cleaned_df = clean_data(df)

        last_processed_line_number = start_row + total_rows - 1
        db_manager.load_cleaned_data(pipeline_version, cleaned_df, input_file_name, last_processed_line_number)

        print(f"Processed {input_file_name} with rows from {start_row} to {last_processed_line_number}")


def files_in_input_directory(directory_path):
    try:
        return [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    except FileNotFoundError:
        print(f"Error: Directory '{directory_path}' not found.")


def trigger_etl():
    file_names = files_in_input_directory(AppConfig.INPUT_DIRECTORY_PATH)

    print(f"Files in directory '{AppConfig.INPUT_DIRECTORY_PATH}':")

    for file_name in file_names:
        print(f"CSV {file_name}")
        do_etl(DatabaseManager(AppConfig.DATABASE_URI), file_name, AppConfig.PIPELINE_VERSION,
               os.path.join(AppConfig.INPUT_DIRECTORY_PATH, file_name))


if __name__ == "__main__":
    trigger_etl()
