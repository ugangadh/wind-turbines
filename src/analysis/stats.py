import pandas as pd
from src.config import AppConfig
from src.database.persistence import DatabaseManager


def trigger_summary_stats_creation():
    pass


def summary_stats(from_date, to_date):
    # Load data from db for this time period
    # Calculate summary stats for this time period
    # Store the stats along with an entry into the statistic_control table for this period
    pass


def calculate_stats(df):
    pass


if __name__ == "__main__":
    trigger_summary_stats_creation()
