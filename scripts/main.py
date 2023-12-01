import sys
import os

script_path = os.path.abspath(__file__)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(script_path), "..")))

from src.config import AppConfig
from src.pipeline.etl import trigger_etl
from src.analysis.stats import trigger_summary_stats_creation

if __name__ == "__main__":
    print(f"Database URI: {AppConfig.DATABASE_URI}")
    print(f"Pipeline version URI: {AppConfig.PIPELINE_VERSION}")
    print(f"Input directory path: {AppConfig.INPUT_DIRECTORY_PATH}")

    print("Triggering ETL")
    trigger_etl()
    print("ETL Complete")

    print("Triggering Summary Stats Creation")
    trigger_summary_stats_creation()
    print("Summary Stats Creation Complete")
