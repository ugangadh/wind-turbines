import sys
import os

script_path = os.path.abspath(__file__)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(script_path), "..")))

from src.config import AppConfig

from src.pipeline.etl import trigger_etl

if __name__ == "__main__":
    print(f"Database URI: {AppConfig.DATABASE_URI}")
    print(f"Pipeline version URI: {AppConfig.PIPELINE_VERSION}")
    print(f"Input directory path: {AppConfig.INPUT_DIRECTORY_PATH}")

    print("Triggering ETL")
    trigger_etl()
