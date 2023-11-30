import sys
import os

script_path = os.path.abspath(__file__)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(script_path), "..")))

from src.database.database_schema import setup_db

if __name__ == "__main__":
    print("Creating DB")
    setup_db()
