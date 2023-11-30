from sqlalchemy import create_engine, text
from src.config import AppConfig


def connect_and_query(query):
    print(AppConfig.DATABASE_URI)
    engine = create_engine(AppConfig.DATABASE_URI, echo=True)

    with engine.connect() as connection:
        result = connection.execute(text(query))
        for row in result.fetchall():
            print(row)


if __name__ == "__main__":
    select_all = 'SELECT * FROM cleaned_reading ORDER BY turbine_id'
    connect_and_query(select_all)
