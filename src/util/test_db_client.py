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
    select_cleaned_readings = 'SELECT * FROM cleaned_reading ORDER BY turbine_id LIMIT 100'
    print("cleaned_reading:")
    connect_and_query(select_cleaned_readings)
    select_stats = ('SELECT * FROM statistics s '
                    'JOIN statistics_control sc ON s.statistics_control_id = sc.id ORDER BY turbine_id LIMIT 100')
    print("stats:")
    connect_and_query(select_stats)