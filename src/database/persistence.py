from datetime import datetime

from sqlalchemy import create_engine, desc, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import pandas as pd

from src.database.database_schema import LoadControlEntity, CleanedReadingEntity, StatisticsControlEntity, \
    StatisticsEntity
from src.config import AppConfig
from src.model.model import LoadControl, StatisticsControl


def singleton(cls):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class DatabaseManager:
    def __init__(self, database_uri):
        self.engine = create_engine(database_uri)
        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def session_scope(self, current_session):
        try:
            yield current_session
            current_session.commit()
        except Exception as e:
            print(e)
            current_session.rollback()
            raise
        finally:
            current_session.close()

    def insert_load_control(self, load_control):
        session_factory = self.Session
        new_session = session_factory()

        with self.session_scope(new_session) as session:
            session.add(load_control_to_entity(load_control))

    def fetch_latest_load_control(self, input_file_name, pipeline_version):
        session_factory = self.Session
        new_session = session_factory()

        load_control = None
        with (self.session_scope(new_session)) as session:
            result = session.query(LoadControlEntity).filter_by(input_file_name=input_file_name,
                                                                pipeline_version=pipeline_version
                                                                ).order_by(desc(LoadControlEntity.id)).limit(1)

            row = result.first()
            if row:
                load_control = entity_to_load_control(row)

                '''
                print(f"Latest load_control: ID: {load_control.id}, Pipeline Version: {load_control.pipeline_version}, "
                      f"Input File Name: {load_control.input_file_name}, "
                      f"Last Loaded Line Number: {load_control.last_loaded_line_number}, "
                      f"Load Timestamp: {load_control.load_timestamp}")
                '''
        return load_control

    def load_cleaned_data(self, pipeline_version, cleaned_df, file_name, last_loaded_line_number):
        session_factory = self.Session
        new_session = session_factory()

        with self.session_scope(new_session) as session:
            load_control_entity = LoadControlEntity(
                pipeline_version=pipeline_version,
                input_file_name=file_name,
                last_loaded_line_number=last_loaded_line_number
            )
            session.add(load_control_entity)
            session.commit()
            load_id = load_control_entity.id
            cleaned_df['load_id'] = load_id
            cleaned_df.to_sql(CleanedReadingEntity.__tablename__, con=self.engine, if_exists='append', index=False,
                              method='multi', chunksize=1000)

    def fetch_latest_statistics_control(self, pipeline_version, stats_version):
        session_factory = self.Session
        new_session = session_factory()

        stats_control = None
        with ((self.session_scope(new_session)) as session):
            result = session.query(StatisticsControlEntity).filter_by(pipeline_version=pipeline_version,
                                                                      stats_version=stats_version
                                                                      ).order_by(
                desc(StatisticsControlEntity.id)).limit(1)

            row = result.first()
            if row:
                stats_control = entity_to_stats_control(row)

        return stats_control

    def fetch_min_max_cleaned_readings_timestamp(self, pipeline_version):
        with self.engine.connect() as connection:
            query_to_fetch_min_max = text("""
                SELECT
                    MIN(cr.timestamp) AS min_timestamp,
                    MAX(cr.timestamp) AS max_timestamp
                FROM cleaned_reading cr
                JOIN load_control lc ON cr.load_id = lc.id
                WHERE lc.pipeline_version = :pipeline_version
            """)
            result = connection.execute(query_to_fetch_min_max, {'pipeline_version': pipeline_version})
            row = result.first()

            return tuple(map_to_timestamp(value) for value in row)

    def fetch_cleaned_readings(self, pipeline_version, from_date, to_date):
        with self.engine.connect() as connection:
            query = text("""
                    SELECT
                        cr.id,
                        cr.load_id,
                        cr.turbine_id,
                        cr.timestamp,
                        cr.wind_speed,
                        cr.wind_direction,
                        cr.power_output,
                        cr.is_imputed
                    FROM cleaned_reading cr
                    JOIN load_control lc ON cr.load_id = lc.id
                    WHERE cr.timestamp >= :from_date
                        AND cr.timestamp < :to_date
                        AND lc.pipeline_version = :pipeline_version
                """)

            # Execute the query and fetch the result as a pandas DataFrame
            result_df = pd.read_sql_query(query, self.engine, params={'from_date': from_date, 'to_date': to_date,
                                                                      'pipeline_version': pipeline_version})

            return result_df

    def store_stats(self, pipeline_version, stats_version, from_date, to_date, stats_df):
        session_factory = self.Session
        new_session = session_factory()

        with self.session_scope(new_session) as session:
            stats_control_entity = StatisticsControlEntity(
                pipeline_version=pipeline_version,
                stats_version=stats_version,
                from_date=from_date,
                to_date=to_date
            )
            session.add(stats_control_entity)
            session.commit()
            stats_id = stats_control_entity.id
            stats_df['statistics_control_id'] = stats_id
            stats_df.to_sql(StatisticsEntity.__tablename__, con=self.engine, if_exists='append', index=False,
                            method='multi', chunksize=1000)


def load_control_to_entity(load_control):
    return LoadControlEntity(
        id=load_control.id,
        pipeline_version=load_control.pipeline_version,
        input_file_name=load_control.input_file_name,
        last_loaded_line_number=load_control.last_loaded_line_number,
        load_timestamp=load_control.load_timestamp
    )


def entity_to_load_control(load_control_entity):
    return LoadControl(
        id=load_control_entity.id,
        pipeline_version=load_control_entity.pipeline_version,
        input_file_name=load_control_entity.input_file_name,
        last_loaded_line_number=load_control_entity.last_loaded_line_number,
        load_timestamp=load_control_entity.load_timestamp
    )


def stats_control_to_entity(stats_control):
    return StatisticsControlEntity(
        id=stats_control.id,
        pipeline_version=stats_control.pipeline_version,
        stats_version=stats_control.stats_version,
        from_date=stats_control.from_date,
        to_date=stats_control.to_date
    )


def entity_to_stats_control(stats_control_entity):
    return StatisticsControl(
        id=stats_control_entity.id,
        pipeline_version=stats_control_entity.pipeline_version,
        stats_version=stats_control_entity.stats_version,
        from_date=stats_control_entity.from_date,
        to_date=stats_control_entity.to_date
    )


def map_to_timestamp(time_in_str):
    return datetime.strptime(time_in_str, "%Y-%m-%d %H:%M:%S.%f")


# TODO For testing, remove later
if __name__ == "__main__":
    db_manager = DatabaseManager(AppConfig.DATABASE_URI)

    load_control_latest = db_manager.fetch_latest_load_control('data_group_1.csv', 1)

    if load_control_latest:
        print("row: " + str(type(load_control_latest)))
        print(f"ID: {load_control_latest.id}, Pipeline Version: {load_control_latest.pipeline_version}, "
              f"Input File Name: {load_control_latest.input_file_name}, "
              f"Last Loaded Line Number: {load_control_latest.last_loaded_line_number}, "
              f"Load Timestamp: {load_control_latest.load_timestamp}")
