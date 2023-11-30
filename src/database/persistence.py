from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from src.database.database_schema import LoadControlEntity, CleanedReadingEntity
from src.config import AppConfig
from src.model.model import LoadControl


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

                print(f"Latest load_control: ID: {load_control.id}, Pipeline Version: {load_control.pipeline_version}, "
                      f"Input File Name: {load_control.input_file_name}, "
                      f"Last Loaded Line Number: {load_control.last_loaded_line_number}, "
                      f"Load Timestamp: {load_control.load_timestamp}")
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
            print(cleaned_df.head())
            cleaned_df.to_sql(CleanedReadingEntity.__tablename__, con=self.engine, if_exists='append', index=False,
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


# Example usage
if __name__ == "__main__":
    db_manager = DatabaseManager(AppConfig.DATABASE_URI)

    load_control_latest = db_manager.fetch_latest_load_control('data_group_1.csv', 1)

    if load_control_latest:
        print("row: " + str(type(load_control_latest)))
        print(f"ID: {load_control_latest.id}, Pipeline Version: {load_control_latest.pipeline_version}, "
              f"Input File Name: {load_control_latest.input_file_name}, "
              f"Last Loaded Line Number: {load_control_latest.last_loaded_line_number}, "
              f"Load Timestamp: {load_control_latest.load_timestamp}")
