from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

from src.config import AppConfig

Base = declarative_base()


class LoadControlEntity(Base):
    __tablename__ = 'load_control'
    id = Column(Integer, primary_key=True)
    pipeline_version = Column(Integer)
    input_file_name = Column(String)
    last_loaded_line_number = Column(Integer)
    load_timestamp = Column(DateTime, default=datetime.now)


class CleaningStatisticsEntity(Base):
    __tablename__ = 'cleaning_statistics'
    id = Column(Integer, primary_key=True)
    load_id = Column(Integer, ForeignKey('load_control.id'))
    turbine_id = Column(Integer)
    action = Column(String)
    reason = Column(String)
    count = Column(Integer)


class StatisticsControlEntity(Base):
    __tablename__ = 'statistics_control'
    id = Column(Integer, primary_key=True)
    from_date = Column(DateTime)
    to_date = Column(DateTime)
    pipeline_version = Column(Integer)
    last_processed_load_control_id = Column(Integer, ForeignKey('load_control.id'))


class CleanedReadingEntity(Base):
    __tablename__ = 'cleaned_reading'
    id = Column(Integer, primary_key=True)
    load_id = Column(Integer, ForeignKey('load_control.id'))
    turbine_id = Column(Integer)
    timestamp = Column(DateTime)
    wind_speed = Column(Float)
    wind_direction = Column(Float)
    power_output = Column(Float)
    is_imputed = Column(Boolean)


class StatisticsEntity(Base):
    __tablename__ = 'statistics'
    id = Column(Integer, primary_key=True)
    statistics_control_id = Column(Integer, ForeignKey('statistics_control.id'))
    turbine_id = Column(Integer)
    min_power = Column(Float)
    max_power = Column(Float)
    average = Column(Float)
    std_deviation = Column(Float)
    has_anomaly_reading = Column(Boolean)


def create_tables(engine):
    Base.metadata.create_all(engine)


def setup_db():
    engine = create_engine(AppConfig.DATABASE_URI, echo=True)
    create_tables(engine)


if __name__ == "__main__":
    print(AppConfig.DATABASE_URI)
    setup_db()
