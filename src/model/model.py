from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class LoadControl:
    pipeline_version: int
    input_file_name: str
    last_loaded_line_number: int
    id: Optional[int] = None
    load_timestamp: Optional[datetime] = None


@dataclass(frozen=True)
class CleaningStatistics:
    id: int
    load_id: int
    turbine_id: int
    action: str
    reason: str
    count: int


@dataclass(frozen=True)
class StatisticsControl:
    id: int
    from_date: datetime
    to_date: datetime
    pipeline_version: int
    stats_version: int


@dataclass(frozen=True)
class CleanedReading:
    id: int
    load_id: int
    turbine_id: int
    timestamp: datetime
    wind_speed: float
    wind_direction: float
    power_output: float
    is_imputed: bool


@dataclass(frozen=True)
class Statistics:
    id: int
    statistics_control_id: int
    turbine_id: int
    min_power: float
    max_power: float
    average: float
    std_deviation: float
    has_anomaly_reading: bool
