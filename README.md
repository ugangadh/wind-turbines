# wind-turbines

# Installation steps

1. Clone the repository
2. Create a virtual environment called - `python3 -m venv venv`
3. Activate the environment - `source venv/bin/activate`
4. Install the requirements - `pip install -r requirements.txt`
5. One time set-up of database - `python3 ./scripts/setup_database.py`
6. Run unit test - `python3 -m unittest discover tests`
7. Add input files to the default directory - `input`
8. Run code `python3 ./scripts/main.py`
9. Set up a cron job to run the script every day

Python version: `3.9.6`

# Description

Implemented a data pipeline to process turbine power output data. The pipeline performs two jobs ETL & statistics generation.
Each job can be run independently of each other and are comprised of the following tasks:

1. ETL
- Read data from CSVs stored in a configured input location
- Clean the data using Pandas library
- Load the cleaned readings into a database using SQLAlchemy. The database used for testing is SQLite.

2. Stats
- Read the cleaned readings from the database
- Use Pandas to compute and store statistics to calculate min, max, average, standard deviation & identify anomalies

# ETL

The ETL is orchestrated using the *load_control* table:

*pipeline_version* : This column of the load_control table is used to version the pipeline data. The version is specified 
in the config and stored along with the data. It can be used to trigger re-processing of the data from start. It can
be used for any of the following reasons.
- If there is a change in logic of the processing code
- If there is change in data format
- If files were corrupted and need to be reprocessed from start

When the version is updated in the config file the pipeline will start processing data from the start and catches up. The previously
processed data is still kept in the database.

The files are processed in configured chunks and *last_loaded_line_number* to keep track of the processing.

********load_control********

| id | pipeline_version | input_file_name | last_loaded_line_number | load_timestamp |
| --- | --- | --- | --- | --- |
| 1 | 1 | data_group_1.csv | 100 | 2022-03-01 00:00:00 |
| 2 | 1 | data_group_2.csv | 200 | 2022-03-01 00:00:00 |
| 3 | 2 | data_group_2.csv | 200 | 2022-03-01 00:00:00 |

The cleaning statistics table is currently not implemented but will be good to have some idea about the quality of data.
**cleaning_statistics(optional)**

| id | load_id | turbine_id | action | reason | count |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 2 | IMPUTED | MISSING_DATE_TIME | 200 |
| 2 | 1 | 3 | DROPPED | INVALID_DATE_TIME | 100 |
| 3 | 1 | null | DROPPED | INVALID_TURBINE_ID | 200 |

**cleaned_reading**

| id | load_id | turbine_id | timestamp | wind_speed | wind_direction | power_output | is_imputed(optional) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 2022-03-01 00:00:00 | 11.8 | 169 | 2.7 |  |
| 2 | 1 | 2 | 2022-03-01 00:00:00 | 11.6 | 24 | 2.2 |  |
| 3 | 2 | 1 | 2022-03-01 00:00:00 | 11.8 | 169 | 2.7 |  |

# Stats

The Stats generation is orchestrated using the *statistics_control* table:

This keeps track of the stats generated for the configured interval.

*stats_version* : This column of the statistics_control table is used to version the stats data. The version is specified in
the config and can be used to re-compute stats for the same version of pipeline data or to recompute for a different time period.

Stats can also be created as one-off for a custom time period as long as the data could be held in memory.

**statistics_control**

| id | from_date | to_date | pipeline_version | stats_version |
| --- | --- | --- | --- | --- |
| 1 | 2022-03-01 00:00:00 | 2022-03-02 00:00:00 | 1 | 1 |
| 2 | 2022-03-01 00:00:00 | 2022-03-01 00:00:00 | 1 | 1 |
| 3 | 2022-03-01 00:00:00 | 2022-03-01 00:00:00 | 1 | 2 |

**statistics**

| id | statistics_control_id | turbine_id | min_power | max_power | average | has_anomaly_reading | std_deviation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 2 | 2.3 | 4.3 | 3.8 | True | 2.1 |
| 2 | 1 | 3 | 2.3 | 3.3 | 2.8 | False | 1.1 |

# Assumptions

1. The CSV would have a consistent format as in the headers would remain the same & the number of values in the data rows does not change
2. The CSV will not be updated in places ie the data will only be appended to the end and existing data will not be modified between runs
3. The units of the values are all consistent
4. The wind_speed will be in the range (0.00, 100.00)
5. The wind_direction will be in the range (0, 360) and only contains whole degrees
6. The wind_output is in the range (0.00, 50.00)

# Future changes

1. Use a proper database instead of SQLLite
2. Create appropriate indexes & constraints
3. Implement handling for ‘On Conflict’ clause to idempotently process data 
4. Implement `cleaning_statistics` table to capture the summary of cleaning actions done on the raw data
5. Implement `is_imputed` in the `cleaned_readings` to specify if the data records has been imputed
6. Use a migration tool like `liquibase` or `flyway` for handling database changes
7. Use proper logging
8. Implement end-to-end tests
9. Consider using Spark if the data that needs to be processed becomes too big to be held in memory of a singe machine
10. Currently, as part of the pipeline, stats can be generated on only one set of time period, 
    - If stats is required for a different period the config needs to be updated for the new period
    - Add support for configuring and generating stats for multiple periods as part of the same job
