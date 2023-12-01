import configparser
import os


class AppConfig:
    DATABASE_URI = None
    PIPELINE_VERSION = None
    INPUT_DIRECTORY_PATH = None

    DURATION_IN_DAYS = None
    STATS_VERSION = None

    @classmethod
    def load_config(cls, config_file=None):
        script_path = os.path.abspath(__file__)
        if not config_file:
            config_dir = os.path.join(os.path.dirname(script_path), "..", "config")
            config_file = os.path.join(config_dir, "config.ini")

        config = configparser.ConfigParser()
        config.read(config_file)

        print(config_file)
        print(config)

        cls.PIPELINE_VERSION = config.get('ETL', 'pipeline_version')

        data_base_uri = config.get('ETL', 'database_uri', fallback=None)
        if data_base_uri:
            cls.DATABASE_URI = config.get('ETL', 'database_uri')
        else:
            database_name = config.get('ETL', 'database_name')
            database_file_path = os.path.abspath(
                os.path.join(os.path.dirname(script_path), "..", "database", database_name))
            cls.DATABASE_URI = f"sqlite:///{database_file_path}"

        input_directory_path = config.get('ETL', 'input_directory', fallback=None)
        if input_directory_path:
            cls.INPUT_DIRECTORY_PATH = config.get('ETL', 'input_directory', fallback=None)
        else:
            input_directory_name = config.get('ETL', 'input_directory_name')
            cls.INPUT_DIRECTORY_PATH = os.path.abspath(
                os.path.join(os.path.dirname(script_path), "..", input_directory_name))

        cls.DURATION_IN_DAYS = float(config.get('Statistics', 'duration_in_days'))
        cls.STATS_VERSION = config.get('Statistics', 'stats_version')

    @classmethod
    def print_config(cls):
        print(f"Database URI: {cls.DATABASE_URI}")
        print(f"Database URI: {cls.PIPELINE_VERSION}")
        print(f"Database URI: {cls.INPUT_DIRECTORY_PATH}")


AppConfig.load_config()
