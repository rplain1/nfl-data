import polars as pl
import duckdb
import logging

import configparser
from datetime import datetime

import os

CONFIG = {
    "DATABASE": {
        'SQLite' : 'data/base.base.sqlite',
        'DuckDB' : 'data/luna.luna.duckdb'
    }
}

class ETL:
    def __init__(self, config_file):
        self.config = config_file #self.load_config(config_file)
        self.raw_data = None
        self.transformed_data = None
        self.setup_logging()

    def load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def extract(self):
        """Extract data from SQLite database."""
        try:
            sqlite_connection_string = "sqlite://" + self.config['DATABASE']['SQLite']['PATH']
            table = self.config['DATABASE']['SQLite']['TABLE']
            self.raw_data = pl.read_database_uri(f"SELECT * FROM {table}", sqlite_connection_string)
            logging.info("Data extraction complete.")
        except Exception as e:
            logging.error(f"Error during extraction of DB {sqlite_connection_string}: {e}")

    def transform(self):
        """Transform the raw data."""
        try:
            # Example transformation: Add a timestamp column
            self.transformed_data = self.raw_data.with_columns(
                RUN_AT = datetime.now()
            )  # Placeholder for more complex transformations
            logging.info("Data transformation complete.")
        except Exception as e:
            logging.error(f"Error during transformation: {e}")

    def load(self):
        """Load transformed data into DuckDB."""
        try:
            duckdb_conn = duckdb.connect(self.config['DATABASE']['DuckDB']['PATH'])
            schema = self.config['DATABASE']['DuckDB']['SCHEMA']
            table = self.config['DATABASE']['DuckDB']['TABLE']

            duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            duckdb_conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            tmp = self.transformed_data.to_arrow()
            duckdb_conn.sql(
                f"""CREATE or REPLACE TABLE {schema}.{table} AS
                SELECT * FROM tmp"""
            )
            logging.info("Data loading complete.")
        except Exception as e:
            logging.error(f"Error during loading: {e}")
            print(e)
        finally:
            duckdb_conn.close()

    def run(self):
        """Run the ETL process."""
        self.extract()
        self.transform()
        self.load()



if __name__ == '__main__':
    config_file = {
        "DATABASE": {
            "SQLite": {
                "PATH": "data/pbp_db.sqlite",
                "TABLE": "nflfastR_pbp"
            },
            "DuckDB": {
                "PATH": "data/luna.duckdb",
                "SCHEMA": "BASE",
                "TABLE": 'NFLFASTR_PBP'
            },
        }
    }
    nflfastrETL = ETL(config_file=config_file)
    nflfastrETL.run()
