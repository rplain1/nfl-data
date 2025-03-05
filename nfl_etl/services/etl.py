import polars as pl
import duckdb
import logging

import configparser
from datetime import datetime

import os

logging.basicConfig(
    level=logging.INFO,  # Minimum level of messages to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format of log messages
)


class ETL:
    def __init__(self, config_file):
        self.config = config_file  # self.load_config(config_file)
        self.raw_data = None
        self.transformed_data = None
        self.setup_logging()

    def load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def extract(self):
        """Extract data from SQLite database."""
        try:
            sqlite_connection_string = (
                "sqlite://" + self.config["DATABASE"]["SQLite"]["PATH"]
            )
            table = self.config["DATABASE"]["SQLite"]["TABLE"]
            self.raw_data = pl.read_database_uri(
                f"SELECT * FROM {table}", sqlite_connection_string
            )
            logging.info("Data extraction complete.")
        except Exception as e:
            logging.error(
                f"Error during extraction of DB {sqlite_connection_string}: {e}"
            )

    def transform(self):
        """Transform the raw data."""
        try:
            # Example transformation: Add a timestamp column
            self.transformed_data = self.raw_data.with_columns(
                RUN_AT=datetime.now()
            )  # Placeholder for more complex transformations
            logging.info("Data transformation complete.")
        except Exception as e:
            logging.error(f"Error during transformation: {e}")

    def load(self):
        """Load transformed data into DuckDB."""
        try:
            duckdb_conn = duckdb.connect(self.config["DATABASE"]["DuckDB"]["PATH"])
            schema = self.config["DATABASE"]["DuckDB"]["SCHEMA"]
            table = self.config["DATABASE"]["DuckDB"]["TABLE"]

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


def update_duckdb(con, url, table_name, schema="BASE"):
    sql = f"""
    CREATE OR REPLACE TABLE {schema}.{table_name.upper()} AS
    SELECT *, get_current_timestamp() AS updated_at
    FROM '{url}'
    """
    con.execute(sql)
    logging.info(f"Table {schema}.{table_name.upper()} updated successfully")


if __name__ == "__main__":
    config_file = {
        "DATABASE": {
            "SQLite": {"PATH": "data/pbp_db.sqlite", "TABLE": "nflfastR_pbp"},
            "DuckDB": {
                "PATH": "data/luna.duckdb",
                "SCHEMA": "BASE",
                "TABLE": "NFLFASTR_PBP",
            },
        }
    }
    # nflfastrETL = ETL(config_file=config_file)
    # nflfastrETL.run()

    con = duckdb.connect(os.getenv("DB_PATH"))
    files = [
        "players",
        "draft_picks",
        # "qbr_season_level",
        # "qbr_week_level",
        # "otc_players",
        # "officials",
        # "historical_contracts",
        # "otc_player_details",
        # "combine",
    ]

    # TODO: this can not handle different schemas in the same directory (or key)
    nflverse_data = {
        "ftn_charting": {"paths": ["ftn_charting"], "min_year": 2022},
        "espn_data": {
            "paths": ["qbr_season_level", "qbr_week_level"],
            "min_year": None,
        },
        "weekly_rosters": {"paths": ["roster_weekly"], "min_year": 2002},
        # 'players_components': {
        #     'paths': ['players', 'otc_players'],
        #     'min_year': None
        # },
        "players": {"paths": ["players"], "min_year": None},
        "officials": {"paths": ["officials"], "min_year": None},
        "draft_picks": {"paths": ["draft_picks"], "min_year": None},
        "contracts": {"paths": ["historical_contracts"], "min_year": None},
        "snap_counts": {"paths": ["snap_counts"], "min_year": 2012},
        "player_stats": {
            "paths": ["player_stats"],
            "min_year": 1999,
        },
        "nextgen_stats": {
            "paths": ["ngs_rushing"],
            "min_year": None,
        },
        "injuries": {"paths": ["injuries"], "min_year": 2009},
        "combine": {"paths": ["combine"], "min_year": None},
    }

    d = {}
    for data in nflverse_data.keys():
        min_year = None
        if "min_year" in nflverse_data[data].keys():
            min_year = nflverse_data[data].get("min_year")
        if min_year:
            paths = []
            for path in nflverse_data[data]["paths"]:
                for year in range(min_year, 2025):
                    _path = f"{path}_{year}"
                    paths.append(_path)
        else:
            paths = [x for x in nflverse_data[data]["paths"]]

        if "metadata" in nflverse_data[data].keys():
            new_paths = []
            for metadata in nflverse_data[data].get("metadata"):
                for _path in paths:
                    new_paths.append(f"{_path}_{metadata}")
            paths = new_paths

        d[data] = paths

    def load_nflreadr(
        con,
        table_name,
        paths,
        url="https://github.com/nflverse/nflverse-data/releases/download",
        schema="BASE",
    ):
        paths = [f"{url}/{table_name}/{x}.parquet" for x in paths]
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.{table_name.upper()} AS
        SELECT *, get_current_timestamp() AS updated_at
        FROM read_parquet({paths})
        """
        con.execute(sql)
        logging.info(f"Table {schema}.{table_name.upper()} updated successfully")

    for table in d.keys():
        if table != "nextgen_stats":
            continue
        load_nflreadr(con, table_name=table, paths=d[table])

    # for table_name in files:
    #     url = f"https://github.com/nflverse/nflverse-data/releases/download/{table_name}/{table_name}.parquet"
    #     update_duckdb(con=con, url=url, table_name=table_name)

    # files = [
    #     f"https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{x}.parquet"
    #     for x in range(2002, 2004)
    # ]
    # url = "https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.parquet"
    # update_duckdb(con, url, "DRAFT_PICKS")
    con.close()
