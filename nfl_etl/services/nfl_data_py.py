import logging
import os
from datetime import datetime

import duckdb

from nfl_etl.services.global_vars import LATEST_YEAR, NFLVERSE_DATA_URL

logging.basicConfig(
    level=logging.INFO,  # Minimum level of messages to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format of log messages
)


class NFLread:
    def __init__(self, base_url: str = NFLVERSE_DATA_URL):
        self.latest_year = self.most_recent_season()
        self.base_url = base_url

    @staticmethod
    def is_list_of_strings(lst: list) -> bool:
        """Check if list contains any string values"""
        return any(isinstance(item, str) for item in lst)

    def most_recent_season(self):
        """
        Return the latest played NFL season
        """
        today = datetime.today()
        year = today.year
        # if before september, use previous year
        # TODO: add edge case handling
        if today.month < 9:
            year -= 1

        return year

    def build_years_list(
        self,
        years: int | list[int] | str | list[str],
        min_year: int | None = None,
    ) -> list[int]:
        """
        Create a list of years that can be a single year, or a range of years.

        Args:
            years: (int | str | list[int] | list[str]) years to build the list of
            min_year: (int | None, optional) Min year of available data

        Returns:
            list[int]: list of years for a desired range
        """
        if isinstance(years, (int, str)):
            years = [years]

        # Convert string years
        if self.is_list_of_strings(years):
            try:
                years = [int(x) for x in years]
            except ValueError as e:
                raise ValueError(f"An error occurred casting to int: {e}")

        # Check for future data
        if max(years) > self.latest_year:
            raise ValueError(f"Year must be less than {self.latest_year}")

        # Create range of years if needed
        years = sorted(set(years))
        if years[-1] - years[0] > 0:
            years = list(range(min(years), max(years) + 1))

        # Remove years before `min_year` if specified
        if min_year and min(years) < min_year:
            logging.warning(f"Data first available in {min_year}, removing prior years")
            years = [x for x in years if x >= min_year]

        return years

    def create_file_list(
        self,
        category: str,
        file: str,
        years: int | list[int] | str | list[str] | None = None,
        min_year: int | None = None,
        filetype: str = "parquet",
    ) -> list[str]:
        """
        Build a list of URLs for nflverse data.

        Args:
            file_type (str): The type of file to generate URLs for (e.g., 'ftn_charting', 'snap_counts').
            years (int | str | list[int] | list[str]): Years to build the list of.
            min_year (int): Minimum valid year for this dataset.

        Returns:
            list[str]: List of URLs for parquet files.
        """

        if years is None:
            return [f"{self.base_url}/{category}/{file}.{filetype}"]

        years = self.build_years_list(years, min_year=min_year)

        if min_year and all(x < min_year for x in years):
            raise ValueError(f"{file} data must start on or after {min_year}")

        return [f"{self.base_url}/{category}/{file}_{x}.{filetype}" for x in years]

    def load_nflreadr(
        self,
        con: duckdb.DuckDBPyConnection,
        category: str,
        file: str,
        table_name: str,
        years=None,
        min_year=None,
        schema="BASE",
    ):
        paths = self.create_file_list(
            category=category, file=file, years=years, min_year=min_year
        )
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.{table_name.upper()} AS
        SELECT *, get_current_timestamp() AS updated_at
        FROM read_parquet({paths})
        """
        con.execute(sql)
        logging.info(f"Table {schema}.{table_name.upper()} updated successfully")


if __name__ == "__main__":
    con = duckdb.connect(os.getenv("DB_PATH"))
    print(NFLVERSE_DATA_URL)
    nflread = NFLread()
    nflread.load_nflreadr(
        con,
        "ftn_charting",
        "ftn_charting",
        table_name="FTN_CHARTING",
        years=[2022, LATEST_YEAR],
    )
    con.close()
