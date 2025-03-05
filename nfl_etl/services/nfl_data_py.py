from nfl_etl.services.globals import NFLVERSE_DATA_URL, LATEST_YEAR
import logging


class NFLread:
    def __init__(
        self, latest_year: int = LATEST_YEAR, base_url: str = NFLVERSE_DATA_URL
    ):
        self.latest_year = latest_year
        self.base_url = base_url

    @staticmethod
    def is_list_of_strings(lst: list) -> bool:
        """Check if list contains any string values"""
        return any(isinstance(item, str) for item in lst)

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

    def ftn_charting_file_list(
        self, years: int | list[int] | str | list[str]
    ) -> list[str]:
        """
        Build a list of URLs for ftn_charting nflverse data.

        Args:
            years: (int | str | list[int] | list[str]) years to build the list of

        Returns:
            list[str]: list of URLs for parquet files
        """
        years = self.build_years_list(years, min_year=2022)

        if all(x < 2022 for x in years):
            raise ValueError("FTN data must start on or after 2022")

        return [f"{self.base_url}/ftn_charting/ftn_charting_{x}.parquet" for x in years]
