from nfl_etl.services.globals import NFLVERSE_DATA_URL, LATEST_YEAR
import logging


def is_list_of_strings(lst: list) -> bool:
    """Check if list contains any string values"""
    return any(isinstance(item, str) for item in lst)


def build_years_list(
    years: int | list[int] | str | list[str],
    latest_year: int = LATEST_YEAR,
    min_year: int | None = None,
) -> list:
    """
    Create a list of years that can be a single year, or a range of years,
    depending on the arguments passed. This will take a string, int, or list and
    return the appropriate range.

    Args:
        years: (int | str | list[int] | list[str]) years to build the list of
        latest_year: (int, optional) Max year of available data
        min_year: (int | None, optional) Min year of available data

    Returns:
        list: list of years for a desired range
    """

    # check if single value was passed instead of iterable
    if isinstance(years, int) or isinstance(years, str):
        years = [years]

    # convert string years
    if is_list_of_strings(years):
        try:
            years = [int(x) for x in years]
        except Exception as e:
            raise ValueError(f"An error occured casting to int {e}")

    # cant have future data
    if max(years) > LATEST_YEAR:
        raise ValueError(f"Year must be less than {latest_year}")

    # setup list of years to provide
    years = sorted(set(years))
    if years[-1] - years[0] > 0:
        years = [x for x in range(min(years), max(years) + 1)]

    # cant have data that never existed
    if min_year and min(years) < min_year:
        logging.warning(f"Data first available in {min_year}, removing prior years")
        years = [x for x in years if x >= min_year]

    return years


def ftn_charting_file_list(
    years: int | list[int] | str | list[str], url: str = NFLVERSE_DATA_URL
) -> list:
    """
    Build a list of urls for ftn_charting nflverse data.

    Args:
        years: (int | str | list[int] | list[str]) years to build the list of
        url: (str, optional) url path of ftn data

    Returns:
        list: list of urls for parquet files
    """
    years = build_years_list(years, min_year=2022)
    if all(x < 2022 for x in years):
        raise ValueError("FTN data must start on or after 2022")
    paths = [f"{url}/ftn_charting/ftn_charting_{x}.parquet" for x in years]

    return paths
