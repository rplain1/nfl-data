NFLVERSE_DATA_URL = "https://github.com/nflverse/nflverse-data/releases/download"
LATEST_YEAR = 2024


def build_years_list(years, latest_year=LATEST_YEAR):
    if isinstance(years, int):
        years = [years]

    if max(years) > LATEST_YEAR:
        raise ValueError(f"Year must be less than {latest_year}")
    years = sorted(set(years))
    if years[-1] - years[0] > 0:
        years = [x for x in range(min(years), max(years) + 1)]

    return years


def ftn_charting_file_list(years, url=NFLVERSE_DATA_URL):
    assert min(years) >= 2022, "FTN data began in 2022"
    years = build_years_list(years)
    paths = [f"{url}/ftn_charting/ftn_charting_{x}.parquet" for x in years]

    return paths


if __name__ == "__main__":
    print(build_years_list(2023))
