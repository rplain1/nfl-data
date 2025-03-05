import pytest
from nfl_etl.services.nfl_data_py import NFLread

nflread = NFLread()


@pytest.mark.parametrize(
    "input_year",
    [3100, "$312"],
)
def test_build_years_invalid(input_year):
    """Test that invalid years raise an error"""
    with pytest.raises(ValueError):  # Expect ValueError to be raised
        nflread.build_years_list(input_year)


@pytest.mark.parametrize(
    "input_year, expected",
    [
        (2023, [2023]),
        ([2023, 2024], [2023, 2024]),
        ([2022, 2024], [2022, 2023, 2024]),
        ([2022, 2020], [2020, 2021, 2022]),
        ([2022, 2022], [2022]),
        (["2022"], [2022]),
        ("2023", [2023]),
        (["2023", 2020], [2020, 2021, 2022, 2023]),
    ],
)
def test_build_years(input_year, expected):
    """
    Test all the comibinations of supported inputs
    """
    test = nflread.build_years_list(input_year)
    assert test == expected


@pytest.mark.parametrize(
    "input_year, expected",
    [
        ([2021, 2024], [2022, 2023, 2024]),
    ],
)
def test_build_years_minimum(input_year, expected):
    """
    Test that minimum years are filtered out
    """
    test = nflread.build_years_list(input_year, min_year=2022)
    assert test == expected


@pytest.mark.parametrize(
    "category, file, years, expected_len",
    [
        ("snap_counts", "snap_counts", None, 1),
        ("ftn_charting", "ftn_charting", [2022, 2024], 3),
    ],
)
def test_create_file_list(category, file, years, expected_len):
    test = nflread.create_file_list(category, file, years=years)
    assert len(test) == expected_len
