import pytest
from nfl_etl.services.nfl_data_py import build_years_list


@pytest.mark.parametrize(
    "input_year",
    [3100],  # Test with invalid years like 0 and -1
)
def test_build_years_invalid(input_year):
    with pytest.raises(ValueError):  # Expect ValueError to be raised
        build_years_list(input_year)


@pytest.mark.parametrize(
    "input_year, expected",  # Ensure the parameters are comma-separated
    [
        (2023, [2023]),
        ([2023, 2024], [2023, 2024]),
        ([2022, 2024], [2022, 2023, 2024]),
        ([2022, 2020], [2020, 2021, 2022]),
        ([2022, 2022], [2022]),
    ],
)
def test_build_years(input_year, expected):
    test = build_years_list(input_year)
    assert test == expected
