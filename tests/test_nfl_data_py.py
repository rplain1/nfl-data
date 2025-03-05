import pytest
from nfl_etl.services.nfl_data_py import build_years_list, ftn_charting_file_list


@pytest.mark.parametrize(
    "input_year",
    [3100, "$312"],
)
def test_build_years_invalid(input_year):
    """Test that invalid years raise an error"""
    with pytest.raises(ValueError):  # Expect ValueError to be raised
        build_years_list(input_year)


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
    test = build_years_list(input_year)
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
    test = build_years_list(input_year, min_year=2022)
    assert test == expected


@pytest.mark.parametrize(
    "input_year",
    [3100, 2010, [2010, 2012]],
)
def test_ftn_charting_file_list_invalid(input_year):
    """
    Test that an input that contains all years less than minimum
    available year raises an error
    """
    with pytest.raises(ValueError):  # Expect ValueError to be raised
        ftn_charting_file_list(input_year)


@pytest.mark.parametrize(
    "input_year, expected_len",
    [(2023, 1), ([2021, 2024], 3)],
)
def test_ftn_charting_file_list(input_year, expected_len):
    """Test FTN charting creates lists after 2022"""
    test = ftn_charting_file_list(input_year)
    assert len(test) == expected_len
    assert all(isinstance(x, str) for x in test)
