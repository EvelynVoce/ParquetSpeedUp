import pytest
import polars as pl
from polars.testing import assert_frame_equal
from new_calc import get_calc_per_type

# Import the function from the module where it's defined
# from your_module import get_calc_per_type

@pytest.fixture
def sample_df():
    """Fixture to provide a sample DataFrame for testing."""
    return pl.DataFrame({
        "ID": [1, 1, 1, 1, 1, 1, 1, 1],
        "Type": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "Min1": [10, 20, 30, 40, 50, 60, 70, 80],
        "Min2": [15, 25, 35, 45, 55, 65, 75, 85],
    })


def test_get_calc_per_type(sample_df):
    """Test the main functionality of get_calc_per_type."""
    result_df = get_calc_per_type(sample_df)

    expected = pl.DataFrame(
        {
            "ID": [1, 1],
            "Type": ["A", "B"],
            "avg_min_per_type": [25.0, 65.0]
        }
    )
    assert_frame_equal(result_df, expected, check_row_order=False)


def test_empty_df():
    """Test that the function handles an empty DataFrame."""
    df = pl.DataFrame({
        "ID": [],
        "Type": [],
        "Min1": [],
        "Min2": [],
    })

    result_df = get_calc_per_type(df)
    assert result_df.shape == (0, 3)


def test_too_many_window_nulls():
    """Test that the function handles a DataFrame with too many nulls in a window"""
    df = pl.DataFrame({
        "ID": [1, 1, 1],
        "Type": ["A", "B", "B"],
        "Min1": [10, 20, 30],
        "Min2": [15, 25, 35],
    })

    result_df = get_calc_per_type(df)
    expected = pl.DataFrame(
        {
            "ID": [1, 1],
            "Type": ["A", "B"],
            "avg_min_per_type": [None, 25.0]
        }
    )
    assert_frame_equal(result_df, expected, check_row_order=False)
