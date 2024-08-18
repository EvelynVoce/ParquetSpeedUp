import pytest
import polars as pl
from polars.testing import assert_frame_equal
from new_calc import get_calc_per_lane

@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "ID": [1, 1, 1, 1, 1, 1, 1, 1],
        "Type": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "Lane": [1, 1, 1, 1, 1, 1, 1, 1],
        "Min1": [10, 20, 30, 40, 50, 60, 70, 80],
        "Min2": [15, 25, 35, 45, 55, 65, 75, 85],
        "Max": [100, 105, 110, 115, 120, 125, 130, 135]
    })


@pytest.fixture
def sample_df_multiple_ids():
    return pl.DataFrame({
        "ID": [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
        "Type": ["A", "A", "A", "A", "B", "B", "B", "B", "A", "A", "A", "A", "B", "B", "B", "B"],
        "Min1": [10, 20, 30, 40, 50, 60, 70, 80, 90, 20, 30, 40, 50, 60, 70, 100],
        "Min2": [15, 25, 35, 45, 55, 65, 75, 85, 95, 25, 35, 45, 55, 65, 75, 105],
    })


class TestCalc2:

    def test_get_calc_per_type(self, sample_df):
        result_df = get_calc_per_lane(sample_df)
        expected = pl.DataFrame(
            {
                "ID": [1, 1],
                "Type": ["A", "B"],
                "Lane": [1, 1],
                "avg_min_per_lane": [25.0, 65.0],
                "min_minimum_rolling_min": [15.0, 55.0],
                "max_rolling_max": [112.5, 132.5]
            }
        )
        assert_frame_equal(result_df, expected, check_row_order=False)

    def test_get_calc_multi_lane(self, sample_df_multiple_ids):
        sample = pl.DataFrame({
            "ID": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "Type": ["A", "A", "A", "A", "B", "B", "B", "B", "A", "A", "A", "A", "B", "B", "B", "B"],
            "Lane": [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
            "Min1": [10, 20, 30, 40, 50, 60, 70, 80, 15, 25, 35, 45, 55, 65, 75, 85],
            "Min2": [15, 25, 35, 45, 55, 65, 75, 85, 20, 30, 40, 50, 60, 70, 80, 90],
            "Max": [100, 105, 110, 115, 120, 125, 130, 135, 105, 110, 115, 120, 125, 130, 135, 140]
        })

        result_df = get_calc_per_lane(sample)
        print(result_df)
        expected = pl.DataFrame(
            {
                "ID": [1, 1, 1, 1],
                "Type": ["A", "B", "A", "B"],
                "Lane": [1, 1, 2, 2],
                "avg_min_per_lane": [21.25, 25.0, 65.0],
                "min_minimum_rolling_min": [15.0, 55.0],
                "max_rolling_max": [112.5, 132.5]
            }
        )
        assert_frame_equal(result_df, expected, check_row_order=False)


    def test_empty_df(self):
        """Test that the function handles an empty DataFrame."""
        df = pl.DataFrame({
            "ID": [],
            "Type": [],
            "Lane": [],
            "Min1": [],
            "Min2": [],
            "Max": []
        })

        result_df = get_calc_per_lane(df)
        assert result_df.shape == (0, 6)


    def test_too_many_window_nulls(self):
        """Test that the function handles a DataFrame with too many nulls in a window"""
        df = pl.DataFrame({
            "ID": [1, 1, 1],
            "Type": ["A", "B", "B"],
            "Lane": [1, 1, 1],
            "Min1": [10, 20, 30],
            "Min2": [15, 25, 35],
            "Max": [100, 105, 110]
        })

        result_df = get_calc_per_lane(df)
        expected = pl.DataFrame(
            {
                "ID": [1, 1],
                "Type": ["A", "B"],
                "Lane": [1, 1],
                "avg_min_per_lane": [None, 25.0],
                "min_minimum_rolling_min": [None, 25.0],
                "max_rolling_max": [None, 107.5]
            }
        )
        assert_frame_equal(result_df, expected, check_row_order=False)
