import pytest
import polars as pl
from polars.testing import assert_frame_equal
from new_calc import get_calc_per_type

@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "ID": [1, 1, 1, 1, 1, 1, 1, 1],
        "Type": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "Min1": [10, 20, 30, 40, 50, 60, 70, 80],
        "Min2": [15, 25, 35, 45, 55, 65, 75, 85],
    })


@pytest.fixture
def sample_df_multiple_ids():
    return pl.DataFrame({
        "ID": [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
        "Type": ["A", "A", "A", "A", "B", "B", "B", "B", "A", "A", "A", "A", "B", "B", "B", "B"],
        "Min1": [10, 20, 30, 40, 50, 60, 70, 80, 90, 20, 30, 40, 50, 60, 70, 100],
        "Min2": [15, 25, 35, 45, 55, 65, 75, 85, 95, 25, 35, 45, 55, 65, 75, 105],
    })


class TestCalc1:

    def test_get_calc_per_type(self, sample_df):
        result_df = get_calc_per_type(sample_df)

        expected = pl.DataFrame(
            {
                "ID": [1, 1],
                "Type": ["A", "B"],
                "avg_min_per_type": [25.5, 65.5],
                "Stage": ["Final", "Final"],
                "stage_constant": [0.5, 0.5],
            }
        )
        print(result_df)
        print(expected)
        assert_frame_equal(result_df, expected, check_row_order=False)

    def test_get_calc_per_type_multi(self, sample_df_multiple_ids):
        result_df = get_calc_per_type(sample_df_multiple_ids)
        expected = pl.DataFrame(
            {
                "ID": [1, 1, 2, 2],
                "Type": ["A", "B", "A", "B"],
                "avg_min_per_type": [25.5, 65.5, 38, 68],
                "Stage": ["Final", "Final", "Final", "Final"],
                "stage_constant": [0.5, 0.5, 0.5, 0.5],
            }
        )
        assert_frame_equal(result_df, expected, check_row_order=False)


    def test_empty_df(self):
        """Test that the function handles an empty DataFrame."""
        df = pl.DataFrame({
            "ID": [],
            "Type": [],
            "Min1": [],
            "Min2": [],
        })

        result_df = get_calc_per_type(df)
        assert result_df.shape == (0, 5)


    def test_not_enough_values(self):
        """Test that the function handles a DataFrame with not enough values to roll"""
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
                "avg_min_per_type": [None, 25.5],
                "Stage": ["Final", "Final"],
                "stage_constant": [0.5, 0.5],
            }
        )
        assert_frame_equal(result_df, expected, check_row_order=False)

    def test_too_many_window_nulls(self):
        """Test that the function handles a DataFrame with too many nulls in the window"""
        df = pl.DataFrame({
            "ID": [1, 1, 1, 1, 1],
            "Type": ["A", "A", "A", "B", "B"],
            "Min1": [10, None, None, 20, 30],
            "Min2": [15, 200, 200, 25, 35],
        })

        result_df = get_calc_per_type(df)
        expected = pl.DataFrame(
            {
                "ID": [1, 1],
                "Type": ["A", "B"],
                "avg_min_per_type": [None, 25.5],
                "Stage": ["Final", "Final"],
                "stage_constant": [0.5, 0.5],
            }
        )
        assert_frame_equal(result_df, expected, check_row_order=False)
