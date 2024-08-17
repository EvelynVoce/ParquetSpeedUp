import polars as pl
import random

def new_calc():
    # Sample data creation (replace this with your actual data)
    df = pl.DataFrame({
        "ID": ["GenericID" for x in range(100)],
        "Type": [y for y in ["A", "C"] for x in range(50)],
        "Min1": [random.randint(1,100) for x  in range(100)],
        "Min2": [random.randint(1,100) for x  in range(100)],
    })

    # Apply rolling median with a window size of 40 and a null threshold of 10% (4 nulls allowed)
    window_size = 40

    # Adjust min_periods to a lower value if needed
    df = df.groupby(["ID", "Type"]).agg(
        Min1_rolling_median=pl.col("Min1").rolling_median(center=True, window_size=window_size, min_periods=4),
        Min2_rolling_median=pl.col("Min2").rolling_median(center=True, window_size=window_size, min_periods=4)
    )

    df = df.explode(["Min1_rolling_median", "Min2_rolling_median"])

    # Create a new column with the minimum of Min1_rolling_median and Min2_rolling_median
    df = df.with_columns(
        min_min=pl.min_horizontal(["Min1_rolling_median", "Min2_rolling_median"]),
    )


    print(df)


if __name__ == "__main__":
    new_calc()