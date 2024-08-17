import polars as pl
import random


def get_calc_per_type(df):
    # Apply rolling median with a window size of 40 and a null threshold of 10% (4 nulls allowed)
    window_size = 40

    # Adjust min_periods to a lower value if needed
    df2 = df.group_by(["ID", "Type"]).agg(
        Min1_rolling_median=pl.col("Min1").rolling_median(center=True, window_size=window_size, min_periods=4),
        Min2_rolling_median=pl.col("Min2").rolling_median(center=True, window_size=window_size, min_periods=4)
    )

    df2 = df2.explode(["Min1_rolling_median", "Min2_rolling_median"])

    # Create a new column with the minimum of Min1_rolling_median and Min2_rolling_median
    df2 = df2.with_columns(
        min_min=pl.min_horizontal(["Min1_rolling_median", "Min2_rolling_median"]),
    )

    df_all_lanes = df2.group_by(["ID", "Type"]).agg(
        avg_min_per_type=pl.col("min_min").mean()
    )
    return df_all_lanes


def get_calc_per_lane(df):
    # Apply rolling median with a window size of 40 and a null threshold of 10% (4 nulls allowed)
    window_size = 40
    # print(df)
    # Adjust min_periods to a lower value if needed
    df2 = df.group_by(["ID", "Type", "Lane"]).agg(
        Min1_rolling_median=pl.col("Min1").rolling_median(center=True, window_size=window_size, min_periods=4),
        Min2_rolling_median=pl.col("Min2").rolling_median(center=True, window_size=window_size, min_periods=4),
        Max_max=pl.col("Max").max()
    )

    df2 = df2.explode(["Min1_rolling_median", "Min2_rolling_median"])

    # Create a new column with the minimum of Min1_rolling_median and Min2_rolling_median
    df2 = df2.with_columns(
        minimum_min=pl.min_horizontal(["Min1_rolling_median", "Min2_rolling_median"]),
    )
    unique_df = df2.unique(subset=["ID", "Type", "Lane"])

    df_all_lanes = unique_df.group_by(["ID", "Type", "Lane"]).agg(
        avg_min_per_lane=pl.col("minimum_min").mean()
    )
    df_all_lanes = unique_df.join(df_all_lanes, on=["ID", "Type", "Lane"])
    return df_all_lanes

def new_calc():
    # Sample data creation (replace this with your actual data)
    df = pl.DataFrame({
        "ID": ["GenericID" for x in range(100)],
        "Lane": [random.randint(1,5) for x in range(100)],
        "Type": [y for y in ["A", "C"] for x in range(50)],
        "Min1": [random.randint(1,100) for x in range(100)],
        "Min2": [random.randint(1,100) for x in range(100)],
        "Max": [random.randint(100,200) for x in range(100)],
    })


    df_per_type = get_calc_per_type(df)
    print(df_per_type)

    df_per_lane = get_calc_per_lane(df)
    print(df_per_lane)


if __name__ == "__main__":
    new_calc()
