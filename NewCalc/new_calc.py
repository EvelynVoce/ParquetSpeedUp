import polars as pl
import random


def get_calc_per_type(df):
    WINDOW_SIZE = 3
    df = df.with_columns(
        Min1_rolling_median=pl.col("Min1").rolling_median(center=True, window_size=WINDOW_SIZE, min_periods=2).over(["ID", "Type"]),
        Min2_rolling_median=pl.col("Min2").rolling_median(center=True, window_size=WINDOW_SIZE, min_periods=2).over(["ID", "Type"])
    )
    df = df.with_columns(
        min_min=pl.min_horizontal(["Min1_rolling_median", "Min2_rolling_median"]),
    )
    return df.group_by(["ID", "Type"]).agg(
        avg_min_per_type=pl.col("min_min").mean()
    )


def get_calc_per_lane(df):
    window_size = 3
    df2 = df.with_columns(
        Min1_rolling_median=pl.col("Min1").rolling_median(center=True, window_size=window_size, min_periods=2).over(
            ["ID", "Type"]),
        Min2_rolling_median=pl.col("Min2").rolling_median(center=True, window_size=window_size, min_periods=2).over(
            ["ID", "Type"]),
        rolling_max=pl.col("Max").rolling_median(center=True, window_size=window_size, min_periods=2).over(
            ["ID", "Type"]),
    )

    df2 = df2.with_columns(
        minimum_rolling_min=pl.min_horizontal(["Min1_rolling_median", "Min2_rolling_median"]),
    )
    unique_df = df2.group_by(["ID", "Type", "Lane"]).agg(
        avg_min_per_lane=pl.col("minimum_rolling_min").mean(),
        min_minimum_rolling_min=pl.col("minimum_rolling_min").min(),
        max_rolling_max=pl.col("rolling_max").max()
    )
    return unique_df

def new_calc():
    # Sample data creation (replace this with your actual data)
    # df = pl.DataFrame({
    #     "ID": ["GenericID" for x in range(100)],
    #     "Lane": [random.randint(1,5) for x in range(100)],
    #     "Type": [y for y in ["A", "C"] for x in range(50)],
    #     "Min1": [random.randint(1,100) for x in range(100)],
    #     "Min2": [random.randint(1,100) for x in range(100)],
    #     "Max": [random.randint(100,200) for x in range(100)],
    # })
    df = pl.DataFrame({
        "ID": ["B1", "B1", "B1", "B1", "B1", "B1", "B1", "B1"],
        "Lane": [1,1,1,1,1,1,1,1],
        "Type": ["A", "A", "A", "A", "C", "C", "C", "C"],
        "Min1": [1,2,3,4,9,7,5,4],
        "Min2": [2,1,2,3,4,5,7,3],
        "Max": [4, 5, 6, 7, 4, 6, 5, 4],
    })

    print(df)
    df_per_type = get_calc_per_type(df)
    print(df_per_type)

    df_per_lane = get_calc_per_lane(df)
    print(df_per_lane)


if __name__ == "__main__":
    new_calc()
