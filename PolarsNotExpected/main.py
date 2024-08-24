import polars as pl


def rolling_median(df):
    df = df.sort("Location")

    df = df.fill_nan(None)
    print(df)
    # With a window of 6 it looks 2 in front and 3 behind including itself making 6
    df = df.with_columns(
        rolling_min=pl.col("Meas").rolling_median(center=True, window_size=6, min_periods=4)
    )
    df = df.fill_null(float("nan"))
    print(df)
    return df


def get_min(df):
    df = df.with_columns(
        min_rolling_min=pl.col("rolling_min").nan_min()
    )
    print(df)
    return df


if __name__ == "__main__":
    init = pl.DataFrame(
        {
            "ID": ["S001"] * 10,
            "Location": [5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
            "Meas": [10,20,30,40,50,60,70,80,90,100]
        },
        schema={
            "ID": pl.String,
            "Location": pl.Float64,
            "Meas": pl.Float64,
        }
    )

    df_normal = rolling_median(init)
    get_min(df_normal)

    init = pl.DataFrame(
        {
            "ID": ["S001"] * 10,
            "Location": [5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
            "Meas": [10, 20, 30, None, None, None, None, 80, 90, 100]
        },
        schema={
            "ID": pl.String,
            "Location": pl.Float64,
            "Meas": pl.Float64,
        }
    )

    df_with_none = rolling_median(init)
    get_min(df_with_none)

