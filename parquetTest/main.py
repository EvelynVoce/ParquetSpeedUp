import itertools
import time

import polars as pl


def get_permu_ross(col1, col2, col3):
    cols1 = []
    cols2 = []
    cols3 = []
    for x in col1:
        for y in col2:
            for z in col3:
                cols1 = cols1 + [x]
                cols2 = cols2 + [y]
                cols3 = cols3 + [z]
    df = pl.DataFrame(
        {
            "Col1": cols1,
            "Col2": cols2,
            "Col3": cols3
        },
        schema = ["col1", "col2", "col3"]
    )
    return df


def get_permutations(col1, col2, col3):
    permutations = itertools.product(col1, col2, col3)
    df = pl.DataFrame(permutations, schema=["col1", "col2", "col3"])
    return df


def combined_permutations(lists):
    # Generate all possible permutations
    for lst in lists:
        yield from itertools.product(*lst)


if __name__ == "__main__":
    length = 1_000
    start_time1 = time.perf_counter()
    for x in range(length):
        list_of_dfs = [
            get_permu_ross([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            get_permu_ross([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            get_permu_ross([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
            get_permu_ross([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            get_permu_ross([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            get_permu_ross([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
            get_permu_ross([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            get_permu_ross([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            get_permu_ross([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
        ]
        df1 = pl.concat(list_of_dfs)
    end_time1 = time.perf_counter()
    elapsed_time1 = end_time1 - start_time1
    print(f"Time taken: {elapsed_time1:.6f} seconds")
    # 12.153 for 50_000


    start_time2 = time.perf_counter()
    for x in range(length):
        list_of_dfs = [
            get_permutations([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            get_permutations([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            get_permutations([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
            get_permu_ross([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            get_permu_ross([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            get_permu_ross([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
            get_permu_ross([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            get_permu_ross([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            get_permu_ross([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
        ]
        df2 = pl.concat(list_of_dfs)
    end_time2 = time.perf_counter()
    elapsed_time2 = end_time2 - start_time2
    print(f"Time taken: {elapsed_time2:.6f} seconds")
    # 9.892 for 50_000
    #
    start_time3 = time.perf_counter()
    for x in range(length):
        lists = [
            ([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            ([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            ([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
            ([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            ([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            ([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"]),
            ([1, 2, 3, 4, 5, 6, 7], ["A", "B", "C", "D"], ["AAA", "BBB"]),
            ([5, 3, 6, 7, 1, 2, 4], ["C", "D", "E", "B"], ["DDD", "CCC"]),
            ([1, 2, 6, 2, 3, 9, 8], ["CC", "DA", "EA", "BA"], ["DD", "CC"])
        ]
        final_df = pl.DataFrame(combined_permutations(lists), schema=["col1", "col2", "col3"])
    end_time3 = time.perf_counter()
    elapsed_time3 = end_time3 - start_time3
    print(f"Time taken: {elapsed_time3:.6f} seconds")

    print(final_df.equals(df1))

    # 3.478 for 50_000
    # 3.484x faster