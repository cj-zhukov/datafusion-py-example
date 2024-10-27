from datafusion import SessionContext, col, DataFrame, lit


def within_limit(df: DataFrame, limit: int) -> DataFrame:
    return df.filter(col("id") > lit(limit))


if __name__ == "__main__":
    ctx = SessionContext()
    df = ctx.from_pydict({"id": [1, 2, 3], "name": ["foo", "bar", "baz"]})
    res = df.transform(within_limit, 2)
    print(res)