from datafusion import DataFrame, SessionContext


def get_columns_names(df: DataFrame) -> list[str]:
    cols = str(df.schema())
    cols = " ".join(cols.split())
    cols = cols.split(" ")
    return [col.replace(":", "") for col in cols if ":" in col]


def select_all_exclude(df: DataFrame, to_exclude: list[str]) -> DataFrame:
    cols = get_columns_names(df)
    cols = [col for col in cols if col not in to_exclude]
    return df.select_columns(" ".join(cols))


if __name__ == "__main__":
    ctx = SessionContext()
    df = ctx.from_pydict({"id": [1, 2, 3], "name": ["foo", "bar", "baz"], "data": [42, 43, 44]})
    print(df)

    columns = get_columns_names(df)
    print("columns names: ", columns)

    res = select_all_exclude(df, ["name", "data"])
    print(res)