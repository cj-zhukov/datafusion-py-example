from source import select_all_exclude, get_columns_names

import datafusion


def test_get_columns_names():
    ctx = datafusion.SessionContext()
    df = ctx.from_pydict({"id": [1, 2, 3], "name": ["foo", "bar", "baz"], "data": [42, 43, 44]})
    columns_names = get_columns_names(df)
    assert columns_names == ["id", "name", "data"]


def test_select_all_exclude():
    ctx = datafusion.SessionContext()
    df = ctx.from_pydict({"id": [1, 2, 3], "name": ["foo", "bar", "baz"], "data": [42, 43, 44]})
    res = select_all_exclude(df, ["name", "data"])
    res = res.to_pydict()
    assert res == {"id": [1, 2, 3]}


if __name__ == "__main__":
    test_get_columns_names()
    test_select_all_exclude()
    