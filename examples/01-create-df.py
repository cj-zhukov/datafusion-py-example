from datafusion import SessionContext
import pyarrow as pa
import pandas as pd
import polars as pl


if __name__ == "__main__":
    ctx = SessionContext()

    df = ctx.from_pydict({"id": [1, 2, 3], "name": ["foo", "name", "data"], "data": [42, 43, 44]})
    print(df)
    # +----+------+------+
    # | id | name | data |
    # +----+------+------+
    # | 1  | foo  | 42   |
    # | 2  | name | 43   |
    # | 3  | data | 44   |
    # +----+------+------+

    df = ctx.from_pylist([
        {"id": 1, "name": "foo", "data": 42}, 
        {"id": 2, "name": "bar", "data": 43}, 
        {"id": 3, "name": "baz", "data": 44}, 
    ])
    # print(df)

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["foo", "bar", "baz"]), pa.array([42, 43, 44])],
        names=["id", "name", "data"],
    )
    df = ctx.create_dataframe([[batch]])
    # print(df)

    pandas_df = pd.DataFrame({"id": [1, 2, 3], "name": ["foo", "name", "data"], "data": [42, 43, 44]})
    df = ctx.from_pandas(pandas_df)
    # print(df)

    polars_df = pl.DataFrame({"id": [1, 2, 3], "name": ["foo", "name", "data"], "data": [42, 43, 44]})
    df = ctx.from_polars(polars_df)
    # print(df)

    arrow_table = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["foo", "name", "data"], "data": [42, 43, 44]})
    df = ctx.from_arrow(arrow_table)
    # print(df)