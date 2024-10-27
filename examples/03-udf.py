from datafusion import udf, SessionContext, functions as f, col
import pyarrow


def split_text(array: pyarrow.Array) -> pyarrow.Array:
    xs = array.to_pylist()
    res = [x.split(".")[0] for x in xs]
    return pyarrow.array(pyarrow.array(res))


if __name__ == "__main__":
    ctx = SessionContext()
    df = ctx.from_pydict({"id": [1, 2, 3], "name": ["foo.txt", "bar.txt", "baz.txt"]})
    exec = udf(split_text, [pyarrow.string()], pyarrow.string(), "stable")
    res = df.select(col("id"), col("name"), exec(f.col("name")).alias("name_new"))
    print(res)

    ctx.from_pydict({"id": [1, 2, 3], "name": ["foo.txt", "bar.txt", "baz.txt"]}, name="t")
    exec = udf(split_text, [pyarrow.string()], pyarrow.string(), "stable", name="executor")
    ctx.register_udf(exec)
    res = ctx.sql("select id, name, executor(name) as name_new from t")
    print(res)