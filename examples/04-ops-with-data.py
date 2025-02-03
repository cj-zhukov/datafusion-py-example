from datafusion import SessionContext


if __name__ == "__main__":
    ctx = SessionContext()
    file = "foo.csv"
    ctx.register_csv("foo", file)
    ctx.sql("copy (select * from foo) to 'foo.parquet'")