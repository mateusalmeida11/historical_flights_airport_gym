import duckdb


class DuckDBManager:
    def __init__(self, s3_endpoint=None):
        self.conn = duckdb.connect(":memory")

    pass
