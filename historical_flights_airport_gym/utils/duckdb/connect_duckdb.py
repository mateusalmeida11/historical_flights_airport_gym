import duckdb


class DuckDBConnectionError(Exception):
    def __init__(self, message, status_code=None):
        super().__init__(message)


class DuckDBManager:
    def __init__(self, s3_endpoint=None):
        self.conn = duckdb.connect(":memory")
        self.s3_endpoint = s3_endpoint
        self._conect_aws()

    def _conect_aws(self):
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        if self.s3_endpoint:
            self.conn.execute(
                f"""
                              CREATE OR REPLACE SECRET secret (
                              TYPE s3,
                              KEY_ID 'teste',
                              SECRET 'teste',
                              REGION 'us-east-1',
                              ENDPOINT '{self.s3_endpoint}',
                              URL_STYLE 'path',
                              USE SSL 'false'
                              )
                              """
            )
        else:
            self.conn.execute(
                """
                              CREATE OR REPLACE SECRET secret (
                              TYPE s3,
                              PROVIDER credential_chain
                              );
                              """
            )

    pass
