import duckdb
from duckdb import BinderException, CatalogException, HTTPException


class DuckDBHTTPError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class DuckDBErrorNotFindKey(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class DuckDBCatalogExceptionError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


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
                              USE_SSL 'false'
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

    def make_query(self, query):
        try:
            self.conn.sql(query)
        except HTTPException as e:
            raise DuckDBHTTPError(str(e)) from e
        except BinderException as e:
            raise DuckDBErrorNotFindKey(str(e)) from e
        except CatalogException as e:
            raise DuckDBCatalogExceptionError(str(e)) from e
