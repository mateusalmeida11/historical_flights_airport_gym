import duckdb
from duckdb import (
    BinderException,
    CatalogException,
    DuckDBPyConnection,
    HTTPException,
    ParserException,
)


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


class DuckDBParserError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class DuckDBExceptionGeneric(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class DuckDBConnection:
    def __init__(self, database=":memory:"):
        self.conn = duckdb.connect(database)

    def get_conn(self):
        return self.conn

    def close(self):
        return self.conn.close()


class DuckDBS3Configurator:
    def __init__(self, conn: DuckDBPyConnection):
        self.conn = conn

    def configure(self, s3_endpoint: str = None):
        self.conn.execute("SET home_directory='/tmp/duckdb/'")
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")

        if s3_endpoint:
            self.conn.execute(
                f"""
                              CREATE OR REPLACE SECRET secret (
                              TYPE s3,
                              KEY_ID 'teste',
                              SECRET 'teste',
                              REGION 'us-east-1',
                              ENDPOINT '{s3_endpoint}',
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


class DuckDBQuery:
    def __init__(self, conn):
        self.conn = conn

    def make_query(self, query):
        try:
            self.conn.sql(query)
        except HTTPException as e:
            raise DuckDBHTTPError(str(e)) from e
        except BinderException as e:
            raise DuckDBErrorNotFindKey(str(e)) from e
        except CatalogException as e:
            raise DuckDBCatalogExceptionError(str(e)) from e
        except ParserException as e:
            raise DuckDBParserError(str(e)) from e
        except Exception as e:
            raise DuckDBExceptionGeneric(str(e)) from e
