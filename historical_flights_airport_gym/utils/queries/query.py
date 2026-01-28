from historical_flights_airport_gym.utils.duckdb.connect_duckdb import DuckDBQuery


class QualityQuerieService:
    def __init__(self, query_executor: DuckDBQuery):
        self.query_executor = query_executor

    def create_view_from_json_staging(self, table_name: str, s3_uri: str):
        query = f"""
        CREATE OR REPLACE VIEW '{table_name}' AS
        SELECT
            content.*
        FROM
            (
                SELECT
                    unnest(content) AS content
            FROM
                read_json('{s3_uri}')
            );
        """
        self.query_executor.make_query(query=query)
