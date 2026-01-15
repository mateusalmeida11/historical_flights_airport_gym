import duckdb


def check(event, context):
    conn = duckdb.connect(":memory")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(
        """
                 CREATE OR REPLACE SECRET secret (
                 TYPE s3,
                 PROVIDER credential_chain
                 );
                 """
    )
    # criar path para o bucket
    bucket = event.get("bucket")
    key = event.get("key")
    uri_bucket = f"s3://{bucket}/{key}"
    # fazer consulta SQL
    query = f"""
    CREATE TABLE IF NOT EXISTS flights AS
    SELECT
        content.*
    FROM
        (
            SELECT
                unnest(content) AS content
            FROM
                read_json('{uri_bucket}')
        ) AS json_content;
    """
    conn.execute(query)
    return {"status": "success"}
