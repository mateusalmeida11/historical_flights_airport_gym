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

    pass
