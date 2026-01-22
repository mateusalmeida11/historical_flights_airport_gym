import json
import os
from pathlib import Path

from soda.scan import Scan

from historical_flights_airport_gym.utils.aws.S3 import (
    S3ClientFactory,
    S3Config,
    S3Storage,
)
from historical_flights_airport_gym.utils.build_path import path_file_raw
from historical_flights_airport_gym.utils.duckdb.connect_duckdb import (
    DuckDBConnection,
    DuckDBQuery,
    DuckDBS3Configurator,
)


def build_s3() -> S3Storage:
    config = S3Config(
        region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        access_key=os.getenv("ACCESS_KEY"),
        endpoint=os.getenv("ENDPOINT_URL"),
        secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    )
    s3_client = S3ClientFactory().create(config)
    return S3Storage(s3_client=s3_client)


def build_conn_duckdb() -> DuckDBConnection:
    db = DuckDBConnection()
    conn = db.get_conn()
    db_s3_config = DuckDBS3Configurator(conn)
    db_s3_config.configure(s3_endpoint=os.getenv("S3_ENDPOINT"))
    return conn


def lambda_handler(event, context):
    bucket = event.get("bucket")
    key = event.get("key")
    table_name = event.get("table_name")
    checks_subpath = event.get("checks_subpath")

    s3 = build_s3()
    conn = build_conn_duckdb()
    db_query = DuckDBQuery(conn=conn)

    root_path = str(Path(__file__).resolve().parent)

    checks_path = f"{root_path}/sources/"

    if checks_subpath:
        checks_path += checks_subpath

    s3_uri = f"s3://{bucket}/{key}"

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

    db_query.make_query(query=query)

    layer = key.split("/")[0]

    scan = Scan()
    scan.set_verbose(True)
    scan.add_duckdb_connection(conn)
    scan.set_data_source_name("duckdb")
    scan.add_sodacl_yaml_files(checks_path)

    result = scan.execute()
    jsonObj = scan.get_scan_results()

    # create path to upload s3

    file_name = path_file_raw()
    key_log = f"logs/dq-checks/{layer}/{file_name}.json"

    jsonData = json.dumps(jsonObj, indent=4)
    response_s3 = s3.upload_file(bucket=bucket, data=jsonData, key=key_log)
    return {
        "status_soda": result,
        "bucket": bucket,
        "path_file_check": f"{bucket}/{key}",
        "s3": {"status_code": response_s3["ResponseMetadata"]["HTTPStatusCode"]},
    }
