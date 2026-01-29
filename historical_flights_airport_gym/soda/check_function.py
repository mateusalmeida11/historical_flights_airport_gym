import json
import os

from historical_flights_airport_gym.utils.aws.S3 import (
    S3ClientFactory,
    S3Config,
    S3Storage,
)
from historical_flights_airport_gym.utils.build_path import (
    build_path_data_quality_check_file,
    get_root_path,
    path_file_raw,
)
from historical_flights_airport_gym.utils.duckdb.connect_duckdb import (
    DuckDBConnection,
    DuckDBQuery,
    DuckDBS3Configurator,
)
from historical_flights_airport_gym.utils.quality.check import SodaAnalyzer
from historical_flights_airport_gym.utils.queries.query import QualityQuerieService


class DataQualityIssue(Exception):
    def __init__(self, message: str, code_error: int):
        super().__init__(message)
        self.message = message
        self.code_error = code_error


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
    # 1. Extracao de parametros
    bucket = event.get("bucket")
    key = event.get("key")
    table_name = event.get("table_name")
    checks_subpath = event.get("checks_subpath")

    # 2. Inicializacao
    s3 = build_s3()
    conn = build_conn_duckdb()
    db_query = DuckDBQuery(conn=conn)

    # 3. Criacao de URI
    s3_uri = f"s3://{bucket}/{key}"

    # 4. Criacao de Path para Quality Check
    root_path = get_root_path()
    checks_path = build_path_data_quality_check_file(
        root_path=root_path, source_file=checks_subpath
    )
    checks_path = str(checks_path)

    # 5. Criacao de View
    query_executor = QualityQuerieService(db_query)
    query_executor.create_view_from_json_staging(table_name=table_name, s3_uri=s3_uri)

    # 6. Run Soda Scan
    soda = SodaAnalyzer(conn=conn)
    exit_code, scan_results = soda.run_scan(checks_path=checks_path)

    # 7. create path to upload s3
    layer = key.split("/")[0]
    file_name = path_file_raw()
    key_log = f"logs/dq-checks/{layer}/{file_name}.json"

    jsonData = json.dumps(scan_results, indent=4)
    response_s3 = s3.upload_file(bucket=bucket, data=jsonData, key=key_log)

    if exit_code != 0:
        raise DataQualityIssue("Falha em Qualidade de Dados", code_error=exit_code)

    return {
        "status_soda": exit_code,
        "bucket": bucket,
        "path_file_check": f"{bucket}/{key}",
        "s3": {"status_code": response_s3["ResponseMetadata"]["HTTPStatusCode"]},
    }
