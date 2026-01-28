import json
import os

import boto3
import pytest

from historical_flights_airport_gym.utils.aws.S3 import (
    S3ClientFactory,
    S3Config,
    S3Storage,
)
from historical_flights_airport_gym.utils.duckdb.connect_duckdb import (
    DuckDBConnection,
    DuckDBHTTPError,
    DuckDBQuery,
    DuckDBS3Configurator,
)
from historical_flights_airport_gym.utils.queries.query import QualityQuerieService

config = S3Config(
    region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    access_key=os.getenv("ACCESS_KEY"),
    endpoint=os.getenv("ENDPOINT_URL"),
    secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
)

s3_client = S3ClientFactory().create(config)


def create_conexao_localstack():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def mock_upload_s3(bucket_name, key):
    # estabelecendo conexao com localstack
    s3_client = create_conexao_localstack()
    s3_client.create_bucket(Bucket=bucket_name)

    # criando arquivo de amostra
    body = {
        "metada": {},
        "content": [
            {
                "sg_empresa_icao": "AAL",
                "nm_empresa": "AMERICAN AIRLINES, INC.",
                "nr_voo": "0904",
                "cd_di": "0",
                "cd_tipo_linha": "I",
                "sg_equipamento_icao": "B772",
                "nr_assentos_ofertados": "288",
                "sg_icao_origem": "SBGL",
                "nm_aerodromo_origem": "AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL",
                "dt_partida_prevista": "20/11/2025 23:55",
                "dt_partida_real": "20/11/2025 23:48",
                "sg_icao_destino": "KMIA",
                "nm_aerodromo_destino": "MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA",
                "dt_chegada_prevista": "21/11/2025 08:40",
                "dt_chegada_real": "21/11/2025 08:31",
                "ds_situacao_voo": "REALIZADO",
                "ds_justificativa": "",
                "dt_referencia": "20/11/2025",
                "ds_situacao_partida": "Antecipado",
                "ds_situacao_chegada": "Antecipado",
            },
        ],
    }

    jsonData = json.dumps(body, indent=4)

    # 2. Upload para o S3
    s3 = S3Storage(s3_client=s3_client)
    s3.upload_file(data=jsonData, bucket=bucket_name, key=key)


def test_integration_make_query_successfull(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    s3_uri = f"s3://{bucket_name}/{key}"
    table_name = "brazilian_flights_staging"

    db = DuckDBConnection()
    conn = db.get_conn()
    db_s3_config = DuckDBS3Configurator(conn)
    db_s3_config.configure(s3_endpoint="localhost:4566")

    query_executor = DuckDBQuery(conn=conn)

    quality_queries = QualityQuerieService(query_executor)
    quality_queries.create_view_from_json_staging(table_name=table_name, s3_uri=s3_uri)

    result = conn.sql(f"SELECT * FROM {table_name}")
    assert len(result.columns) == 20
    assert len(result) == 1


def test_integration_error_in_query(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    s3_uri = f"s3://{bucket_name}/{key}"
    table_name = "brazilian_flights_staging"
    db = DuckDBConnection()
    conn = db.get_conn()
    db_s3_config = DuckDBS3Configurator(conn)
    db_s3_config.configure()

    query_executor = DuckDBQuery(conn=conn)

    quality_queries = QualityQuerieService(query_executor)

    with pytest.raises(DuckDBHTTPError) as excinfo:
        quality_queries.create_view_from_json_staging(
            table_name=table_name, s3_uri=s3_uri
        )

    e = excinfo.value
    assert "HTTP Error" in e.message
