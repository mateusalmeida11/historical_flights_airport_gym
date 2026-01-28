import json
import os
from unittest.mock import MagicMock

import boto3
import pytest
from duckdb import (
    BinderException,
    CatalogException,
    DuckDBPyConnection,
    HTTPException,
    ParserException,
)

from historical_flights_airport_gym.utils.aws.S3 import (
    S3ClientFactory,
    S3Config,
    S3Storage,
)
from historical_flights_airport_gym.utils.duckdb.connect_duckdb import (
    DuckDBCatalogExceptionError,
    DuckDBConnection,
    DuckDBErrorNotFindKey,
    DuckDBHTTPError,
    DuckDBParserError,
    DuckDBQuery,
    DuckDBS3Configurator,
)

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


def test_conexao_duckdb_s3_success():
    db = DuckDBConnection()
    conn = db.get_conn()

    assert isinstance(conn, DuckDBPyConnection)


def test_query_simple_to_validate_duckdb_conn():
    db = DuckDBConnection()
    conn = db.get_conn()
    result = conn.sql("SELECT 35").fetchall()[0][0]

    assert result == 35


def test_insert_value_in_memory_duckdb():
    db = DuckDBConnection()
    conn = db.get_conn()
    conn.execute("CREATE OR REPLACE TABLE test (id INTEGER)")
    conn.execute("INSERT INTO test VALUES (1)")
    result = conn.execute("SELECT * FROM test").fetchall()
    db.close()

    assert result[0][0] == 1


def test_setup_inicial_aws():
    db = DuckDBConnection()
    conn = db.get_conn()
    s3_config = DuckDBS3Configurator(conn=conn)
    result = s3_config.configure(s3_endpoint=os.getenv("S3_ENDPOINT"))

    assert result is None


def test_mock_raise_http_error_duckdb():
    mock_conn = MagicMock()
    mock_conn.sql.side_effect = HTTPException("HTTP Error")
    query_service = DuckDBQuery(conn=mock_conn)
    query = "SELECT * FROM read_json('s3://bucket/private.json')"
    with pytest.raises(DuckDBHTTPError) as excinfo:
        query_service.make_query(query)
    e = excinfo.value
    assert "HTTP Error" in e.message


def test_mock_raise_binder_exception_duckdb():
    mock_conn = MagicMock()
    mock_conn.sql.side_effect = BinderException("Binder Error")
    query_service = DuckDBQuery(conn=mock_conn)
    query = "SELECT column_not_exist FROM read_json('s3://bucket/file.json')"
    with pytest.raises(DuckDBErrorNotFindKey) as excinfo:
        query_service.make_query(query)

    e = excinfo.value
    assert "Binder Error" in e.message


def test_mock_raise_catalog_exception_duckdb():
    mock_conn = MagicMock()
    mock_conn.sql.side_effect = CatalogException("Catalog Exception")
    query_service = DuckDBQuery(conn=mock_conn)
    query = "SELECT * FROM tabela_inexistente"
    with pytest.raises(DuckDBCatalogExceptionError) as excinfo:
        query_service.make_query(query)

    e = excinfo.value
    assert "Catalog Exception" in e.message


def test_mock_raise_parse_exception_duckdb():
    mock_conn = MagicMock()
    mock_conn.sql.side_effect = ParserException("syntax error")
    query_service = DuckDBQuery(conn=mock_conn)
    query = "SELECTT * FROM tabela_inexistente"
    with pytest.raises(DuckDBParserError) as excinfo:
        query_service.make_query(query)

    e = excinfo.value
    assert "syntax error" in e.message
