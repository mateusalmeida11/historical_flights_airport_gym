import json

import boto3
import pytest
from duckdb import DuckDBPyConnection

from historical_flights_airport_gym.utils.aws.S3 import S3
from historical_flights_airport_gym.utils.duckdb.connect_duckdb import (
    DuckDBCatalogExceptionError,
    DuckDBConnection,
    DuckDBErrorNotFindKey,
    DuckDBHTTPError,
    DuckDBManager,
    DuckDBParserError,
    DuckDBS3Configurator,
)


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
    s3 = S3()
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


def test_connection_s3_with_valid_query(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    # 1. criando nome do bucket e key
    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    uri_bucket = f"s3://{bucket_name}/{key}"

    query = f"""
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

    db = DuckDBConnection()
    conn = db.get_conn()
    s3_config = DuckDBS3Configurator(conn)
    s3_config.configure(s3_endpoint="localhost:4566")

    result = conn.sql(query)

    assert len(result.columns) == 20
    assert len(result) == 1


def test_setup_inicial_aws():
    duck = DuckDBManager()
    result = duck._conect_aws()

    assert result is None


def test_raise_error_missing_credential_duckdb_aws(monkeypatch):
    # set variaveis de ambiente
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    # 1. criando nome do bucket e key
    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    # 3. Fazer a Query
    key_err = "staging/2025_10_06_1456789_0.json"
    uri_bucket = f"s3://{bucket_name}/{key_err}"
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

    # 4. Instanciar a Classe
    db = DuckDBManager()
    with pytest.raises(DuckDBHTTPError) as excinfo:
        db.make_query(query)

    e = excinfo.value
    assert "HTTP Error" in e.message


def test_raise_binder_error_query_duckdb(monkeypatch):
    # set variaveis de ambiente
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    # 1. criando nome do bucket e key
    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    # 3. Fazer a Query
    uri_bucket = f"s3://{bucket_name}/{key}"
    column_missing = "qualquer_coluna"
    query = f"""
    CREATE TABLE IF NOT EXISTS flights AS
    SELECT
        content.{column_missing}
    FROM
        (
            SELECT
                unnest(content) AS content
            FROM
                read_json('{uri_bucket}')
        ) AS json_content;
    """
    db = DuckDBManager()
    with pytest.raises(DuckDBErrorNotFindKey) as excinfo:
        db.make_query(query)

    e = excinfo.value
    assert "Binder Error" in e.message
    assert column_missing in e.message


def test_raise_catalog_exception_error_duckdb(monkeypatch):
    # set variaveis de ambiente
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    # 1. criando nome do bucket e key
    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    # 3. Fazer a Query
    uri_bucket = f"s3://{bucket_name}/{key}"
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

    query_view = f"""
    CREATE OR REPLACE VIEW flights AS
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
    db = DuckDBManager()
    with pytest.raises(DuckDBCatalogExceptionError) as excinfo:
        db.make_query(query)
        db.make_query(query_view)

    e = excinfo.value
    assert "Catalog Error" in e.message


def test_raise_exception_parser(monkeypatch):
    # set variaveis de ambiente
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")
    bucket_name = "mateus-us-east-1-etl-flights"

    key = "staging/2025_10_06_123456789_0.json"

    # 2. Chamando funcao de upload
    mock_upload_s3(bucket_name=bucket_name, key=key)

    uri_bucket = f"s3://{bucket_name}/{key}"
    db = DuckDBManager()
    with pytest.raises(DuckDBParserError) as excinfo:
        db.make_query(f"SELECTT * FROM read_json('{uri_bucket}')")

    e = excinfo.value
    assert "syntax error" in e.message
