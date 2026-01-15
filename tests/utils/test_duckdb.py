import boto3
import pytest
from duckdb import DuckDBPyConnection, HTTPException

from historical_flights_airport_gym.utils.duckdb.connect_duckdb import DuckDBManager


def create_conexao_localstack():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def test_conexao_duckdb_s3_success():
    duck = DuckDBManager()
    conn = duck.conn

    assert isinstance(conn, DuckDBPyConnection)


def test_setup_inicial_aws():
    duck = DuckDBManager()
    result = duck._conect_aws()

    assert result is None


def test_raise_error_missing_credential_duckdb_aws():
    with pytest.raises(HTTPException) as excinfo:
        DuckDBManager()

    e = excinfo.value
    assert "HTTP Error" in e
