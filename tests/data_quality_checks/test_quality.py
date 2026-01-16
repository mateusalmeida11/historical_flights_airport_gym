from historical_flights_airport_gym.soda.check_function import check
from tests.utils.test_duckdb import mock_upload_s3


def teste_data_quality_staging_to_bronze_source_anac_success(monkeypatch):
    # set variaveis de ambiente
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ACCESS_KEY", "test")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ENDPOINT_URL", "http://localhost:4566")

    # 1. criando nome do bucket e key
    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    # 2. Fazendo Upload de Arquivo
    mock_upload_s3(bucket_name=bucket_name, key=key)

    # 3. Simulacao do Event da Camada Staging
    table_name = "brazilian_flights_staging"
    check_subpath_name = "staging_bronze_check.yml"
    event = {
        "status": "success",
        "bucket": "mateus-us-east-1-etl-flights",
        "key": "staging/2025_10_06_123456789_0.json",
        "table_name": table_name,
        "checks_subpath": check_subpath_name,
        "s3_response": {"status_code": 200},
    }

    context = {}

    result = check(event, context)

    assert result["status_soda"] == 0
    assert result["path_file_check"] == f"{bucket_name}/{key}"
    assert result["s3"]["status_code"] == 200
