import json
import os

import boto3
import pytest

from historical_flights_airport_gym.soda.check_function import check
from historical_flights_airport_gym.utils.aws.S3 import S3


@pytest.fixture(scope="session", autouse=True)
def setup_localstack():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    s3 = boto3.client("s3", endpoint_url="http://localhost:4566")

    s3.create_bucket(Bucket="mateus-us-east-1-etl-flights")
    yield


def teste_data_quality_staging_to_bronze_source_anac_success():
    # 1. Mockar Bucket S3 na AWS
    bucket_name = "mateus-us-east-1-etl-flights"
    key = "staging/2025_10_06_123456789_0.json"

    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    s3_client.create_bucket(Bucket=bucket_name)

    # 2. Simular Arquivo
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
    s3 = S3(s3_client=s3_client)
    s3.upload_file(data=jsonData, bucket=bucket_name, key=key)

    # 3. Simulacao do Event da Camada Staging
    event = {
        "status": "success",
        "bucket": "mateus-us-east-1-etl-flights",
        "key": "staging/2025_10_06_123456789_0.json",
        "s3_response": {"status_code": 200},
    }

    context = {}

    result = check(event, context)

    assert result["status"] == "success"
    assert result["file_analyzed"] == f"s3://{bucket_name}/{key}"
    assert result["bucket"] == bucket_name
