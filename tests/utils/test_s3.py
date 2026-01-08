import json

import boto3
import pytest
from botocore.response import StreamingBody
from moto import mock_aws

from historical_flights_airport_gym.utils.aws.S3 import (
    S3,
    S3EmptyFile,
    S3GetError,
    S3UploadError,
)


@mock_aws
def test_upload_s3_success():
    bucket_name = "etl-brazilian-flights"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)
    key = "flights/flights-brazil/2025_10_06_123456789_0.json"
    body = [
        {
            "sg_empresa_icao": "AAL",
            "nm_empresa": "AMERICAN AIRLINES, INC.",
            "nr_voo": "0904",
            "cd_di": "0",
            "cd_tipo_linha": "I",
            "sg_equipamento_icao": "B788",
            "nr_assentos_ofertados": "295",
            "sg_icao_origem": "SBGL",
            "nm_aerodromo_origem": "AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL",
            "dt_partida_prevista": "01/01/2025 23:55",
            "dt_partida_real": "01/01/2025 23:50",
            "sg_icao_destino": "KMIA",
            "nm_aerodromo_destino": "MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA",
            "dt_chegada_prevista": "02/01/2025 07:45",
            "dt_chegada_real": "02/01/2025 08:11",
            "ds_situacao_voo": "REALIZADO",
            "ds_justificativa": "",
            "dt_referencia": "01/01/2025",
            "ds_situacao_partida": "Antecipado",
            "ds_situacao_chegada": "Pontual",
        }
    ]

    jsonData = json.dumps(body, indent=4)
    s3 = S3()
    response = s3.upload_file(data=jsonData, bucket=bucket_name, key=key)

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    assert status_code == 200
    assert "ETag" in response


@mock_aws
def test_upload_s3_inexistente():
    bucket_name = "etl-brazilian-flights"

    boto3.client("s3", region_name="us-east-1")
    key = "flights/flights-brazil/2025_10_06_123456789_0.json"

    body = [
        {
            "sg_empresa_icao": "AAL",
            "nm_empresa": "AMERICAN AIRLINES, INC.",
            "nr_voo": "0904",
            "cd_di": "0",
            "cd_tipo_linha": "I",
            "sg_equipamento_icao": "B788",
            "nr_assentos_ofertados": "295",
            "sg_icao_origem": "SBGL",
            "nm_aerodromo_origem": "AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL",
            "dt_partida_prevista": "01/01/2025 23:55",
            "dt_partida_real": "01/01/2025 23:50",
            "sg_icao_destino": "KMIA",
            "nm_aerodromo_destino": "MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA",
            "dt_chegada_prevista": "02/01/2025 07:45",
            "dt_chegada_real": "02/01/2025 08:11",
            "ds_situacao_voo": "REALIZADO",
            "ds_justificativa": "",
            "dt_referencia": "01/01/2025",
            "ds_situacao_partida": "Antecipado",
            "ds_situacao_chegada": "Pontual",
        }
    ]

    jsonData = json.dumps(body, indent=4)

    with pytest.raises(S3UploadError) as excinfo:
        s3 = S3()
        s3.upload_file(data=jsonData, bucket=bucket_name, key=key)

    e = excinfo.value
    assert e.status_code == 404
    assert e.message == "The specified bucket does not exist"


@mock_aws
def test_get_object_bucket_inexistente():
    bucket_name = "bucket-inexistente"
    key = "staging/2025_10_06_123456789_0.json"

    boto3.client("s3", region_name="us-east-1")

    with pytest.raises(S3GetError) as excinfo:
        s3 = S3()
        s3.get_file(bucket_name=bucket_name, key=key)
    e = excinfo.value

    assert e.status_code == 404
    assert e.message == "The specified bucket does not exist"


@mock_aws
def test_get_object_bucket_success():
    bucket_name = "etl-brazilian-flights"
    key = "staging/2025_10_06_123456789_0.json"
    client = boto3.client("s3", region_name="us-east-1")

    client.create_bucket(Bucket=bucket_name)

    body = [
        {
            "sg_empresa_icao": "AAL",
            "nm_empresa": "AMERICAN AIRLINES, INC.",
            "nr_voo": "0904",
            "cd_di": "0",
            "cd_tipo_linha": "I",
            "sg_equipamento_icao": "B788",
            "nr_assentos_ofertados": "295",
            "sg_icao_origem": "SBGL",
            "nm_aerodromo_origem": "AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL",
            "dt_partida_prevista": "01/01/2025 23:55",
            "dt_partida_real": "01/01/2025 23:50",
            "sg_icao_destino": "KMIA",
            "nm_aerodromo_destino": "MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA",
            "dt_chegada_prevista": "02/01/2025 07:45",
            "dt_chegada_real": "02/01/2025 08:11",
            "ds_situacao_voo": "REALIZADO",
            "ds_justificativa": "",
            "dt_referencia": "01/01/2025",
            "ds_situacao_partida": "Antecipado",
            "ds_situacao_chegada": "Pontual",
        }
    ]

    jsonData = json.dumps(body, indent=4)
    s3 = S3()
    s3.upload_file(data=jsonData, bucket=bucket_name, key=key)

    response = s3.get_file(bucket_name=bucket_name, key=key)

    assert isinstance(response, StreamingBody)


@mock_aws
def test_get_object_response_empty_file():
    bucket_name = "etl-brazilian-flights"
    key = "staging/2025_10_06_123456789_0.json"
    client = boto3.client("s3", region_name="us-east-1")

    client.create_bucket(Bucket=bucket_name)
    s3 = S3()
    s3.upload_file(data=b"", bucket=bucket_name, key=key)

    with pytest.raises(S3EmptyFile) as excinfo:
        s3.get_file(bucket_name=bucket_name, key=key)

    e = excinfo.value
    assert e.message == "Empty File"
