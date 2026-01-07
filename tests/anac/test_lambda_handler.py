import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws
from requests.exceptions import HTTPError

from historical_flights_airport_gym.conectores.anac.lambda_handler import lambda_handler


@mock_aws
@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_success_connect_data_and_upload_s3(mock_get):
    mock_response = MagicMock()
    json_str_response = '[{"sg_empresa_icao":"AAL","nm_empresa":"AMERICAN AIRLINES, INC.","nr_voo":"0904","cd_di":"0","cd_tipo_linha":"I","sg_equipamento_icao":"B788","nr_assentos_ofertados":"295","sg_icao_origem":"SBGL","nm_aerodromo_origem":"AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL","dt_partida_prevista":"01/01/2025 23:55","dt_partida_real":"01/01/2025 23:50","sg_icao_destino":"KMIA","nm_aerodromo_destino":"MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA","dt_chegada_prevista":"02/01/2025 07:45","dt_chegada_real":"02/01/2025 08:11","ds_situacao_voo":"REALIZADO","ds_justificativa":"","dt_referencia":"01/01/2025","ds_situacao_partida":"Antecipado","ds_situacao_chegada":"Pontual"}]'
    mock_response.status_code = 200
    mock_response.json.return_value = json_str_response
    mock_get.return_value = mock_response

    bucket_name = "etl-brazilian-flights"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)

    event = {
        "layer": "bronze",
        "bucket": bucket_name,
        "dt_voo": "01012025",
    }

    context = {}

    result = lambda_handler(event=event, context=context)

    assert result["status"] == "success"
    assert result["bucket"] == bucket_name
    assert result["s3_response"]["status_code"] == 200


@mock_aws
@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_erro_request_api(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = """
    <!DOCTYPE html>
    <html>
      <head><title>The resource cannot be found.</title></head>
      <body>
        <h1>Server Error in '/sas/vra_api' Application.</h1>
        <h2>The resource cannot be found.</h2>
        <p>Requested URL: /sas/vra_api/vra/airport</p>
      </body>
    </html>
    """
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    bucket_name = "etl-brazilian-flights"
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)

    event = {
        "layer": "bronze",
        "bucket": bucket_name,
        "dt_voo": "01012025",
    }

    context = {}

    with pytest.raises(Exception) as excinfo:
        lambda_handler(event=event, context=context)

    e_message = str(excinfo.value)
    assert "APIError" in e_message
    assert "404" in e_message


@mock_aws
@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_upload_s3(mock_get):
    mock_response = MagicMock()
    json_str_response = '[{"sg_empresa_icao":"AAL","nm_empresa":"AMERICAN AIRLINES, INC.","nr_voo":"0904","cd_di":"0","cd_tipo_linha":"I","sg_equipamento_icao":"B788","nr_assentos_ofertados":"295","sg_icao_origem":"SBGL","nm_aerodromo_origem":"AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL","dt_partida_prevista":"01/01/2025 23:55","dt_partida_real":"01/01/2025 23:50","sg_icao_destino":"KMIA","nm_aerodromo_destino":"MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA","dt_chegada_prevista":"02/01/2025 07:45","dt_chegada_real":"02/01/2025 08:11","ds_situacao_voo":"REALIZADO","ds_justificativa":"","dt_referencia":"01/01/2025","ds_situacao_partida":"Antecipado","ds_situacao_chegada":"Pontual"}]'
    mock_response.status_code = 200
    mock_response.json.return_value = json_str_response
    mock_get.return_value = mock_response

    bucket_name = "etl-brazilian-flights"

    boto3.client("s3", region_name="us-east-1")

    event = {
        "layer": "bronze",
        "bucket": bucket_name,
        "dt_voo": "01012025",
    }

    context = {}

    with pytest.raises(Exception) as excinfo:
        lambda_handler(event=event, context=context)

    e_message = str(excinfo.value)

    assert "S3UploadError" in e_message
    assert "bucket does not exist" in e_message


@mock_aws
@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_error_json(mock_get):
    mock_response = MagicMock()
    json_str_response = '[{"sg_empresa_icao'
    mock_response.status_code = 200

    mock_response.json.side_effect = json.JSONDecodeError(
        "Expecting Value", json_str_response, 1
    )

    mock_get.return_value = mock_response
    bucket_name = "etl-brazilian-flights"
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)

    event = {
        "layer": "bronze",
        "bucket": bucket_name,
        "dt_voo": "01012025",
    }

    context = {}

    result = lambda_handler(event=event, context=context)

    assert result["type"] == "JSONProcessingError"
    assert result["status_code"] == 500
    assert result["status"] == "error"


@mock_aws
@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
@patch("historical_flights_airport_gym.conectores.anac.lambda_handler.lambda_handler")
def test_error_inesperado(mock_get, mock_handler):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    mock_unexp = MagicMock()
    mock_unexp.side_effect = Exception("Memory Size")
    mock_handler.return_value = mock_unexp

    bucket_name = "etl-brazilian-flights"
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)

    event = {
        "layer": "bronze",
        "bucket": bucket_name,
        "dt_voo": "01012025",
    }

    context = {}

    result = lambda_handler(event=event, context=context)

    assert result["status"] == "error"
    assert result["type"] == "LambdaError"
    assert result["status_code"] == 500
