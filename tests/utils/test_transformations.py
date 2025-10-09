import json
from unittest.mock import MagicMock, patch

import pytest

from historical_flights_airport_gym.utils.get_data import get_data
from historical_flights_airport_gym.utils.transformations import (
    JsonProcessingError,
    from_str_to_json,
)


@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_conv_str_json_funciona(mock_get):
    mock_response = MagicMock()
    json_str_response = '[{"sg_empresa_icao":"AAL","nm_empresa":"AMERICAN AIRLINES, INC.","nr_voo":"0904","cd_di":"0","cd_tipo_linha":"I","sg_equipamento_icao":"B788","nr_assentos_ofertados":"295","sg_icao_origem":"SBGL","nm_aerodromo_origem":"AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL","dt_partida_prevista":"01/01/2025 23:55","dt_partida_real":"01/01/2025 23:50","sg_icao_destino":"KMIA","nm_aerodromo_destino":"MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA","dt_chegada_prevista":"02/01/2025 07:45","dt_chegada_real":"02/01/2025 08:11","ds_situacao_voo":"REALIZADO","ds_justificativa":"","dt_referencia":"01/01/2025","ds_situacao_partida":"Antecipado","ds_situacao_chegada":"Pontual"}]'
    mock_response.status_code = 200
    mock_response.json.return_value = json_str_response
    mock_get.return_value = mock_response

    url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
    params = {"dt_voo": "01012025"}

    response = get_data(url=url, params=params)
    responseJson = from_str_to_json(response=response)

    assert isinstance(responseJson, list)


@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_conv_str_json_error_transf(mock_get):
    mock_response = MagicMock()
    json_str_response = '[{"sg_empresa_icao":'
    mock_response.status_code = 200

    mock_response.json.side_effect = json.JSONDecodeError(
        "Expecting Value", json_str_response, 1
    )
    mock_get.return_value = mock_response

    url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
    params = {"dt_voo": "01012025"}

    response = get_data(url=url, params=params)

    with pytest.raises(JsonProcessingError) as excinfo:
        from_str_to_json(response=response)

    e = excinfo.value

    assert e.status_code == 500
    assert "Expecting Value" in e.response_body
