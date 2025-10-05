from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError, RequestException, Timeout

from historical_flights_airport_gym.utils.get_data import RequestError, get_data


@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_status_200_get_request(mock_get):
    mock_response = MagicMock()
    json_str_response = '[{"sg_empresa_icao":"AAL","nm_empresa":"AMERICAN AIRLINES, INC.","nr_voo":"0904","cd_di":"0","cd_tipo_linha":"I","sg_equipamento_icao":"B788","nr_assentos_ofertados":"295","sg_icao_origem":"SBGL","nm_aerodromo_origem":"AEROPORTO INTERNACIONAL DO RIO DE JANEIRO (GALEÃO) - ANTONIO CARLOS JOBIM - RIO DE JANEIRO - RJ - BRASIL","dt_partida_prevista":"01/01/2025 23:55","dt_partida_real":"01/01/2025 23:50","sg_icao_destino":"KMIA","nm_aerodromo_destino":"MIAMI INTERNATIONAL AIRPORT - MIAMI, FLORIDA - ESTADOS UNIDOS DA AMÉRICA","dt_chegada_prevista":"02/01/2025 07:45","dt_chegada_real":"02/01/2025 08:11","ds_situacao_voo":"REALIZADO","ds_justificativa":"","dt_referencia":"01/01/2025","ds_situacao_partida":"Antecipado","ds_situacao_chegada":"Pontual"}]'
    mock_response.status_code = 200
    mock_response.json.return_value = json_str_response
    mock_get.return_value = mock_response

    url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
    params = {"dt_voo": "01012025"}

    response = get_data(url=url, params=params)

    assert response.status_code == 200
    assert isinstance(response.json(), str)


@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_status_404_error_rout_api(mock_get):
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
    url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
    params = {"dt_voo": "01012025"}
    with pytest.raises(RequestError) as excinfo:
        get_data(url=url, params=params)

    e = excinfo.value
    assert e.status_code == 404
    assert "The resource cannot be found" in e.response_body


@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_error_timeout(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Timeout(response=mock_response)
    mock_get.return_value = mock_response
    url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
    params = {"dt_voo": "01012025"}

    with pytest.raises(RequestError) as excinfo:
        get_data(url=url, params=params)

    e = excinfo.value
    assert e.status_code is None
    assert "Erro de Timeout" == e.response_body


@patch("historical_flights_airport_gym.utils.get_data.requests.Session.get")
def test_erro_generico(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = RequestException(
        response=mock_response
    )
    mock_get.return_value = mock_response
    url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
    params = {"dt_voo": "01012025"}
    with pytest.raises(RequestError) as excinfo:
        get_data(url=url, params=params)

    e = excinfo.value
    assert e.status_code is None
    assert "Erro Generico de Requisicao" in e.response_body
