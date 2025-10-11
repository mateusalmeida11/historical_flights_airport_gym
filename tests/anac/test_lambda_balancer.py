from datetime import datetime

import pytest

from historical_flights_airport_gym.conectores.anac.lambda_balancer import (
    lambda_handler,
)
from historical_flights_airport_gym.utils.transformations import (
    DateTransformationError,
    from_str_to_datetime,
)


def test_date_initial_greater_than_8_dig():
    start_str_date = "01012025"
    end_str_date = "320120253"

    event = {"date_start": start_str_date, "date_end": end_str_date}

    context = {}

    result = lambda_handler(event=event, context=context)

    assert result["status"] == "error"
    assert result["type"] == "InvalidDateLenghtError"
    assert result["status_code"] == 500
    assert (
        result["message"]
        == "Objeto de Data Deve Conter Exatamente 8 caracteres  no formato DDMMYYY"
    )


def test_funcao_de_transformacao_str_para_data_funciona():
    start_date = "01012025"
    obj_convertido = from_str_to_datetime(date=start_date)

    assert isinstance(obj_convertido, datetime)


def test_funcao_de_transformacao_str_para_data_erro_date_out_range():
    start_date = "33012025"
    date_str_formated = f"{start_date[:2]}/{start_date[2:4]}/{start_date[4:]}"

    with pytest.raises(DateTransformationError) as excinfo:
        from_str_to_datetime(date=start_date)

    e = excinfo.value

    assert e.message == f"day is out of range for month: {date_str_formated}"


def test_funcao_de_transformacao_str_para_data_erro_parser():
    start_date = "aa012025"
    date_str_formated = f"{start_date[:2]}/{start_date[2:4]}/{start_date[4:]}"

    with pytest.raises(DateTransformationError) as excinfo:
        from_str_to_datetime(date=start_date)

    e = excinfo.value

    assert e.message == f"Unknown string format: {date_str_formated}"


def test_date_out_of_range_lambda():
    start_str_date = "01012025"
    end_str_date = "32012025"

    date_str_formated = f"{end_str_date[:2]}/{end_str_date[2:4]}/{end_str_date[4:]}"

    event = {"date_start": start_str_date, "date_end": end_str_date}
    context = {}

    result = lambda_handler(event=event, context=context)

    assert result["status"] == "error"
    assert result["type"] == "DateTransformationError"
    assert result["status_code"] == 500
    assert result["message"] == f"day is out of range for month: {date_str_formated}"


def test_lambda_data_inicial_maior_que_final():
    start_str_date = "01012025"
    end_str_date = "32012025"

    event = {"date_start": start_str_date, "date_end": end_str_date}
    context = {}

    result = lambda_handler(event=event, context=context)

    assert result["status"] == "error"
    assert result["type"] == "IntervaloDataInvalido"
    assert result["status_code"] == 500
    assert result["message"] == "Data Inicial Maior do que a Final"
