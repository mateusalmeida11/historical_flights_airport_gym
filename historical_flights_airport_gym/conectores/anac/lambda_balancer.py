from datetime import datetime, timedelta

from historical_flights_airport_gym.utils.transformations import (
    DateTransformationError,
    from_str_to_datetime,
)


class IntervaloDataInvalido(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def criacao_range_data(date_ini: datetime, date_end: datetime):
    if date_ini > date_end:
        raise IntervaloDataInvalido("Data Inicial Maior do que a Final")

    num_dias = (date_end - date_ini).days + 1
    list_datas = [date_ini + timedelta(i) for i in range(num_dias)]
    return list_datas


def lambda_handler(event, context):
    start_str_date = event.get("date_start")
    end_str_date = event.get("date_end")

    if len(start_str_date) != 8 or len(end_str_date) != 8:
        return {
            "status": "error",
            "type": "InvalidDateLenghtError",
            "status_code": 500,
            "message": "Objeto de Data Deve Conter Exatamente 8 caracteres  no formato DDMMYYY",
        }

    try:
        date_ini = from_str_to_datetime(date=start_str_date)
        date_end = from_str_to_datetime(date=end_str_date)
        list_datas = criacao_range_data(date_ini=date_ini, date_end=date_end)
        list_data_str = [date_str.strftime("%d%m%Y") for date_str in list_datas]
        return {"status": "success", "status_code": 200, "data": list_data_str}
    except DateTransformationError as e:
        return {
            "status": "error",
            "type": "DateTransformationError",
            "status_code": 500,
            "message": str(e),
        }
    except IntervaloDataInvalido as e:
        return {
            "status": "error",
            "type": "IntervaloDataInvalido",
            "status_code": 500,
            "message": str(e),
        }
