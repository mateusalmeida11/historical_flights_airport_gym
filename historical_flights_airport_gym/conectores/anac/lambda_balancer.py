from historical_flights_airport_gym.utils.transformations import (
    DateTransformationError,
    from_str_to_datetime,
)


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
        from_str_to_datetime(date=start_str_date)
        from_str_to_datetime(date=end_str_date)
    except DateTransformationError as e:
        return {
            "status": "error",
            "type": "DateTransformationError",
            "status_code": 500,
            "message": str(e),
        }
