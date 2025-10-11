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
