import json

import requests
from dateutil import parser


class JsonProcessingError(Exception):
    def __init__(self, message, status_code=None, response_body=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class DateTransformationError(Exception):
    def __init__(self, message, date_str):
        super().__init__(message)
        self.message = message


def from_str_to_json(response: requests.Response):
    try:
        jsonStr = response.json()
        return json.loads(jsonStr)
    except json.JSONDecodeError as e:
        raise JsonProcessingError(
            "Erro ao Processar Json", status_code=500, response_body=str(e)
        ) from e


def from_str_to_datetime(date: str):
    try:
        date_str_formated = f"{date[:2]}/{date[2:4]}/{date[4:]}"
        date_convertida = parser.parse(date_str_formated, dayfirst=True)
        return date_convertida
    except ValueError as e:
        raise DateTransformationError(str(e), date_str=date) from e


def add_metadata_to_json(records):
    record_count = len(records)
    return {"metadata": {"recourd_count": record_count}, "content": records}
