import json

import requests


class JsonProcessingError(Exception):
    def __init__(self, message, status_code=None, response_body=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


def from_str_to_json(response: requests.Response):
    try:
        jsonStr = response.json()
        return json.loads(jsonStr)
    except requests.exceptions.JSONDecodeError as e:
        raise JsonProcessingError(
            "Erro ao Processar Json", status_code=500, response_body=e
        ) from e
