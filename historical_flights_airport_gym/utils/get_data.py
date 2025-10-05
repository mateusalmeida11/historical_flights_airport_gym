import requests


class RequestError(Exception):
    def __init__(self, message, status_code=None, endpoint=None, response_body=None):
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint
        self.response_body = response_body


def get_data(url, params):
    http = requests.Session()
    response = http.get(url=url, params=params)
    return response
