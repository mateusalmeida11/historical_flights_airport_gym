import json

import requests


def from_str_to_json(response: requests.Response):
    jsonStr = response.json()
    return json.loads(jsonStr)
