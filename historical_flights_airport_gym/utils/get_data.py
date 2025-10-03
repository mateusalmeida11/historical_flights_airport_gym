import requests


def get_data(url, params):
    http = requests.Session()
    response = http.get(url=url, params=params)
    return response
