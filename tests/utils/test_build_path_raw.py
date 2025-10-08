import re

from historical_flights_airport_gym.utils.build_path import path_file_raw


def is_valid_filename(filename):
    pattern = r"^\d{4}_\d{2}_\d{2}_\d{13}"
    return bool(re.match(pattern, filename))


def test_format_file_is_correct():
    filename = path_file_raw()
    assert is_valid_filename(filename=filename)
