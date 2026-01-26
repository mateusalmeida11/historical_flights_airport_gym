import re

import pytest

from historical_flights_airport_gym.utils.build_path import (
    RootPathDoesntExist,
    build_path_data_quality_check_file,
    get_root_path,
    path_file_raw,
)


def is_valid_filename(filename):
    pattern = r"^\d{4}_\d{2}_\d{2}_\d{13}"
    return bool(re.match(pattern, filename))


def test_format_file_is_correct():
    filename = path_file_raw()
    assert is_valid_filename(filename=filename)


def test_to_find_path_root_correctly():
    root_path = get_root_path()
    assert (
        str(root_path)
        == "/Users/mateusalmeida/Desktop/projetos/historical_flights_airport_gym/historical_flights_airport_gym"
    )


def test_to_find_path_with_error():
    project_name = "task"
    with pytest.raises(RootPathDoesntExist) as excinfo:
        get_root_path(project_name=project_name)

    e = excinfo.value

    assert f"Diretorio do Projeto {project_name} nao encontrado" in e.message


def test_to_build_path_data_quality_yml():
    source_file = "staging_bronze_check.yml"
    check_file = build_path_data_quality_check_file(source_file)

    assert (
        check_file
        == "/Users/mateusalmeida/Desktop/projetos/historical_flights_airport_gym/historical_flights_airport_gym/soda/sources/staging_bronze_check.yml"
    )
