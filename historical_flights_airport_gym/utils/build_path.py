import datetime
import time
from pathlib import Path


class RootPathDoesntExist(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def path_file_raw():
    today = datetime.datetime.today()
    today_str = today.strftime("%Y_%m_%d")
    timestamp_ms = int(time.time() * 1000)
    return f"{today_str}_{timestamp_ms}"


def get_root_path(project_name="historical_flights_airport_gym"):
    path = Path(__file__).resolve()
    for parent in path.parents:
        if parent.name == project_name:
            return parent
    raise RootPathDoesntExist(f"Diretorio do Projeto {project_name} nao encontrado")


def build_path_data_quality_check_file(root_path: Path, source_file: str):
    return root_path / "soda" / "sources" / source_file
