import datetime
import time


def path_file_raw():
    today = datetime.datetime.today()
    today_str = today.strftime("%Y_%m_%d")
    timestamp_ms = int(time.time() * 1000)
    return f"{today_str}_{timestamp_ms}"
