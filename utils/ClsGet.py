import os
from datetime import datetime, timezone, timedelta

from utils.ClsConvert import ClsConvert


class ClsGet:
    @staticmethod
    def current_time():
        brazil_timezone = timezone(timedelta(hours=-3))  # Horário padrão do Brasil (BRT)
        return datetime.now(tz=brazil_timezone)

    @staticmethod
    def get_file_size(file_path):
        return ClsConvert.convert_bytes_to_mb(os.path.getsize(file_path))