import os
import re


class ClsFormat:
    def fromFloat2Decimals(num):
        return "{:.2f}".format(num)

    def from_float_4_decimals(num):
        return "{:.4f}".format(num)

    def from_float_none_decimals(num):
        return int(num)

    def from_int_to_hhmmssss(secs):
        # Calcular hr
        hr = secs // 3600
        # Calcular minutos restantes
        minleft = (secs % 3600) // 60
        # Calcular segundos restantes
        secleft = (secs % 3600) % 60

        return "{:02d}:{:02d}:{:02d}".format(hr, minleft, secleft)

    @staticmethod
    def format_file_path(file_path) -> str:
        # Ajusta o FILEPATH para começar a partir do primeiro ano completo após 1999
        match = re.search(r'(19[9][9]|20\d{2})', file_path)
        if match:
            year = match.group(0)
            year_index = file_path.index(match.group(0))
            file_path = file_path[year_index:]
            # Substitui a primeira ocorrência do ano e remove duplicidades
            file_path = file_path.replace(year, '', 1).lstrip('\\/')
        return file_path

    @staticmethod
    def format_and_sanitize_path_and_remove_prefix(file_path):
        if ":" in file_path:
            file_path = ClsFormat.format_file_path(file_path)
        if file_path.endswith(('.zip', '.gz')):
            file_path = os.path.splitext(file_path)[0]

        return file_path

    @staticmethod
    def format_rs_rf_file_record(record):
        # Implement the formatting logic
        return record
