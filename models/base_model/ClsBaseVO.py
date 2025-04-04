import numpy as np
from datetime import datetime
from utils.ClsConvert import ClsConvert
from utils.ClsFormat import ClsFormat


class ClsBaseVO:
    def __init__(self, file_path: str, record):
        try:
            self.FILEPATH = file_path
            time_value = record['TIME']
            self.TIME: int = int(time_value)
            self.UTC_TIME = self.get_full_datetime(file_path, self.TIME)
            utc_time = self.parse_utc_time(self.UTC_TIME)
            self.init_by_utc_time(file_path, utc_time)
        except:
            self.TIME = None
            self.UTC_TIME = None

    def init_by_utc_time(self, file_path, utc_time):
        self.UTC_TIME_YEAR = utc_time.year
        self.UTC_TIME_MONTH = utc_time.month
        self.UTC_TIME_DAY = utc_time.day
        self.UTC_TIME_HOUR = utc_time.hour
        self.UTC_TIME_MINUTE = utc_time.minute
        self.UTC_TIME_SECOND = utc_time.second
        self.UTC_TIME_MILLISECOND = 0 #str(int(utc_time.microsecond / 1000)).zfill(3)


    @staticmethod
    def get_full_datetime(file_path: str, time_value: int) -> datetime:
        return ClsConvert.get_full_datetime(file_path, time_value)

    @staticmethod
    def parse_utc_time(utc_time) -> datetime:
        if '.' in str(utc_time):
            return datetime.strptime(str(utc_time), "%Y-%m-%d %H:%M:%S.%f")
        else:
            return datetime.strptime(str(utc_time), "%Y-%m-%d %H:%M:%S")

    def to_dict(self):
        dict_representation = self.__dict__.copy()
        dict_representation['FILEPATH'] = ClsFormat.format_file_path(str(dict_representation['FILEPATH']))

        # Converte tipos numpy para tipos nativos do Python
        for key, value in dict_representation.items():
            if isinstance(value, (np.integer, np.int32, np.int64)):
                dict_representation[key] = int(value)
            elif isinstance(value, (np.floating, np.float32, np.float64)):
                dict_representation[key] = float(value)
            elif isinstance(value, np.ndarray):
                dict_representation[key] = value.tolist()
            elif isinstance(value, datetime):
                # Garante que UTC_TIME permanece como um objeto datetime
                dict_representation[key] = value
        return dict_representation
