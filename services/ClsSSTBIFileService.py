import time
from datetime import datetime
from services.ClsBiFileService_1900_01_01_to_2002_09_15 import ClsBiFileService_1900_01_01_to_2002_09_15
from services.ClsBiFileService_2002_09_16_to_2002_11_23 import ClsBiFileService_2002_09_16_to_2002_11_23
from services.ClsBiFileService_2002_11_24_to_2002_12_13 import ClsBiFileService_2002_11_24_to_2002_12_13
from services.ClsBiFileService_2002_12_14_to_2100_01_01 import ClsBiFileService_2002_12_14_to_2100_01_01
from models.sst.utils.ClsSSTFileGetCommonProperties import ClsSSTFileGetCommonProperties
from services.ClsLoggerService import ClsLoggerService


class ClsSSTBIFileService:
    @staticmethod
    def process_file(input_file) -> int:

        bi_file_properties = ClsSSTFileGetCommonProperties(input_file)
        file_date = bi_file_properties.get_file_date()

        date_2002_12_14 = datetime.strptime('2002-12-14 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')
        date_2002_11_24 = datetime.strptime('2002-11-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')
        date_2002_09_16 = datetime.strptime('2002-09-16 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')

        if file_date >= date_2002_12_14:
            bi_file_service = ClsBiFileService_2002_12_14_to_2100_01_01(input_file)
        elif file_date >= date_2002_11_24:
            bi_file_service = ClsBiFileService_2002_11_24_to_2002_12_13(input_file)
        elif file_date >= date_2002_09_16:
            bi_file_service = ClsBiFileService_2002_09_16_to_2002_11_23(input_file)
        else:
            bi_file_service = ClsBiFileService_1900_01_01_to_2002_09_15(input_file)

        return ClsSSTBIFileService._process_and_insert(bi_file_service, input_file)

    @staticmethod
    def _process_and_insert(bi_file_service, input_file) -> int:
        try:
            bi_file_service.process_records()
            file_lines_qty = len(bi_file_service.records)
            bi_file_service.insert_records_to_mongodb(input_file)
            return file_lines_qty
        except Exception as e:
            print(f"Erro ao processar e inserir registros: {e}")
