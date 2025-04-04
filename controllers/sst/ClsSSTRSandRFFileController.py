import os
from controllers.logs.ClsLoggerController import ClsLoggerController
from repositories.sst.ClsRFandRSFileRepository import ClsRFandRSFileRepository
from services.ClsRFandRSFileService import ClsRFandRSFileService


class ClsRFandRSFileController:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.prefix = os.path.basename(file_path)[:2]
        self.dtype = ClsRFandRSFileRepository.get_dtype(self.prefix)
        self.records = []

    @staticmethod
    def process_file(file_path):
        # controller = ClsRFandRSFileController(file_path)
        service = ClsRFandRSFileService(file_path)
        service.process_records()
        ClsLoggerController.write_file_read(file_path, str(len(service.records)))
        service.insert_records_to_mongodb()
