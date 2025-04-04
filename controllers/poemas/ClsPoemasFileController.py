from services.ClsPoemasFileService import ClsPoemasFileService
from utils.ClsTrace import ClsTrace


class ClsPoemasFileController:
    @staticmethod
    def process_file(file_path):
        ClsPoemasFileService.process_file(file_path)
