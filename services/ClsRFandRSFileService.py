import os  # Biblioteca para operações de sistema de arquivos
import pandas as pd  # Biblioteca Pandas para manipulação de dados em DataFrames

from config.ClsSettings import ClsSettings
from models.sst.rs_rf_file.ClsRFandRSFileVO import ClsRFandRSFileVO
from models.sst.utils.ClsSSTFileFormat import ClsSSTFileFormat

from services.ClsLoggerService import ClsLoggerService
from repositories.sst.ClsRFandRSFileRepository import ClsRFandRSFileRepository
from utils.ClsConsolePrint import CLSConsolePrint


class ClsRFandRSFileService:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.prefix = os.path.basename(file_path)[:2]  # Extrai o prefixo do nome do arquivo
        self.dtype = ClsRFandRSFileRepository.get_dtype(self.prefix)
        self.records: list = []  # Lista para armazenar os registros processados

    @staticmethod
    def process_file(file_path) -> int:
        service = ClsRFandRSFileService(file_path)
        service.process_records()
        service.insert_records_to_mongodb()
        return len(service.records)
    def process_records(self) -> None:
        records = ClsRFandRSFileRepository.read_records(self.file_path, self.dtype)
        df = pd.DataFrame(records)
        self.records = df.apply(
            lambda row: ClsSSTFileFormat.format_rs_rf_file_record(ClsRFandRSFileVO(self.file_path, row.to_dict())), axis=1
        )

    def insert_records_to_mongodb(self) -> str:
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT  # Tamanho do lote para inserções em massa
        mongo_collection = ClsSettings.get_mongo_collection_name_by_file_type(self.file_path)

        for i in range(0, len(self.records), batch_size):
            batch = self.records[i:i + batch_size]
            res = ClsRFandRSFileRepository.insert_records(batch, self.file_path)

            ClsLoggerService.write_processing_batch(self.file_path, batch_size, mongo_collection)
            if res.failed_count > 0:
                ClsLoggerService.write_failed_lines(self.file_path, res.failed_count)
            if res.duplicate_count > 0:
                ClsLoggerService.write_duplicate_lines(self.file_path, res.duplicate_count)

            ClsLoggerService.write_lines_inserted(self.file_path, res.inserted_count)
            CLSConsolePrint.debug(f"Lote de {len(batch)} registros inserido com sucesso.")

        return mongo_collection
