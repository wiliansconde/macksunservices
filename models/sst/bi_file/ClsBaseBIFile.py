import os
from abc import ABC, abstractmethod

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from services.ClsLoggerService import ClsLoggerService
from utils.ClsConsolePrint import CLSConsolePrint
from utils.ClsFormat import ClsFormat


class ClsBaseBIFile(ABC):

    def __init__(self, input_file: str):
        """
        Inicializa a classe com o caminho do arquivo de entrada e cria uma lista para armazenar os registros processados.
        :param input_file: Caminho do arquivo de entrada.
        """
        self.input_file = input_file  # Caminho do arquivo de entrada
        self.records: list = []  # Lista para armazenar os registros processados
        self.TELESCOPE = 'SST'
    @abstractmethod
    def read_records_with_numpy(self, input_file: str, prefix: str):
        """
        Método abstrato para leitura de registros com numpy.
        Este método deve ser implementado pelas classes filhas.
        :param input_file: Caminho do arquivo de entrada.
        :param prefix: Prefixo do nome do arquivo.
        """
        pass

    def insert_records_to_mongodb(self, file_path) -> None:
        """
        Insere todos os registros processados no MongoDB em lotes.
        """
        batch_size = 5000  # Tamanho do lote para inserções em massa
        for i in range(0, len(self.records), batch_size):
            batch = self.records[i:i + batch_size]
            res = ClsMongoHelper.insert_vos_to_mongodb(batch, ClsSettings.MONGO_COLLECTION_DATA_SST_BI_FILE_1S, file_path)
            print('1 - ' + res.file_path)
            file_path_to_log = ClsFormat.format_file_path(res.file_path)
            ClsLoggerService.write_lines_inserted(file_path_to_log, res.inserted_count)
            if res.duplicate_count > 0:
                ClsLoggerService.write_duplicate_lines(file_path_to_log, res.duplicate_count)
            if res.failed_count > 0:
                ClsLoggerService.write_failed_lines(file_path_to_log, res.failed_count)
            CLSConsolePrint.debug(f"Lote de {len(batch)} registros inserido com sucesso.")

    def process_records(self) -> None:
        """
        Processa os registros lidos do arquivo de entrada.
        """
        # Extrai o prefixo do nome do arquivo
        prefix = os.path.basename(self.input_file)[:2]
        # Lê e formata os registros, armazenando-os na lista de registros processados
        self.records = self.read_records_with_numpy(self.input_file, prefix)
