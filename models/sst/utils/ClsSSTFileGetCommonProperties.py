import os
import numpy as np
from datetime import datetime

from models.sst.bi_file.ClsBIVO_2002_12_14_to_2100_01_01 import ClsBIVO_2002_12_14_to_2100_01_01
from models.sst.utils.ClsSSTFileFormat import ClsSSTFileFormat
from utils.ClsConvert import ClsConvert


class ClsSSTFileGetCommonProperties:
    """
    Classe responsável por ler e processar registros de um arquivo 'bi'.
    """

    def __init__(self, input_file: str):
        """
        Inicializa a classe com o caminho do arquivo de entrada e cria uma lista para armazenar os registros processados.
        :param input_file: Caminho do arquivo de entrada.
        """
        self.input_file = input_file  # Caminho do arquivo de entrada
        self.records: list = []  # Lista para armazenar os registros processados

    def calculate_record_size(self, prefix: str) -> int:
        """
        Calcula o tamanho do registro com base no prefixo do arquivo.
        :param prefix: Prefixo do arquivo ('bi' neste caso).
        :return: Tamanho do registro em bytes.
        """
        if prefix == "bi":
            return 123  # Tamanho do registro para arquivos com prefixo 'bi'
        else:
            raise ValueError("Prefixo desconhecido")  # Levanta um erro se o prefixo não for reconhecido

    def read_records_with_numpy(self, file_path: str, prefix: str) -> list:
        """
        Lê os registros do arquivo usando NumPy, formata cada registro e converte em objetos clsBIVO.

        :param file_path: Caminho do arquivo de entrada.
        :param prefix: Prefixo do arquivo ('bi' neste caso).
        :return: Lista de objetos clsBIVO.
        """
        if prefix == "bi":
            # Define o formato de dados esperado para os registros do arquivo 'bi'
            dtype = np.dtype([
                ('TIME', np.int32),  # 4 bytes
                ('AZIPOS', np.float32),  # 4 bytes
                ('ELEPOS', np.float32),  # 4 bytes
                ('AZIERR', np.float32),  # 4 bytes
                ('ELEERR', np.float32),  # 4 bytes
                ('ADC_1', np.uint16),  # 2 bytes
                ('ADC_2', np.uint16),  # 2 bytes
                ('ADC_3', np.uint16),  # 2 bytes
                ('ADC_4', np.uint16),  # 2 bytes
                ('ADC_5', np.uint16),  # 2 bytes
                ('ADC_6', np.uint16),  # 2 bytes
                ('SIGMA_1', np.float32),  # 4 bytes
                ('SIGMA_2', np.float32),  # 4 bytes
                ('SIGMA_3', np.float32),  # 4 bytes
                ('SIGMA_4', np.float32),  # 4 bytes
                ('SIGMA_5', np.float32),  # 4 bytes
                ('SIGMA_6', np.float32),  # 4 bytes
                ('GPS_STATUS', np.int16),  # 2 bytes
                ('ACQ_GAIN', np.int16),  # 2 bytes
                ('TARGET', np.int8),  # 1 byte
                ('OPMODE', np.int8),  # 1 byte
                ('OFF_1', np.int16),  # 2 bytes
                ('OFF_2', np.int16),  # 2 bytes
                ('OFF_3', np.int16),  # 2 bytes
                ('OFF_4', np.int16),  # 2 bytes
                ('OFF_5', np.int16),  # 2 bytes
                ('OFF_6', np.int16),  # 2 bytes
                ('HOT_TEMP', np.float32),  # 4 bytes
                ('AMB_TEMP', np.float32),  # 4 bytes
                ('OPT_TEMP', np.float32),  # 4 bytes
                ('IF_BOARD_TEMP', np.float32),  # 4 bytes
                ('RADOME_TEMP', np.float32),  # 4 bytes
                ('HUMIDITY', np.float32),  # 4 bytes
                ('TEMPERATURE', np.float32),  # 4 bytes
                ('OPAC_210', np.float32),  # 4 bytes
                ('OPAC_405', np.float32),  # 4 bytes
                ('ELEVATION', np.float32),  # 4 bytes
                ('PRESSURE', np.float32),  # 4 bytes
                ('BURST', np.int8),  # 1 byte
                ('ERRORS', np.int32)  # 4 bytes
            ])
        else:
            raise ValueError("Prefixo desconhecido")

        raw_records = np.fromfile(file_path, dtype=dtype, count=5)

        # Converte os registros brutos em uma lista de objetos clsBIVO após formatar
        formatted_records = []
        for record in raw_records:
            formatted_record = ClsSSTFileFormat.format_record_2002_12_14_to_2100_01_01(record)
            if formatted_record[0] > 0:
                formatted_records.append(ClsBIVO_2002_12_14_to_2100_01_01(file_path, formatted_record))
        return formatted_records

    def process_records(self) -> None:
        """
        Processa os registros lidos do arquivo de entrada.
        """
        # Extrai o prefixo do nome do arquivo
        prefix = os.path.basename(self.input_file)[:2]
        # Lê e formata os registros, armazenando-os na lista de registros processados
        self.records = self.read_records_with_numpy(self.input_file, prefix)

    def get_file_date(self) -> datetime:
        """
        Retorna o valor da propriedade TIME do primeiro registro processado como um objeto datetime em UTC.
        :return: Valor da propriedade TIME do primeiro registro como datetime, ou None se não houver registros.
        """
        # Verifica se a lista de registros está vazia
        if not self.records:
            self.process_records()  # Processa os registros se ainda não foram processados

        # Retorna a propriedade TIME do primeiro registro, se disponível
        if self.records:
            time_value = self.records[0].TIME
            full_datetime = ClsConvert.get_full_datetime(self.input_file, time_value)
            return full_datetime
