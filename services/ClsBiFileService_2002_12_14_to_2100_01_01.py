import os
import numpy as np

from models.sst.bi_file.ClsBIVO_2002_12_14_to_2100_01_01 import ClsBIVO_2002_12_14_to_2100_01_01
from models.sst.bi_file.ClsBaseBIFile import ClsBaseBIFile
from models.sst.utils.ClsSSTFileFormat import ClsSSTFileFormat


class ClsBiFileService_2002_12_14_to_2100_01_01(ClsBaseBIFile):

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
            raise ValueError("Prefixo desconhecido")  # Levanta um erro se o prefixo não for reconhecido

        # Calcula o tamanho do registro baseado no prefixo
        record_size = self.calculate_record_size(prefix)
        # Obtém o tamanho do arquivo em bytes
        file_size = os.path.getsize(file_path)
        # Calcula o número de registros no arquivo
        num_records = file_size // record_size
        # Lê os registros do arquivo usando NumPy
        raw_records = np.fromfile(file_path, dtype=dtype, count=num_records)

        # Converte os registros brutos em uma lista de objetos clsBIVO após formatar
        formatted_records = []
        for record in raw_records:
            # Formata o registro bruto
            formatted_record = ClsSSTFileFormat.format_record_2002_12_14_to_2100_01_01(record)
            # Verifica se TIME > 0 antes de adicionar à lista
            if formatted_record[0] > 0:
                # Cria um objeto clsBIVO com o registro formatado e adiciona à lista
                formatted_records.append(ClsBIVO_2002_12_14_to_2100_01_01(file_path, formatted_record))
        return formatted_records
