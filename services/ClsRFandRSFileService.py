import os  # Biblioteca para operações de sistema de arquivos
import struct
from datetime import datetime
from sys import prefix

import numpy as np
import pandas as pd  # Biblioteca Pandas para manipulação de dados em DataFrames
from matplotlib.widgets import EllipseSelector

from config.ClsSettings import ClsSettings
from controllers.partitioning.ClsPartition_map_controller import ClsPartitionMapController
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from models.sst.rs_rf_file.ClsRFandRSFileVO import ClsRFandRSFileVO
from models.sst.utils.ClsSSTFileFormat import ClsSSTFileFormat
from services.ClsDataAvailabilityStatsService import ClsDataAvailabilityStatsService

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
    def dump_hex_from_file(file_path, num_records=5, record_size=64):
        with open(file_path, "rb") as f:
            raw_bytes = f.read(num_records * record_size)

        print(f"\n[DEBUG] Hex dump dos primeiros {num_records} registros ({num_records * record_size} bytes):\n")
        hex_str = ' '.join(f"{b:02X}" for b in raw_bytes)

        # Exibir organizado em blocos de 16 bytes (como um hexdump)
        for i in range(0, len(hex_str), 16 * 3):  # 3 chars por byte (2 hex + espaço)
            print(hex_str[i:i + 16 * 3])

    import struct
    import os

    def export_sst_fast_file_one_line_per_record(file_path: str, num_records_to_print: int = 1000):
        """
        Faz a leitura de um arquivo FAST (.rf) do SST, extrai os 27 campos por registro, e exporta para um .txt,
        com exatamente uma linha por registro (sem separadores, apenas os números colados).

        :param file_path: Caminho completo do arquivo .rf
        :param num_records_to_print: Quantos registros processar (opcional, default = 1000)
        """
        if not os.path.exists(file_path):
            print(f"[ERRO] Arquivo não encontrado: {file_path}")
            return

        record_format = '<i6H3i2h2i2h6h2bhi'  # Little endian, 64 bytes
        record_size = 64
        unpacker = struct.Struct(record_format)

        file_size = os.path.getsize(file_path)
        total_records = file_size // record_size

        output_txt_path = file_path + ".txt"

        with open(output_txt_path, 'w', encoding='utf-8') as output_file, open(file_path, 'rb') as f:
            records_to_process = min(num_records_to_print, total_records)
            print(f"[INFO] Exportando {records_to_process} registros de {file_path} para {output_txt_path}...")

            for i in range(records_to_process):
                bytes_data = f.read(record_size)
                if len(bytes_data) < record_size:
                    print(f"[WARN] Fim do arquivo encontrado no registro {i}.")
                    break

                fields = unpacker.unpack(bytes_data)
                line = ''.join(str(value) for value in fields)
                output_file.write(line + '\n')

        print(f"[INFO] Exportação finalizada: {output_txt_path}")

    import numpy as np
    import os

    import numpy as np
    import pandas as pd
    import os

    def read_and_validate_rf_rs_file(file_path: str, output_preview: bool = True, output_csv: bool = True):
        # Define o dtype conforme seu backup original
        dtype = np.dtype([
            ('TIME', np.int32),
            ('ADCVAL_1', np.uint16),
            ('ADCVAL_2', np.uint16),
            ('ADCVAL_3', np.uint16),
            ('ADCVAL_4', np.uint16),
            ('ADCVAL_5', np.uint16),
            ('ADCVAL_6', np.uint16),
            ('POS_TIME', np.int32),
            ('AZIPOS', np.int32),
            ('ELEPOS', np.int32),
            ('PM_DAZ', np.int16),
            ('PM_DEL', np.int16),
            ('AZIERR', np.int32),
            ('ELEERR', np.int32),
            ('X_OFF', np.int16),
            ('Y_OFF', np.int16),
            ('OFF_1', np.int16),
            ('OFF_2', np.int16),
            ('OFF_3', np.int16),
            ('OFF_4', np.int16),
            ('OFF_5', np.int16),
            ('OFF_6', np.int16),
            ('TARGET', np.int8),
            ('OPMODE', np.int8),
            ('GPS_STATUS', np.int16),
            ('RECNUM', np.int32),
        ])

        record_size = 64  # Tamanho fixo do registro

        # Verificar tamanho do arquivo
        file_size = os.path.getsize(file_path)
        num_records = file_size // record_size
        print(f"Tamanho do arquivo: {file_size} bytes")
        print(f"Registros esperados (baseado em 64 bytes por registro): {num_records}")

        # Leitura com numpy
        data = np.fromfile(file_path, dtype=dtype, count=num_records)

        # Converter para DataFrame para validar visualmente
        df = pd.DataFrame(data)

        # Preview
        if output_preview:
            print("\n=== Preview dos primeiros registros ===\n")
            print(df.head(10))

        # Salvar como CSV se desejar
        if output_csv:
            output_file = file_path + "_preview.csv"
            df.to_csv(output_file, index=False)
            print(f"\nArquivo CSV de preview gerado: {output_file}")

        return df

    @staticmethod
    def process_file(file_path) -> int:
        #service = ClsRFandRSFileService(file_path)
        #service.process_records()
        #service.insert_records_to_mongodb()
        #ClsRFandRSFileService.debug_read_sst_file(file_path)
        #ClsRFandRSFileService.read_and_validate_rf_rs_file(file_path)

        service = ClsRFandRSFileService(file_path)
        instrument = ClsInstrumentEnum.SST
        resolution = ClsResolutionEnum.Milliseconds_05
        file_name = os.path.basename(file_path)
        prefix_file = file_name[:2]
        sst_type=""
        if prefix_file == "rf":
            resolution = ClsResolutionEnum.Milliseconds_05
            sst_type="FAST"
        elif prefix_file == "rs":
            resolution = ClsResolutionEnum.Milliseconds_40
            sst_type="INTG"

        service.process_records(sst_type)

        file_timestamp = service.records[
            0].UTC_TIME.date()  # datetime.strptime(service.records[0].UTC_TIME, "%Y-%m-%d")

        # elif prefix == "bi":
        #    resolution = ClsResolutionEnum.Seconds_01

        controller = ClsPartitionMapController()
        mongo_collection = controller.get_target_collection(instrument, resolution, file_timestamp)

        service.insert_records_to_mongodb(file_timestamp, instrument, resolution, mongo_collection)

        ClsDataAvailabilityStatsService.recalculate_for_day(instrument, resolution, file_timestamp, mongo_collection)

        return len(service.records)
    def process_records(self, sst_type) -> None:
        records = ClsRFandRSFileRepository.read_records(self.file_path, self.dtype)
        df = pd.DataFrame(records)

        #for index, row in df.iterrows():
        #    print(row.get('TIME', '[Sem UTC_TIME]'))

        self.records = df.apply(
            lambda row: ClsSSTFileFormat.format_rs_rf_file_record(ClsRFandRSFileVO(self.file_path, row.to_dict()), sst_type), axis=1
        )

    def insert_records_to_mongodb(self, timestamp, instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, mongo_collection) -> str:
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT  # Tamanho do lote para inserções em massa
        #mongo_collection = ClsSettings.get_mongo_collection_name_by_file_type(self.file_path)

        for i in range(0, len(self.records), batch_size):
            batch = self.records[i:i + batch_size]
            res = ClsRFandRSFileRepository.insert_records(batch, self.file_path, mongo_collection)

            ClsLoggerService.write_processing_batch(self.file_path, batch_size, mongo_collection)
            if res.failed_count > 0:
                ClsLoggerService.write_failed_lines(self.file_path, res.failed_count)
            if res.duplicate_count > 0:
                ClsLoggerService.write_duplicate_lines(self.file_path, res.duplicate_count)

            ClsLoggerService.write_lines_inserted(self.file_path, res.inserted_count)
            CLSConsolePrint.debug(f"Lote de {len(batch)} registros inserido com sucesso.")

        return mongo_collection
