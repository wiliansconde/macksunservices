import os
import numpy as np
from datetime import datetime, timedelta
import time as t

from pymongo.errors import PyMongoError

from config.ClsSettings import ClsSettings

from models.poemas.ClsPoemasVO import ClsPoemasVO
from repositories.poemas.ClsPoemasFileRepository import ClsPoemasFileRepository
from services.ClsLoggerService import ClsLoggerService
from utils.ClsConsolePrint import CLSConsolePrint
from utils.ClsFormat import ClsFormat


class ClsPoemasFileService:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.records = None

    def save_poemas_to_txt(self, poemas_list):
        output_file = r'C:\\Y\\WConde\\Estudo\\DoutoradoMack\\Disciplinas\\_PesquisaFinal\\Dados\\_FINAL\\POEMAS\\2011\\2011\\M01\\log.txt'

        with open(output_file, 'w') as f:
            # Cabeçalho
            header = ("ID", "DATE", "TIME", "NREC", "NFREQ", "FREQS", "FREQ1", "FREQ2", "TBMIN", "TBMAX",
                      "HOUR", "ELE", "AZI", "TBL45", "TBR45", "TBL90", "TBR90")
            f.write(
                "{:<10} {:<10} {:<10} {:<10} {:<10} {:<15} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n".format(
                    *header))

            # Linha de separação
            f.write("=" * 160 + "\n")

            # Conteúdo
            for poemas in poemas_list:
                f.write(
                    "{:<10} {:<10} {:<10} {:<10} {:<10} {:<15} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n".format(
                        str(poemas.ID),
                        str(poemas.DATE),
                        str(poemas.TIME),
                        str(poemas.NREC),
                        str(poemas.NFREQ),
                        str(poemas.FREQS),
                        str(poemas.FREQ1),
                        str(poemas.FREQ2),
                        str(poemas.TBMIN),
                        str(poemas.TBMAX),
                        str(poemas.HOUR),
                        str(poemas.ELE),
                        str(poemas.AZI),
                        str(poemas.TBL45),
                        str(poemas.TBR45),
                        str(poemas.TBL90),
                        str(poemas.TBR90)
                    ))
        # Abrir o arquivo após a gravação
        os.startfile(output_file)

    @staticmethod
    def process_file(file_path) -> int:
        service = ClsPoemasFileService(file_path)
        service.process_records()
        service.insert_records_to_mongodb()
        return len(service.records)

    def insert_records_to_mongodb(self) -> str:
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT  # Tamanho do lote para inserções em massa
        mongo_collection = ClsSettings.get_mongo_collection_name_by_file_type(self.file_path)
        #AQUI.... ...
        #AQUI....retornar o batch para Logica para criar arquivo
        for i in range(0, len(self.records), batch_size):
            batch = self.records[i:i + batch_size]
            res = ClsPoemasFileRepository.insert_records(batch, self.file_path)

            ClsLoggerService.write_processing_batch(self.file_path, batch_size, mongo_collection)
            if res.failed_count > 0:
                ClsLoggerService.write_failed_lines(self.file_path, res.failed_count)
            if res.duplicate_count > 0:
                ClsLoggerService.write_duplicate_lines(self.file_path, res.duplicate_count)

            ClsLoggerService.write_lines_inserted(self.file_path, res.inserted_count)
            CLSConsolePrint.debug(f"Lote de {len(batch)} registros inserido com sucesso.")
        return mongo_collection

    def process_records(self, flux=False, ms=False):
        file_name = os.path.basename(self.file_path)
        file_parts = file_name.split('.')
        sufix = file_parts[-1]

        ftype = self._get_file_type(sufix)
        flag_flux, flag_ms = flux, ms

        name_pieces = file_parts[0].split('_')
        year = '20' + name_pieces[1][:2]
        month = name_pieces[1][2:4]
        day = name_pieces[1][4:6]
        initial_time = name_pieces[2]

        with open(self.file_path, 'rb') as f:
            hdr1, hdr2 = self._read_headers(f, ftype)
            nr = hdr1[1]
            date = year + '-' + month + '-' + day

            header = self._create_header(hdr1, hdr2, ftype, date)
            data = self._read_data(f, hdr1, ftype, year, month, day, flag_ms)

        final_data = {'header': header, 'data': data}
        self.convert_data_to_business_object(final_data)

    def convert_data_to_business_object(self, data: dict):
        poemas_list = []
        general_counter = 0
        header = data['header']
        for record in data['data']:
            time, ele, azi, tb = record
            for i in range(len(tb)):
                for j in range(0, len(tb[i]), 4):
                    datetime_str = (f"{header['Auxiliary_obs']['DATE']} "
                                    f"{ClsFormat.from_int_to_hhmmssss(ClsFormat.from_float_none_decimals(time / 1000))}")

                    datetime_str_with_ms = f"{datetime_str}.{000:03d}"

                    utc_time = datetime.strptime(datetime_str_with_ms, "%Y-%m-%d %H:%M:%S.%f")
                    poemas_obj = None
                    poemas_obj = ClsPoemasVO(self.file_path, utc_time, data)

                    if 'Auxiliary_obs' in header:
                        poemas_obj.ID = header['ID']
                        poemas_obj.DATE = header['Auxiliary_obs']['DATE']
                        poemas_obj.TBMIN = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['TBMIN'])
                        poemas_obj.TBMAX = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['TBMAX'])
                        poemas_obj.NREC = header['Auxiliary_obs']['NREC']
                        poemas_obj.NFREQ = header['Auxiliary_obs']['NFREQ']
                        poemas_obj.FREQS = len(header['Auxiliary_obs']['FREQS'])
                        poemas_obj.PROC_SEQ = general_counter

                        if poemas_obj.FREQS > 0:
                            poemas_obj.FREQ1 = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['FREQS'][0])
                            poemas_obj.FREQ2 = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['FREQS'][1])

                    poemas_obj.TIME = ClsFormat.from_float_none_decimals(time / 1000)
                    poemas_obj.HOUR = ClsFormat.from_int_to_hhmmssss(poemas_obj.TIME)
                    poemas_obj.ELE = ClsFormat.from_float_4_decimals(ele)
                    poemas_obj.AZI = ClsFormat.from_float_4_decimals(azi)

                    #poemas_obj.HOUR = f"{poemas_obj.HOUR}.{'{:04}'.format(count_ms)}"
                    #poemas_obj.HOUR = f"{poemas_obj.HOUR}.{'{:04}'}"

                    poemas_obj.TBL45 = ClsFormat.from_float_4_decimals(tb[i][j] * 1)
                    poemas_obj.TBR45 = ClsFormat.from_float_4_decimals(tb[i][j + 1])
                    poemas_obj.TBL90 = ClsFormat.from_float_4_decimals(tb[i][j + 2])
                    poemas_obj.TBR90 = ClsFormat.from_float_4_decimals(tb[i][j + 3])

                    # Adiciona o objeto poemas_obj à lista poemas_list
                    poemas_list.append(poemas_obj)
                    general_counter += 1
                    # print(poemas_obj.PROC_SEQ)

        last_time = None  # Variável para armazenar o último (hora, minuto, segundo)
        count_ms = 0
        #AJUSTA O MILISSEGUNDO
        for poemas_obj in poemas_list:
            current_time = (poemas_obj.UTC_TIME_HOUR, poemas_obj.UTC_TIME_MINUTE, poemas_obj.UTC_TIME_SECOND)
            if current_time != last_time:
                count_ms = 0  # Reinicia milissegundos para 0
                last_time = current_time  # Atualiza o último tempo

            poemas_obj.UTC_TIME_MILLISECOND = count_ms
            updated_utc_time = poemas_obj.UTC_TIME + timedelta(milliseconds=count_ms)
            poemas_obj.UTC_TIME = updated_utc_time

            count_ms += 10

            if count_ms >= 1000:
                count_ms = 0  # Reinicia para 0 se ultrapassar 999

        self.records = poemas_list

    def _get_file_type(self, sufix: str) -> int:
        if sufix.upper() == 'BRT':
            return 1
        elif sufix.upper() == 'HKD':
            return 2
        else:
            return 0

    def _read_headers(self, f, ftype: int):
        hdr1 = np.fromfile(f, dtype=np.uint32, count=3)
        if ftype < 2:
            hdr2 = np.fromfile(f, dtype=np.float32, count=4)
        else:
            hdr2 = np.fromfile(f, dtype=np.float32, count=2)
        return hdr1, hdr2

    def _create_header(self, hdr1, hdr2, ftype: int, date: str) -> dict:
        if ftype < 2:
            header = {
                'Auxiliary_obs': {
                    'DATE': date,
                    'NREC': hdr1[1],
                    'NFREQ': hdr1[2],
                    'FREQS': hdr2[0:2],
                    'TBMIN': hdr2[2] if len(hdr2) > 2 else 0.0,
                    'TBMAX': hdr2[3] if len(hdr2) > 3 else 0.0
                },
                'ID': hdr1[0],
                'TIME': hdr1[1],
            }
        else:
            header = {
                'Auxiliary_hkd': {
                    'DATE': date,
                    'NREC': hdr1[1],
                    'NFREQ': hdr1[2],
                    'FREQS': hdr2[0:2]
                },
                'ID': hdr1[0],
                'TIME': hdr1[1],
            }
        return header

    def _read_data(self, f, hdr1, ftype: int, year: str, month: str, day: str, flag_ms: bool):
        nr = hdr1[1]
        if ftype == 0:
            nrep = 100
            dtype = np.dtype([('time', 'u4'), ('ele', 'f4'), ('azi', 'f4'), ('tb', 'f4', (4, nrep))])
        elif ftype == 1:
            dtype = np.dtype([('time', 'u4'), ('tb', 'f4', (4)), ('ele', 'f4'), ('azi', 'f4')])
        elif ftype == 2:
            dtype = np.dtype([('time', 'u4'), ('tenv', 'f4'), ('trec', 'f4', (2)), ('srec', 'f4', (2)),
                              ('press', 'f4'), ('rain', 'u4'), ('junk', 'f4')])
        else:
            raise ValueError("Unknown file type")

        data = np.fromfile(f, dtype=dtype)

        ndays = (datetime(int(year), int(month), int(day)) - datetime(2001, 1, 1)).days
        nsecs = ndays * 3600 * 24
        data['time'] -= nsecs

        if not flag_ms:
            data['time'] *= 1000

        if ftype < 2:
            data['azi'] = np.interp(data['time'], data['time'], data['azi'])
            data['ele'] = np.interp(data['time'], data['time'], data['ele'])

        return data

    @staticmethod
    def delete_records(file_path):
        ClsPoemasFileRepository.delete_records(file_path)
