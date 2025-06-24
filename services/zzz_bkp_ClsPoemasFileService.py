import os
import numpy as np
from datetime import datetime

from config.ClsSettings import ClsSettings
from models.poemas.bkp.ClsBRTVO import ClsBrtVO
from models.poemas.bkp.ClsHWConfigVO import ClsHWConfigVO
from models.poemas.bkp.ClsHeaderVO import ClsHeaderVO
from models.poemas.bkp.zClsPoemasVO import ClsPoemasVO
from repositories.poemas.ClsPoemasFileRepository import ClsPoemasFileRepository
from services.ClsLoggerService import ClsLoggerService
from utils.ClsFormat import ClsFormat


class zzzzClsPoemasFileService:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.records = None

    @staticmethod
    def process_file(file_path) -> int:
        """service = ClsPoemasFileService(file_path)
        service.process_records()
        service.insert_records_to_mongodb()
        return len(service.records)"""

    def insert_records_to_mongodb(self) -> str:
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT  # Tamanho do lote para inserções em massa
        mongo_collection = ""#ClsSettings.get_mongo_collection_name_by_file_type(self.file_path)

        for i in range(0, len(self.records), batch_size):
            batch = self.records[i:i + batch_size]
            res = ClsPoemasFileRepository.insert_records(batch, self.file_path)

            ClsLoggerService.write_processing_batch(self.file_path, batch_size, mongo_collection)
            if res.failed_count > 0:
                ClsLoggerService.write_failed_lines(self.file_path, res.failed_count)
            if res.duplicate_count > 0:
                ClsLoggerService.write_duplicate_lines(self.file_path, res.duplicate_count)

            ClsLoggerService.write_lines_inserted(self.file_path, res.inserted_count)
            print(f"Lote de {len(batch)} registros inserido com sucesso.")

        return mongo_collection
    def process_records(self, flux=False, ms=False) :
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



    def convertzzzzzz_data_to_business_object(self, data: dict):
        # Time é necessário no construtor do VO para transformar o int numa hora, min, seg e ms
        data['TIME'] = data['header']['ID']
        poemas_obj = ClsPoemasVO(self.file_path, data)

        header = data['header']
        if 'Auxiliary_obs' in header:
            poemas_obj.header = ClsHeaderVO()
            poemas_obj.header.ID = header['ID']
            poemas_obj.header.date = header['Auxiliary_obs']['DATE']
            poemas_obj.header.tbmin = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['TBMIN'])
            poemas_obj.header.tbmax = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['TBMAX'])
            poemas_obj.header.nrec = header['Auxiliary_obs']['NREC']
            poemas_obj.header.nfreq = header['Auxiliary_obs']['NFREQ']
            if len(header['Auxiliary_obs']['FREQS']) > 0:
                poemas_obj.header.freq1 = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['FREQS'][0])
                poemas_obj.header.freq2 = ClsFormat.from_float_4_decimals(header['Auxiliary_obs']['FREQS'][1])

        poemas_obj.hwConfig = []
        for record in data['data']:
            time, ele, azi, tb = record
            hw_config_obj = ClsHWConfigVO()
            hw_config_obj.time = ClsFormat.from_float_none_decimals(time / 1000)
            hw_config_obj.hour = ClsFormat.from_int_to_hhmmssss(hw_config_obj.time)
            hw_config_obj.ele = ClsFormat.from_float_4_decimals(ele)
            hw_config_obj.azi = ClsFormat.from_float_4_decimals(azi)
            hw_config_obj.brts = []
            count_ms = 0
            for i in range(len(tb)):
                for j in range(0, len(tb[i]), 4):
                    brt_obj = ClsBrtVO()
                    brt_obj.hour = f"{hw_config_obj.hour}.{'{:04}'.format(count_ms)}"
                    brt_obj.tbl45 = ClsFormat.from_float_4_decimals(tb[i][j] * 1)
                    brt_obj.tbr45 = ClsFormat.from_float_4_decimals(tb[i][j + 1])
                    brt_obj.tbl90 = ClsFormat.from_float_4_decimals(tb[i][j + 2])
                    brt_obj.tbr90 = ClsFormat.from_float_4_decimals(tb[i][j + 3])

                    count_ms += 10
                    hw_config_obj.brts.append(brt_obj)
            poemas_obj.hwConfig.append(hw_config_obj)
        self.records = poemas_obj

    @staticmethod
    def _get_file_type(sufix: str) -> int:
        if sufix.upper() == 'BRT':
            return 1
        elif sufix.upper() == 'HKD':
            return 2
        else:
            return 0

    @staticmethod
    def _read_headers(f, ftype: int):
        hdr1 = np.fromfile(f, dtype=np.uint32, count=3)
        if ftype < 2:
            hdr2 = np.fromfile(f, dtype=np.float32, count=4)
        else:
            hdr2 = np.fromfile(f, dtype=np.float32, count=2)
        return hdr1, hdr2

    @staticmethod
    def _create_header(hdr1, hdr2, ftype: int, date: str) -> dict:
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

    @staticmethod
    def _read_data(f, hdr1, ftype: int, year: str, month: str, day: str, flag_ms: bool):
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
