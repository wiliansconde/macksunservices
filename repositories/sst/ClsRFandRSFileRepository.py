import os
import numpy as np

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsRFandRSFileRepository:
    @staticmethod
    def get_dtype(prefix: str) -> np.dtype:
        if prefix in ["rf", "rs"]:
            return np.dtype([
                ('TIME', '<i4'),
                ('ADCVAL_1', '<u2'),
                ('ADCVAL_2', '<u2'),
                ('ADCVAL_3', '<u2'),
                ('ADCVAL_4', '<u2'),
                ('ADCVAL_5', '<u2'),
                ('ADCVAL_6', '<u2'),
                ('POS_TIME', '<i4'),
                ('AZIPOS', '<i4'),
                ('ELEPOS', '<i4'),
                ('PM_DAZ', '<i2'),
                ('PM_DEL', '<i2'),
                ('AZIERR', '<i4'),
                ('ELEERR', '<i4'),
                ('X_OFF', '<i2'),
                ('Y_OFF', '<i2'),
                ('OFF_1', '<i2'),
                ('OFF_2', '<i2'),
                ('OFF_3', '<i2'),
                ('OFF_4', '<i2'),
                ('OFF_5', '<i2'),
                ('OFF_6', '<i2'),
                ('TARGET', 'i1'),
                ('OPMODE', 'i1'),
                ('GPS_STATUS', '<i2'),
                ('RECNUM', '<i4')
            ], align=False)


        else:
            raise ValueError("Prefixo desconhecido")

    def zz_old_get_dtype(prefix: str) -> np.dtype:
        if prefix in ["rf", "rs"]:
            return np.dtype([
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
                ('RECNUM', np.int32)
            ])
        else:
            raise ValueError("Prefixo desconhecido")

    @staticmethod
    def calculate_record_size(prefix: str) -> int:
        if prefix in ["rf", "rs"]:
            return 64
        else:
            raise ValueError("Prefixo desconhecido")

    @staticmethod
    def read_records(file_path: str, dtype: np.dtype) -> np.ndarray:
        record_size = ClsRFandRSFileRepository.calculate_record_size(os.path.basename(file_path)[:2])
        file_size = os.path.getsize(file_path)
        num_records = file_size // record_size
        return np.fromfile(file_path, dtype=dtype, count=num_records)

    @staticmethod
    def insert_records(records, file_path,mongo_collection):
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT
        #mongo_collection = ClsSettings.get_mongo_collection_name_by_file_type(file_path)


        res = None
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            res = ClsMongoHelper.insert_vos_to_mongodb(batch, mongo_collection, file_path)
        return res
