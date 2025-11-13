from typing import Union  # Tipagem para melhor controle de tipos de dados
import numpy as np  # Biblioteca NumPy para manipulação eficiente de arrays
from models.base_model.ClsBaseVO import ClsBaseVO


class ClsRFandRSFileVO(ClsBaseVO):
    def __init__(self, file_path: str, record: Union[np.void, dict]):
        super().__init__(file_path, record)
        self.ADCVAL_1 = record['adcval_1']
        self.ADCVAL_2 = record['adcval_2']
        self.ADCVAL_3 = record['adcval_3']
        self.ADCVAL_4 = record['adcval_4']
        self.ADCVAL_5 = record['adcval_5']
        self.ADCVAL_6 = record['adcval_6']
        self.POS_TIME = record['pos_time']
        self.AZIPOS = record['azipos']
        self.ELEPOS = record['elepos']
        self.PM_DAZ = record['pm_daz']
        self.PM_DEL = record['pm_del']
        self.AZIERR = record['azierr']
        self.ELEERR = record['eleerr']
        self.X_OFF = record['x_off']
        self.Y_OFF = record['y_off']
        self.OFF_1 = record['off_1']
        self.OFF_2 = record['off_2']
        self.OFF_3 = record['off_3']
        self.OFF_4 = record['off_4']
        self.OFF_5 = record['off_5']
        self.OFF_6 = record['off_6']
        self.TARGET = record['target']
        self.OPMODE = record['opmode']
        self.GPS_STATUS = record['gps_status']
        self.RECNUM = record['recnum']
        self.UTC_TIME = record['UTC_TIME']
        self.TELESCOPE = 'SST'
        self.SSTType=''
