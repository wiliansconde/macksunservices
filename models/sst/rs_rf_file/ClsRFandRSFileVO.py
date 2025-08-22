from typing import Union  # Tipagem para melhor controle de tipos de dados
import numpy as np  # Biblioteca NumPy para manipulação eficiente de arrays
from models.base_model.ClsBaseVO import ClsBaseVO


class ClsRFandRSFileVO(ClsBaseVO):
    def __init__(self, file_path: str, record: Union[np.void, dict]):
        super().__init__(file_path, record)

        self.ADCVAL_1 = record['ADCVAL_1']
        self.ADCVAL_2 = record['ADCVAL_2']
        self.ADCVAL_3 = record['ADCVAL_3']
        self.ADCVAL_4 = record['ADCVAL_4']
        self.ADCVAL_5 = record['ADCVAL_5']
        self.ADCVAL_6 = record['ADCVAL_6']
        self.POS_TIME = record['POS_TIME']
        self.AZIPOS = record['AZIPOS']
        self.ELEPOS = record['ELEPOS']
        self.PM_DAZ = record['PM_DAZ']
        self.PM_DEL = record['PM_DEL']
        self.AZIERR = record['AZIERR']
        self.ELEERR = record['ELEERR']
        self.X_OFF = record['X_OFF']
        self.Y_OFF = record['Y_OFF']
        self.OFF_1 = record['OFF_1']
        self.OFF_2 = record['OFF_2']
        self.OFF_3 = record['OFF_3']
        self.OFF_4 = record['OFF_4']
        self.OFF_5 = record['OFF_5']
        self.OFF_6 = record['OFF_6']
        self.TARGET = record['TARGET']
        self.OPMODE = record['OPMODE']
        self.GPS_STATUS = record['GPS_STATUS']
        self.RECNUM = record['RECNUM']
        self.TELESCOPE = 'SST'
        self.SSTType=''
