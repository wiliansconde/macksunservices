from models.base_model.ClsBaseVO import ClsBaseVO


class ClsBIVO_2002_11_24_to_2002_12_13(ClsBaseVO):
    def __init__(self, file_path: str, record):
        try:
            super().__init__(file_path, record)
            self.AZIPOS: float = round(record[1], 2)
            self.ELEPOS: float = round(record[2], 2)
            self.AZIERR: float = round(record[3], 2)
            self.ELEERR: float = round(record[4], 2)

            self.ADC_1: int = int(record[5])
            self.ADC_2: int = int(record[6])
            self.ADC_3: int = int(record[7])
            self.ADC_4: int = int(record[8])
            self.ADC_5: int = int(record[9])
            self.ADC_6: int = int(record[10])

            self.SIGMA_1: float = round(record[11], 2)
            self.SIGMA_2: float = round(record[12], 2)
            self.SIGMA_3: float = round(record[13], 2)
            self.SIGMA_4: float = round(record[14], 2)
            self.SIGMA_5: float = round(record[15], 2)
            self.SIGMA_6: float = round(record[16], 2)

            self.GPS_STATUS: int = int(record[17])
            self.DAQ_STATUS: int = int(record[18])
            self.ACQ_GAIN: int = int(record[19])
            self.TARGET: int = int(record[20])
            self.OPMODE: int = int(record[21])

            self.OFF_1: int = int(record[22])
            self.OFF_2: int = int(record[23])
            self.OFF_3: int = int(record[24])
            self.OFF_4: int = int(record[25])
            self.OFF_5: int = int(record[26])
            self.OFF_6: int = int(record[27])

            self.HOT_TEMP: float = round(record[28], 2)
            self.AMB_TEMP: float = round(record[29], 2)
            self.OPT_TEMP: float = round(record[30], 2)
            self.IF_BOARD_TEMP: float = round(record[31], 2)
            self.RADOME_TEMP: float = round(record[32], 2)

            self.HUMIDITY: float = round(record[33], 2)
            self.TEMPERATURE: float = round(record[34], 2)

            self.OPAC_210: float = round(record[35], 2)
            self.OPAC_405: float = round(record[36], 2)

            self.ELEVATION: float = round(record[37], 2)
            self.PRESSURE: float = round(record[38], 2)
            self.BURST: int = int(record[39])
            self.ERRORS: int = int(record[40])
        except Exception as e:
            print(f"Error processing record: {e}")
            print(f"Record: {record}")
