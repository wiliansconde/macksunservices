import os
import subprocess
import numpy as np
import pandas as pd
from models.poemas.bkp.zClsPoemasVO import ClsPoemasVO
from models.sst.bi_file.ClsBIVO_1900_01_01_to_2002_09_15 import \
    ClsBIVO_1900_01_01_to_2002_09_15
from models.sst.bi_file.ClsBIVO_2002_09_16_to_2002_11_23 import \
    ClsBIVO_2002_09_16_to_2002_11_23
from models.sst.bi_file.ClsBIVO_2002_11_24_to_2002_12_13 import \
    ClsBIVO_2002_11_24_to_2002_12_13
from models.sst.bi_file.ClsBIVO_2002_12_14_to_2100_01_01 import \
    ClsBIVO_2002_12_14_to_2100_01_01


class ClsTrace:
    @staticmethod
    def np_to_list(np_array):
        if isinstance(np_array, np.ndarray):
            return np_array.tolist()
        return np_array

    @staticmethod
    def openFile(filePath):
        # Abrir o arquivo com o aplicativo padrão
        try:
            # Abrir o arquivo com o aplicativo padrão
            subprocess.Popen(["start", "", filePath], shell=True)
        except FileNotFoundError:
            print(f"O arquivo '{filePath}' não foi encontrado.")
        except PermissionError:
            print(f"Permissão negada ao tentar abrir o arquivo '{filePath}'.")
        except Exception as e:
            print(f"Ocorreu um erro ao tentar abrir o arquivo '{filePath}': {e}")

    @staticmethod
    def print_by_poemas_data_dictionary(trkStructData, openFileAfterCreated=False):
        logFile = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\log.txt'
        if os.path.exists(logFile):
            os.remove(logFile)
        with open(logFile, "w") as f:
            for key, value in trkStructData.items():
                f.write(str(key) + '\n')
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        f.write(str(sub_key) + ': ' + str(ClsTrace.np_to_list(sub_value)) + '\n')
                else:
                    f.write(str(ClsTrace.np_to_list(value)) + '\n\n')
        if openFileAfterCreated:
            ClsTrace.openFile(logFile)

    @staticmethod
    def print_by_poemas_business_object(poemas: ClsPoemasVO, openFileAfterCreated=False):
        filename = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\logObjs.txt'
        if os.path.exists(filename):
            os.remove(filename)

        with open(filename, 'w') as f:
            # Imprime os atributos do objeto Header
            f.write("Objeto: Header\n")
            for attr_name, attr_value in vars(poemas.header).items():
                f.write(f"  {attr_name}: {attr_value}\n")

            f.write("Objeto: HWConfig\n")
            for x, hw in enumerate(poemas.hwConfig):
                f.write(f"\nTime: {hw.time} \n")
                f.write(f"Hour: {hw.hour} \n")
                f.write(f"ele: {hw.ele} \n")
                f.write(f"azi: {hw.azi} \n")

                # Imprime os atributos de cada objeto BRT na lista brts
                for i, brt in enumerate(hw.brts):
                    f.write(f"{brt.hour} {brt.tbl45} {brt.tbr45} {brt.tbl90} {brt.tbr90}\n")
                    if (i % 100 == 0):
                        f.flush()

        if openFileAfterCreated:
            ClsTrace.openFile(filename)

    @staticmethod
    def write_to_txt_2002_12_14_to_2100_01_01(records: list, output_txt: str) -> None:
        with open(output_txt, 'w') as f:
            headers = [
                'TIME', 'UTC_TIME', 'UTC_TIME_YEAR', 'UTC_TIME_MONTH', 'UTC_TIME_DAY', 'UTC_TIME_HOUR',
                'UTC_TIME_MINUTE', 'UTC_TIME_SECOND', 'UTC_TIME_MILLISECOND',
                'AZIPOS', 'ELEPOS', 'AZIERR', 'ELEERR', 'ADC_1', 'ADC_2', 'ADC_3', 'ADC_4', 'ADC_5', 'ADC_6',
                'SIGMA_1', 'SIGMA_2', 'SIGMA_3', 'SIGMA_4', 'SIGMA_5', 'SIGMA_6', 'GPS_STATUS', 'ACQ_GAIN', 'TARGET',
                'OPMODE', 'OFF_1', 'OFF_2', 'OFF_3', 'OFF_4', 'OFF_5', 'OFF_6', 'HOT_TEMP', 'AMB_TEMP', 'OPT_TEMP',
                'IF_BOARD_TEMP', 'RADOME_TEMP', 'HUMIDITY', 'TEMPERATURE', 'OPAC_210', 'OPAC_405', 'ELEVATION',
                'PRESSURE', 'BURST', 'ERRORS'

            ]
            f.write(';'.join(headers) + '\n')

            for record in records:
                if isinstance(record, ClsBIVO_2002_12_14_to_2100_01_01):
                    values = [
                        str(record.TIME), str(record.UTC_TIME),
                        str(record.UTC_TIME_YEAR), str(record.UTC_TIME_MONTH), str(record.UTC_TIME_DAY),
                        str(record.UTC_TIME_HOUR),
                        str(record.UTC_TIME_MINUTE), str(record.UTC_TIME_SECOND), str(record.UTC_TIME_MILLISECOND),
                        str(record.AZIPOS), str(record.ELEPOS), str(record.AZIERR),
                        str(record.ELEERR), str(record.ADC_1), str(record.ADC_2), str(record.ADC_3), str(record.ADC_4),
                        str(record.ADC_5), str(record.ADC_6), str(record.SIGMA_1), str(record.SIGMA_2),
                        str(record.SIGMA_3), str(record.SIGMA_4), str(record.SIGMA_5), str(record.SIGMA_6),
                        str(record.GPS_STATUS), str(record.ACQ_GAIN), str(record.TARGET), str(record.OPMODE),
                        str(record.OFF_1), str(record.OFF_2), str(record.OFF_3), str(record.OFF_4), str(record.OFF_5),
                        str(record.OFF_6), str(record.HOT_TEMP), str(record.AMB_TEMP), str(record.OPT_TEMP),
                        str(record.IF_BOARD_TEMP), str(record.RADOME_TEMP), str(record.HUMIDITY),
                        str(record.TEMPERATURE),
                        str(record.OPAC_210), str(record.OPAC_405), str(record.ELEVATION), str(record.PRESSURE),
                        str(record.BURST), str(record.ERRORS)
                    ]
                    line = ';'.join(values)
                    f.write(line + '\n')
                else:
                    raise TypeError("Record is not an instance of clsBIVO_2002_12_14_to_2100_01_01")

    @staticmethod
    def write_to_txt_2002_11_24_to_2002_12_13(records: list, output_txt: str) -> None:
        with open(output_txt, 'w') as f:
            headers = [
                'TIME', 'UTC_TIME', 'UTC_TIME_YEAR', 'UTC_TIME_MONTH', 'UTC_TIME_DAY', 'UTC_TIME_HOUR',
                'UTC_TIME_MINUTE', 'UTC_TIME_SECOND', 'UTC_TIME_MILLISECOND',
                'AZIPOS', 'ELEPOS', 'AZIERR', 'ELEERR', 'ADC_1', 'ADC_2', 'ADC_3', 'ADC_4', 'ADC_5', 'ADC_6',
                'ADC_7', 'ADC_8', 'SIGMA_1', 'SIGMA_2', 'SIGMA_3', 'SIGMA_4', 'SIGMA_5', 'SIGMA_6', 'SIGMA_7',
                'SIGMA_8',
                'GPS_STATUS', 'DAQ_STATUS', 'ACQ_GAIN', 'TARGET', 'OPMODE', 'OFF_1', 'OFF_2', 'OFF_3', 'OFF_4', 'OFF_5',
                'OFF_6', 'HOT_TEMP', 'AMB_TEMP', 'OPT_TEMP', 'IF_BOARD_TEMP', 'RADOME_TEMP', 'HUMIDITY', 'TEMPERATURE',
                'OPAC_210', 'OPAC_405', 'ELEVATION', 'PRESSURE', 'BURST', 'ERRORS'
            ]
            f.write(';'.join(headers) + '\n')

            for record in records:
                if isinstance(record, ClsBIVO_2002_11_24_to_2002_12_13):
                    values = [
                        str(record.TIME), str(record.UTC_TIME),
                        str(record.UTC_TIME_YEAR), str(record.UTC_TIME_MONTH), str(record.UTC_TIME_DAY),
                        str(record.UTC_TIME_HOUR), str(record.UTC_TIME_MINUTE), str(record.UTC_TIME_SECOND),
                        str(record.UTC_TIME_MILLISECOND),
                        str(record.AZIPOS), str(record.ELEPOS), str(record.AZIERR),
                        str(record.ELEERR), str(record.ADC_1), str(record.ADC_2), str(record.ADC_3), str(record.ADC_4),
                        str(record.ADC_5), str(record.ADC_6), str(record.ADC_7), str(record.ADC_8), str(record.SIGMA_1),
                        str(record.SIGMA_2), str(record.SIGMA_3), str(record.SIGMA_4), str(record.SIGMA_5),
                        str(record.SIGMA_6), str(record.SIGMA_7), str(record.SIGMA_8), str(record.GPS_STATUS),
                        str(record.DAQ_STATUS), str(record.ACQ_GAIN), str(record.TARGET), str(record.OPMODE),
                        str(record.OFF_1), str(record.OFF_2), str(record.OFF_3), str(record.OFF_4), str(record.OFF_5),
                        str(record.OFF_6), str(record.HOT_TEMP), str(record.AMB_TEMP), str(record.OPT_TEMP),
                        str(record.IF_BOARD_TEMP), str(record.RADOME_TEMP), str(record.HUMIDITY),
                        str(record.TEMPERATURE),
                        str(record.OPAC_210), str(record.OPAC_405), str(record.ELEVATION), str(record.PRESSURE),
                        str(record.BURST), str(record.ERRORS)
                    ]
                    line = ';'.join(values)
                    f.write(line + '\n')
                else:
                    raise TypeError("Record is not an instance of clsBIVO_2002_11_24_to_2002_12_13")

    @staticmethod
    def write_to_txt_2002_09_16_to_2002_11_23(records: list, output_txt: str) -> None:
        with open(output_txt, 'w') as f:
            headers = [
                'TIME', 'UTC_TIME', 'UTC_TIME_YEAR', 'UTC_TIME_MONTH', 'UTC_TIME_DAY', 'UTC_TIME_HOUR',
                'UTC_TIME_MINUTE', 'UTC_TIME_SECOND', 'UTC_TIME_MILLISECOND',
                'AZIPOS', 'ELEPOS', 'AZIERR', 'ELEERR', 'ADC_1', 'ADC_2', 'ADC_3', 'ADC_4', 'ADC_5', 'ADC_6',
                'ADC_7', 'ADC_8', 'SIGMA_1', 'SIGMA_2', 'SIGMA_3', 'SIGMA_4', 'SIGMA_5', 'SIGMA_6', 'SIGMA_7',
                'SIGMA_8',
                'GPS_STATUS', 'DAQ_STATUS', 'ACQ_GAIN', 'TARGET', 'OPMODE', 'ATT_1', 'ATT_2', 'ATT_3', 'ATT_4', 'ATT_5',
                'ATT_6', 'OFF_1', 'OFF_2', 'OFF_3', 'OFF_4', 'OFF_5', 'OFF_6', 'MIX_VOLT_1', 'MIX_VOLT_2', 'MIX_VOLT_3',
                'MIX_VOLT_4', 'MIX_VOLT_5', 'MIX_VOLT_6', 'MIX_CURR_1', 'MIX_CURR_2', 'MIX_CURR_3', 'MIX_CURR_4',
                'MIX_CURR_5', 'MIX_CURR_6', 'HOT_TEMP', 'AMB_TEMP', 'OPT_TEMP', 'IF_BOARD_TEMP', 'RADOME_TEMP',
                'HUMIDITY', 'TEMPERATURE', 'OPAC_210', 'OPAC_405', 'ELEVATION', 'PRESSURE', 'BURST', 'ERRORS'
            ]
            f.write(';'.join(headers) + '\n')

            for record in records:
                if isinstance(record, ClsBIVO_2002_09_16_to_2002_11_23):
                    values = [
                        str(record.TIME), str(record.UTC_TIME),
                        str(record.UTC_TIME_YEAR), str(record.UTC_TIME_MONTH), str(record.UTC_TIME_DAY),
                        str(record.UTC_TIME_HOUR), str(record.UTC_TIME_MINUTE), str(record.UTC_TIME_SECOND),
                        str(record.UTC_TIME_MILLISECOND),
                        str(record.AZIPOS), str(record.ELEPOS), str(record.AZIERR),
                        str(record.ELEERR), str(record.ADC_1), str(record.ADC_2), str(record.ADC_3), str(record.ADC_4),
                        str(record.ADC_5), str(record.ADC_6), str(record.ADC_7), str(record.ADC_8), str(record.SIGMA_1),
                        str(record.SIGMA_2), str(record.SIGMA_3), str(record.SIGMA_4), str(record.SIGMA_5),
                        str(record.SIGMA_6), str(record.SIGMA_7), str(record.SIGMA_8), str(record.GPS_STATUS),
                        str(record.DAQ_STATUS), str(record.ACQ_GAIN), str(record.TARGET), str(record.OPMODE),
                        str(record.ATT_1), str(record.ATT_2), str(record.ATT_3), str(record.ATT_4), str(record.ATT_5),
                        str(record.ATT_6), str(record.OFF_1), str(record.OFF_2), str(record.OFF_3), str(record.OFF_4),
                        str(record.OFF_5), str(record.OFF_6), str(record.MIX_VOLT_1), str(record.MIX_VOLT_2),
                        str(record.MIX_VOLT_3), str(record.MIX_VOLT_4), str(record.MIX_VOLT_5), str(record.MIX_VOLT_6),
                        str(record.MIX_CURR_1), str(record.MIX_CURR_2), str(record.MIX_CURR_3), str(record.MIX_CURR_4),
                        str(record.MIX_CURR_5), str(record.MIX_CURR_6), str(record.HOT_TEMP), str(record.AMB_TEMP),
                        str(record.OPT_TEMP), str(record.IF_BOARD_TEMP), str(record.RADOME_TEMP), str(record.HUMIDITY),
                        str(record.TEMPERATURE), str(record.OPAC_210), str(record.OPAC_405), str(record.ELEVATION),
                        str(record.PRESSURE), str(record.BURST), str(record.ERRORS)
                    ]
                    line = ';'.join(values)
                    f.write(line + '\n')
                else:
                    raise TypeError("Record is not an instance of clsBIVO_2002_09_16_to_2002_11_23")

    @staticmethod
    def write_to_txt_1900_01_01_to_2002_09_15(records: list, output_txt: str) -> None:
        with open(output_txt, 'w') as f:
            headers = [
                'TIME', 'UTC_TIME', 'UTC_TIME_YEAR', 'UTC_TIME_MONTH', 'UTC_TIME_DAY', 'UTC_TIME_HOUR',
                'UTC_TIME_MINUTE', 'UTC_TIME_SECOND', 'UTC_TIME_MILLISECOND',
                'AZIPOS', 'ELEPOS', 'AZIERR', 'ELEERR', 'ADC_1', 'ADC_2', 'ADC_3', 'ADC_4', 'ADC_5', 'ADC_6',
                'ADC_7', 'ADC_8', 'SIGMA_1', 'SIGMA_2', 'SIGMA_3', 'SIGMA_4', 'SIGMA_5', 'SIGMA_6', 'SIGMA_7',
                'SIGMA_8',
                'GPS_STATUS', 'DAQ_STATUS', 'ACQ_GAIN', 'TARGET', 'OPMODE', 'ATT_1', 'ATT_2', 'ATT_3', 'ATT_4', 'ATT_5',
                'ATT_6', 'OFF_1', 'OFF_2', 'OFF_3', 'OFF_4', 'OFF_5', 'OFF_6', 'MIX_VOLT_1', 'MIX_VOLT_2', 'MIX_VOLT_3',
                'MIX_VOLT_4', 'MIX_VOLT_5', 'MIX_VOLT_6', 'MIX_CURR_1', 'MIX_CURR_2', 'MIX_CURR_3', 'MIX_CURR_4',
                'MIX_CURR_5', 'MIX_CURR_6', 'HOT_TEMP', 'AMB_TEMP', 'OPT_TEMP', 'IF_BOARD_TEMP', 'RADOME_TEMP',
                'HUMIDITY', 'WIND', 'OPAC_210', 'OPAC_405', 'ELEVATION', 'SEEING', 'BURST', 'ERRORS'
            ]
            f.write(';'.join(headers) + '\n')

            for record in records:
                if isinstance(record, ClsBIVO_1900_01_01_to_2002_09_15):
                    values = [
                        str(record.TIME), str(record.UTC_TIME),
                        str(record.UTC_TIME_YEAR), str(record.UTC_TIME_MONTH), str(record.UTC_TIME_DAY),
                        str(record.UTC_TIME_HOUR), str(record.UTC_TIME_MINUTE), str(record.UTC_TIME_SECOND),
                        str(record.UTC_TIME_MILLISECOND),
                        str(record.AZIPOS), str(record.ELEPOS), str(record.AZIERR),
                        str(record.ELEERR), str(record.ADC_1), str(record.ADC_2), str(record.ADC_3), str(record.ADC_4),
                        str(record.ADC_5), str(record.ADC_6), str(record.ADC_7), str(record.ADC_8), str(record.SIGMA_1),
                        str(record.SIGMA_2), str(record.SIGMA_3), str(record.SIGMA_4), str(record.SIGMA_5),
                        str(record.SIGMA_6), str(record.SIGMA_7), str(record.SIGMA_8), str(record.GPS_STATUS),
                        str(record.DAQ_STATUS), str(record.ACQ_GAIN), str(record.TARGET), str(record.OPMODE),
                        str(record.ATT_1), str(record.ATT_2), str(record.ATT_3), str(record.ATT_4), str(record.ATT_5),
                        str(record.ATT_6), str(record.OFF_1), str(record.OFF_2), str(record.OFF_3), str(record.OFF_4),
                        str(record.OFF_5), str(record.OFF_6), str(record.MIX_VOLT_1), str(record.MIX_VOLT_2),
                        str(record.MIX_VOLT_3), str(record.MIX_VOLT_4), str(record.MIX_VOLT_5), str(record.MIX_VOLT_6),
                        str(record.MIX_CURR_1), str(record.MIX_CURR_2), str(record.MIX_CURR_3), str(record.MIX_CURR_4),
                        str(record.MIX_CURR_5), str(record.MIX_CURR_6), str(record.HOT_TEMP), str(record.AMB_TEMP),
                        str(record.OPT_TEMP), str(record.IF_BOARD_TEMP), str(record.RADOME_TEMP), str(record.HUMIDITY),
                        str(record.WIND), str(record.OPAC_210), str(record.OPAC_405), str(record.ELEVATION),
                        str(record.SEEING), str(record.BURST), str(record.ERRORS)
                    ]
                    line = ';'.join(values)
                    f.write(line + '\n')
                else:
                    raise TypeError("Record is not an instance of clsBIVO_1900_01_01_to_2002_09_15")

    @staticmethod
    def write_to_file_rs_and_rf_file(records: pd.DataFrame, output_file: str) -> None:
        records.to_csv(output_file, sep=';', index=False)
        #print(output_file)
