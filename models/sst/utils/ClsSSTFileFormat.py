import datetime
from models.sst.rs_rf_file.ClsRFandRSFileVO import ClsRFandRSFileVO


class ClsSSTFileFormat:
    @staticmethod
    def format_record_2002_12_14_to_2100_01_01(record) -> list:
        if record[1] > 0:  # AZIPOS
            record[1] = round(record[1], 2)
        if record[2] > 0:  # ELEPOS
            record[2] = round(record[2], 2)
        if record[3] > 0:  # AZIERR
            record[3] = round(record[3], 2)
        if record[4] > 0:  # ELEERR
            record[4] = round(record[4], 2)
        if record[5] > 0:  # ADC_1
            record[5] = round(record[5], 2)
        if record[6] > 0:  # ADC_2
            record[6] = round(record[6], 2)
        if record[7] > 0:  # ADC_3
            record[7] = round(record[7], 2)
        if record[8] > 0:  # ADC_4
            record[8] = round(record[8], 2)
        if record[9] > 0:  # ADC_5
            record[9] = round(record[9], 2)
        if record[10] > 0:  # ADC_6
            record[10] = round(record[10], 2)
        if record[11] > 0:  # SIGMA_1
            record[11] = round(record[11], 2)
        if record[12] > 0:  # SIGMA_2
            record[12] = round(record[12], 2)
        if record[13] > 0:  # SIGMA_3
            record[13] = round(record[13], 2)
        if record[14] > 0:  # SIGMA_4
            record[14] = round(record[14], 2)
        if record[15] > 0:  # SIGMA_5
            record[15] = round(record[15], 2)
        if record[16] > 0:  # SIGMA_6
            record[16] = round(record[16], 2)
        if record[17] > 0:  # GPS_STATUS
            record[17] = round(record[17], 2)
        if record[18] > 0:  # ACQ_GAIN
            record[18] = round(record[18], 2)
        if record[19] > 0:  # TARGET
            record[19] = round(record[19], 2)
        if record[20] > 0:  # OPMODE
            record[20] = round(record[20], 2)
        if record[21] > 0:  # OFF_1
            record[21] = round(record[21], 2)
        if record[22] > 0:  # OFF_2
            record[22] = round(record[22], 2)
        if record[23] > 0:  # OFF_3
            record[23] = round(record[23], 2)
        if record[24] > 0:  # OFF_4
            record[24] = round(record[24], 2)
        if record[25] > 0:  # OFF_5
            record[25] = round(record[25], 2)
        if record[26] > 0:  # OFF_6
            record[26] = round(record[26], 2)
        if record[27] > 0:  # HOT_TEMP
            record[27] = round(record[27] / 1, 2)
        if record[28] > 0:  # AMB_TEMP
            record[28] = round(record[28] / 1, 2)
        if record[29] > 0:  # OPT_TEMP
            record[29] = round(record[29] / 1, 2)
        if record[30] > 0:  # IF_BOARD_TEMP
            record[30] = round(record[30] / 1, 2)
        if record[31] > 0:  # RADOME_TEMP
            record[31] = round(record[31] / 1, 2)
        if record[32] > 0:  # HUMIDITY
            record[32] = round(record[32], 2)
        if record[33] > 0:  # TEMPERATURE
            record[33] = round(record[33], 2)
        if record[34] > 0:  # OPAC_210
            record[34] = round(record[34], 2)
        if record[35] > 0:  # OPAC_405
            record[35] = round(record[35], 2)
        if record[36] > 0:  # ELEVATION
            record[36] = round(record[36], 2)
        if record[37] > 0:  # PRESSURE
            record[37] = round(record[37], 2)
        if record[38] > 0:  # BURST
            record[38] = round(record[38], 2)
        if record[39] > 0:  # ERRORS
            record[39] = round(record[39], 2)

        return record

    @staticmethod
    def format_record_2002_11_24_to_2002_12_13(record) -> list:
        if record[1] > 0:  # AZIPOS
            record[1] = round(record[1], 2)
        if record[2] > 0:  # ELEPOS
            record[2] = round(record[2], 2)
        if record[3] > 0:  # AZIERR
            record[3] = round(record[3], 2)
        if record[4] > 0:  # ELEERR
            record[4] = round(record[4], 2)

        if record[5] > 0:  # ADC_1
            record[5] = int(record[5])
        if record[6] > 0:  # ADC_2
            record[6] = int(record[6])
        if record[7] > 0:  # ADC_3
            record[7] = int(record[7])
        if record[8] > 0:  # ADC_4
            record[8] = int(record[8])
        if record[9] > 0:  # ADC_5
            record[9] = int(record[9])
        if record[10] > 0:  # ADC_6
            record[10] = int(record[10])

        if record[11] > 0:  # SIGMA_1
            record[11] = round(record[11], 2)
        if record[12] > 0:  # SIGMA_2
            record[12] = round(record[12], 2)
        if record[13] > 0:  # SIGMA_3
            record[13] = round(record[13], 2)
        if record[14] > 0:  # SIGMA_4
            record[14] = round(record[14], 2)
        if record[15] > 0:  # SIGMA_5
            record[15] = round(record[15], 2)
        if record[16] > 0:  # SIGMA_6
            record[16] = round(record[16], 2)

        if record[17] > 0:  # GPS_STATUS
            record[17] = int(record[17])
        if record[18] > 0:  # DAQ_STATUS
            record[18] = int(record[18])
        if record[19] > 0:  # ACQ_GAIN
            record[19] = int(record[19])
        if record[20] > 0:  # TARGET
            record[20] = int(record[20])
        if record[21] > 0:  # OPMODE
            record[21] = int(record[21])

        if record[22] > 0:  # OFF_1
            record[22] = int(record[22])
        if record[23] > 0:  # OFF_2
            record[23] = int(record[23])
        if record[24] > 0:  # OFF_3
            record[24] = int(record[24])
        if record[25] > 0:  # OFF_4
            record[25] = int(record[25])
        if record[26] > 0:  # OFF_5
            record[26] = int(record[26])
        if record[27] > 0:  # OFF_6
            record[27] = int(record[27])

        if record[28] > 0:  # HOT_TEMP
            record[28] = round(record[28], 2)
        if record[29] > 0:  # AMB_TEMP
            record[29] = round(record[29], 2)
        if record[30] > 0:  # OPT_TEMP
            record[30] = round(record[30], 2)
        if record[31] > 0:  # IF_BOARD_TEMP
            record[31] = round(record[31], 2)
        if record[32] > 0:  # RADOME_TEMP
            record[32] = round(record[32], 2)

        if record[33] > 0:  # HUMIDITY
            record[33] = round(record[33], 2)
        if record[34] > 0:  # TEMPERATURE
            record[34] = round(record[34], 2)

        if record[35] > 0:  # OPAC_210
            record[35] = round(record[35], 2)
        if record[36] > 0:  # OPAC_405
            record[36] = round(record[36], 2)

        if record[37] > 0:  # ELEVATION
            record[37] = round(record[37], 2)
        if record[38] > 0:  # PRESSURE
            record[38] = round(record[38], 2)
        if record[39] > 0:  # BURST
            record[39] = int(record[39])
        if record[40] > 0:  # ERRORS
            record[40] = int(record[40])

        return record

    @staticmethod
    def format_record_2002_09_16_to_2002_11_23(record) -> list:
        if record[1] > 0:  # AZIPOS
            record[1] = round(record[1], 2)
        if record[2] > 0:  # ELEPOS
            record[2] = round(record[2], 2)
        if record[3] > 0:  # AZIERR
            record[3] = round(record[3], 2)
        if record[4] > 0:  # ELEERR
            record[4] = round(record[4], 2)
        for i in range(5, 13):  # ADC_1 to ADC_8
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(13, 21):  # SIGMA_1 to SIGMA_8
            if record[i] > 0:
                record[i] = round(record[i], 2)
        if record[21] > 0:  # GPS_STATUS
            record[21] = round(record[21], 2)
        if record[22] > 0:  # DAQ_STATUS
            record[22] = round(record[22], 2)
        if record[23] > 0:  # ACQ_GAIN
            record[23] = round(record[23], 2)
        if record[24] > 0:  # TARGET
            record[24] = round(record[24], 2)
        if record[25] > 0:  # OPMODE
            record[25] = round(record[25], 2)
        for i in range(26, 32):  # ATT_1 to ATT_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(32, 38):  # OFF_1 to OFF_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(38, 44):  # MIX_VOLT_1 to MIX_VOLT_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(44, 50):  # MIX_CURR_1 to MIX_CURR_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(50, 55):  # HOT_TEMP to RADOME_TEMP
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(55, 57):  # HUMIDITY, TEMPERATURE
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(57, 59):  # OPAC_210, OPAC_405
            if record[i] > 0:
                record[i] = round(record[i], 2)
        if record[59] > 0:  # ELEVATION
            record[59] = round(record[59], 2)
        if record[60] > 0:  # PRESSURE
            record[60] = round(record[60], 2)
        if record[61] > 0:  # BURST
            record[61] = round(record[61], 2)
        if record[62] > 0:  # ERRORS
            record[62] = round(record[62], 2)
        return record

    @staticmethod
    def format_record_1900_01_01_to_2002_09_15(record) -> list:
        if record[1] > 0:  # AZIPOS
            record[1] = round(record[1], 2)
        if record[2] > 0:  # ELEPOS
            record[2] = round(record[2], 2)
        if record[3] > 0:  # AZIERR
            record[3] = round(record[3], 2)
        if record[4] > 0:  # ELEERR
            record[4] = round(record[4], 2)
        for i in range(5, 13):  # ADC_1 to ADC_8
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(13, 21):  # SIGMA_1 to SIGMA_8
            if record[i] > 0:
                record[i] = round(record[i], 2)
        if record[21] > 0:  # GPS_STATUS
            record[21] = round(record[21], 2)
        if record[22] > 0:  # DAQ_STATUS
            record[22] = round(record[22], 2)
        if record[23] > 0:  # ACQ_GAIN
            record[23] = round(record[23], 2)
        if record[24] > 0:  # TARGET
            record[24] = round(record[24], 2)
        if record[25] > 0:  # OPMODE
            record[25] = round(record[25], 2)
        for i in range(26, 32):  # ATT_1 to ATT_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(32, 38):  # OFF_1 to OFF_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(38, 44):  # MIX_VOLT_1 to MIX_VOLT_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(44, 50):  # MIX_CURR_1 to MIX_CURR_6
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(50, 56):  # HOT_TEMP to WIND
            if record[i] > 0:
                record[i] = round(record[i], 2)
        for i in range(56, 58):  # OPAC_210, OPAC_405
            if record[i] > 0:
                record[i] = round(record[i], 2)
        if record[58] > 0:  # ELEVATION
            record[58] = round(record[58], 2)
        if record[59] > 0:  # SEEING
            record[59] = round(record[59], 2)
        if record[60] > 0:  # PRESSURE
            record[60] = round(record[60], 2)
        if record[61] > 0:  # BURST
            record[61] = round(record[61], 2)
        if record[62] > 0:  # ERRORS
            record[62] = round(record[62], 2)
        return record

    @staticmethod
    def convert_to_utc(time_value: int) -> datetime:
        time_value = int(time_value)
        utc_datetime = datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=time_value)
        return utc_datetime

    def format_rs_rf_file_record(record: ClsRFandRSFileVO, sst_type:str) -> ClsRFandRSFileVO:
        record.SSTType=sst_type
        # Formata os valores dos atributos arredondando-os
        if record.ADCVAL_1 > 0:
            record.ADCVAL_1 = round(record.ADCVAL_1, 2)
        if record.ADCVAL_2 > 0:
            record.ADCVAL_2 = round(record.ADCVAL_2, 2)
        if record.ADCVAL_3 > 0:
            record.ADCVAL_3 = round(record.ADCVAL_3, 2)
        if record.ADCVAL_4 > 0:
            record.ADCVAL_4 = round(record.ADCVAL_4, 2)
        if record.ADCVAL_5 > 0:
            record.ADCVAL_5 = round(record.ADCVAL_5, 2)
        if record.ADCVAL_6 > 0:
            record.ADCVAL_6 = round(record.ADCVAL_6, 2)
        if record.AZIPOS > 0:
            record.AZIPOS = round(record.AZIPOS, 2)
        if record.ELEPOS > 0:
            record.ELEPOS = round(record.ELEPOS, 2)
        if record.AZIERR > 0:
            record.AZIERR = round(record.AZIERR, 2)
        if record.ELEERR > 0:
            record.ELEERR = round(record.ELEERR, 2)
        return record
