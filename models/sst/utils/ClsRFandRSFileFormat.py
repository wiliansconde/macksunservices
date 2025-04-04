from models.sst.rs_rf_file.ClsRFandRSFileVO import ClsRFandRSFileVO


class ClsRFandRSFileFormat:
    @staticmethod
    def format_record(record: ClsRFandRSFileVO) -> ClsRFandRSFileVO:
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
