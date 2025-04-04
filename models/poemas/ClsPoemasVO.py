from models.base_model.ClsBaseVO import ClsBaseVO


class ClsPoemasVO(ClsBaseVO):
    def __init__(self, file_path: str, utc_time, record):
        super().__init__(file_path, record)
        super().init_by_utc_time(file_path, utc_time)
        self.ID = record.get('ID')
        self.DATE = record.get('DATE')
        self.TIME = record.get('TIME')
        self.UTC_TIME = utc_time
        self.NREC = record.get('NREC')
        self.NFREQ = record.get('NFREQ')
        self.FREQS = record.get('FREQS')
        self.FREQ1 = record.get('FREQ1')
        self.FREQ2 = record.get('FREQ2')
        self.TBMIN = record.get('TBMIN')
        self.TBMAX = record.get('TBMAX')
        self.HOUR = record.get('HOUR')
        self.ELE = record.get('ELE')
        self.AZI = record.get('AZI')
        self.TBL45 = record.get('TBL45')
        self.TBR45 = record.get('TBR45')
        self.TBL90 = record.get('TBL90')
        self.TBR90 = record.get('TBR90')
        self.OBJID=0
        self.PROC_SEQ=0
        self.UTC_TIME_MILLISECOND=0
        self.TELESCOPE='POEMAS'
