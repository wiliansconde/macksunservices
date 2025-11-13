import time
from datetime import datetime
from services.ClsBiFileService_1900_01_01_to_2002_09_15 import ClsBiFileService_1900_01_01_to_2002_09_15
from services.ClsBiFileService_2002_09_16_to_2002_11_23 import ClsBiFileService_2002_09_16_to_2002_11_23
from services.ClsBiFileService_2002_11_24_to_2002_12_13 import ClsBiFileService_2002_11_24_to_2002_12_13
from services.ClsBiFileService_2002_12_14_to_2100_01_01 import ClsBiFileService_2002_12_14_to_2100_01_01
from models.sst.utils.ClsSSTFileGetCommonProperties import ClsSSTFileGetCommonProperties
from services.ClsLoggerService import ClsLoggerService
# NOVO
from pathlib import Path
import os
import xml.etree.ElementTree as ET


class ClsSSTBIFileService:
    _XML_LOCAL_ROOT = Path(__file__).resolve().parents[1] / "config" / "sst_xml"
    XML_ROOT = Path(os.environ.get("SST_XML_ROOT", str(_XML_LOCAL_ROOT)))
    TIMESPAN_TABLE = "SSTDataFormatTimeSpanTable.xml"

    @staticmethod
    def process_file(input_file) -> int:
        bi_file_properties = ClsSSTFileGetCommonProperties(input_file)
        file_date = ClsSSTBIFileService.extract_bi_date_from_name(input_file)

        di, df = ClsSSTBIFileService._resolve_aux_timespan(file_date)
        cls_name = f"ClsBiFileService_{di.replace('-', '_')}_to_{df.replace('-', '_')}"
        mod_name = f"services.{cls_name}"

        import importlib
        mod = importlib.import_module(mod_name)
        svc_cls = getattr(mod, cls_name)
        bi_file_service = svc_cls(input_file)

        return ClsSSTBIFileService._process_and_insert(bi_file_service, input_file)

    @staticmethod
    def _process_and_insert(bi_file_service, input_file) -> int:
        try:
            bi_file_service.process_records()
            file_lines_qty = len(bi_file_service.records)
            bi_file_service.insert_records_to_mongodb(input_file)
            return file_lines_qty
        except Exception as e:
            print(f"Erro ao processar e inserir registros: {e}")

    # NOVO
    @classmethod
    def _resolve_aux_timespan(cls, file_date):
        table_path = cls.XML_ROOT / cls.TIMESPAN_TABLE
        if not table_path.exists():
            raise FileNotFoundError(f"Tabela XML não encontrada em {table_path}")

        # normaliza para date
        if isinstance(file_date, datetime):
            fdate = file_date.date()
        else:
            fdate = file_date  # já é date

        root = ET.parse(str(table_path)).getroot()

        for elem in root.findall(".//SSTDataFormatTimeSpanElement"):
            etype = (elem.findtext("SSTDataType") or "").strip().upper()
            if etype != "AUXILIARY":
                continue
            di = (elem.findtext("InitialDate") or "").strip()
            df = (elem.findtext("FinalDate") or "").strip()
            if not di or not df:
                continue

            d0 = datetime.fromisoformat(di).date()
            d1 = datetime.fromisoformat(df).date()

            if d0 <= fdate <= d1:
                return di, df

        raise RuntimeError(f"Nenhum intervalo auxiliar cobre a data {fdate.isoformat()}")

    @staticmethod
    def extract_bi_date_from_name(file_path: str):
        """
        Extrai a data base (YYYY-MM-DD) de arquivos BI no formato biYYYMMDD,
        onde YYY representa o ano desde 1900.

        Exemplo:
          bi1160101  → 2016-01-01
          bi2151231  → 2021-12-31
        """
        import os
        from datetime import datetime

        name = os.path.basename(file_path)
        digits = "".join(ch for ch in name if ch.isdigit())

        if len(digits) != 7:
            raise ValueError(f"Formato inesperado para arquivo BI: {name} (dígitos={digits})")

        y = 1900 + int(digits[0:3])  # 116 → 2016
        m = int(digits[3:5])
        d = int(digits[5:7])

        return datetime(y, m, d).date()
