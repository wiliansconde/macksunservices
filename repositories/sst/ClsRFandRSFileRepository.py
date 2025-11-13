
import numpy as np
import json
from collections import defaultdict
from datetime import datetime
from astropy.io import fits
import csv
import os
from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
# NOVO
import struct
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import json
# converte o campo time (Hus = 100 µs) para datetime UTC
from datetime import datetime, timedelta, timezone

class ClsRFandRSFileRepository:
    # NOVO
    # Caminho base dos XML (relativo ao projeto)
    _XML_LOCAL_ROOT = Path(__file__).resolve().parents[2] / "config" / "sst_xml"

    # Permite sobrescrever via variável de ambiente, mas padrão é config/sst_xml
    XML_ROOT = Path(os.environ.get("SST_XML_ROOT", str(_XML_LOCAL_ROOT)))

    # Nome do arquivo principal de mapeamento temporal
    TIMESPAN_TABLE = "SSTDataFormatTimeSpanTable.xml"

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
    def read_records_old(file_path: str, dtype: np.dtype) -> np.ndarray:
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

    def get_records_by_time_range(date_to_generate_file, mongo_collection_name, limit=1000):
        """
        Obtém os registros com base no intervalo de tempo fornecido e aplica um limite opcional.
        """
        #mongo_collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_DATA_POEMAS_FILE_10ms)
        records = ClsMongoHelper.find_records_by_time_range(mongo_collection_name, date_to_generate_file, limit)

        if records:
            print(f"{len(records)} documentos encontrados na coleção {mongo_collection_name}")
        else:
            print(f"Nenhum documento encontrado na coleção {mongo_collection_name} para o intervalo de tempo especificado.")

        return records

    @staticmethod
    def read_records(file_path: str, dtype: np.dtype) -> np.ndarray:
        # resolve layout via XML
        header_xml = ClsRFandRSFileRepository._resolve_header_xml(file_path)

        if header_xml:
            names, fmt = ClsRFandRSFileRepository._build_layout_from_xml(header_xml)
            rec_size = struct.calcsize(fmt)
            file_size = os.path.getsize(file_path)
            nrec = file_size // rec_size
            unpacker = struct.Struct(fmt)
            rows = []

            from datetime import datetime, timedelta, timezone
            base_date = ClsRFandRSFileRepository._extract_iso_date_from_name(file_path)
            base_dt = datetime(base_date.year, base_date.month, base_date.day, tzinfo=timezone.utc)

            debug_path = f"{file_path}.debug.txt"
            print('debug_path: ' + debug_path)
            with open(debug_path, "w", encoding="utf8") as dbg, open(file_path, "rb") as f:
                dbg.write(f"[DEBUG STRUCT INFO]\n")
                dbg.write(f"header_xml: {header_xml}\n")
                dbg.write(f"fmt: {fmt}\n")
                dbg.write(f"record_size: {rec_size}\n")
                dbg.write(f"num_records: {nrec}\n\n")
                dbg.write("=== PRIMEIROS REGISTROS ===\n")

                for rec_index in range(nrec):
                    data = f.read(rec_size)
                    if len(data) < rec_size:
                        break

                    values = unpacker.unpack(data)
                    row = {name: values[i] for i, name in enumerate(names)}

                    # UTC_TIME a partir de time em unidades de 100 microssegundos
                    tval = row.get("time", row.get("TIME"))
                    if isinstance(tval, (int, float)):
                        row["UTC_TIME"] = base_dt + timedelta(microseconds=int(tval) * 100)
                    else:
                        row["UTC_TIME"] = None

                    rows.append(row)

                    if rec_index < 10:
                        dbg.write(f"--- Registro {rec_index + 1} ---\n")
                        for name, value in row.items():
                            dbg.write(f"{name}: {value}\n")
                        dbg.write("\n")

            return rows

        # fallback legado sem XML
        record_size = ClsRFandRSFileRepository.calculate_record_size(os.path.basename(file_path)[:2])
        file_size = os.path.getsize(file_path)
        num_records = file_size // record_size
        return np.fromfile(file_path, dtype=dtype, count=num_records)


    @staticmethod
    def get_records_by_time_range_sst_type(date_to_generate_file, mongo_collection_name, sst_type):
        limit = 1000
        """
        Obtém os registros com base no intervalo de tempo fornecido e aplica um limite opcional.
        """
        # mongo_collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_DATA_POEMAS_FILE_10ms)
        records = ClsMongoHelper.find_records_by_time_range_sst_type(mongo_collection_name, date_to_generate_file,
                                                                     sst_type)

        if records:
            print(f"{len(records)} documentos encontrados na coleção {mongo_collection_name}")
        else:
            print(
                f"Nenhum documento encontrado na coleção {mongo_collection_name} para o intervalo de tempo especificado.")

        return records

    # NOVOS MÉTODOS
    @staticmethod
    def _extract_iso_date_from_name(file_path: str):
        """
        Extrai a data ISO (YYYY-MM-DD) a partir do nome do arquivo SST.

        Aceita diferentes convenções históricas do SST:
        - rf1160101.1150  → 2016-01-01   (ano desde 1900)
        - rs2151231.2230  → 2021-12-31   (ano desde 1900)
        - rf20030715T102030.rbd → 2003-07-15 (ano completo)
        - rf030715102030.rbd   → 2003-07-15  (ano abreviado)
        """
        import os
        from datetime import datetime

        name = os.path.basename(file_path)
        digits = "".join(ch for ch in name if ch.isdigit())

        # 1) formato com ano completo: ex. 20030715...
        if len(digits) >= 14:
            y, m, d = int(digits[0:4]), int(digits[4:6]), int(digits[6:8])

        # 2) formato SST antigo (ano desde 1900): ex. rf1160101.1150 -> 2016-01-01
        elif len(digits) in (10, 11):
            y = 1900 + int(digits[0:3])  # 116 → 2016
            m = int(digits[3:5])
            d = int(digits[5:7])

        # 3) formato abreviado tipo rf030715... -> 2003-07-15
        elif len(digits) >= 12:
            y = 2000 + int(digits[0:2])
            m = int(digits[2:4])
            d = int(digits[4:6])

        else:
            raise ValueError(f"Não foi possível extrair data de {name} (dígitos={digits})")

        return datetime(y, m, d).date()

    @classmethod
    def _resolve_header_xml(cls, file_path: str):
        """
        Seleciona o XML de layout correto conforme o tipo (RF/RS = Data, BI = Auxiliary)
        e a data extraída do nome do arquivo.
        """
        file_type = os.path.basename(file_path)[:2].upper()
        file_date = cls._extract_iso_date_from_name(file_path)

        # A tabela principal é sempre a mesma
        table_path = cls.XML_ROOT / "SSTDataFormatTimeSpanTable.xml"
        if not table_path.exists():
            return None

        root = ET.parse(str(table_path)).getroot()

        # Define o tipo esperado dentro da tabela
        expected_type = "DATA" if file_type in ("RF", "RS") else "AUXILIARY"

        # Percorre todos os elementos
        for elem in root.findall(".//SSTDataFormatTimeSpanElement"):
            etype = (elem.findtext("SSTDataType") or "").strip().upper()
            di = (elem.findtext("InitialDate") or "").strip()
            df = (elem.findtext("FinalDate") or "").strip()
            hdr = (elem.findtext("DataFormatDecriptionFile") or "").strip()  # cuidado: “Decription” sem o s

            if not etype or not di or not df or not hdr:
                continue

            if etype != expected_type:
                continue

            d0 = datetime.fromisoformat(di).date()
            d1 = datetime.fromisoformat(df).date()

            # se a data do arquivo estiver dentro do intervalo, retorna o XML correspondente
            if d0 <= file_date <= d1:
                header_xml = cls.XML_ROOT / hdr
                if header_xml.exists():
                    return str(header_xml)

        return None

    @staticmethod
    def _build_layout_from_xml(header_xml_path: str):
        """
        Lê o XML e gera (names, fmt) para leitura binária via struct.
        Compatível com os arquivos SSTDataVariable (Data e Auxiliary).
        """
        xr = ET.parse(header_xml_path).getroot()
        names = []
        fmt_parts = []

        def map_type(vtype: str) -> str:
            t = vtype.strip().lower()
            if t in ("xs:int", "int", "xs:integer"):
                return "i"
            if t in ("xs:unsignedshort", "unsignedshort", "ushort"):
                return "H"
            if t in ("xs:short", "short"):
                return "h"
            if t in ("xs:byte", "byte", "xs:unsignedbyte", "unsignedbyte"):
                return "B"
            if t in ("xs:float", "float"):
                return "f"
            if t in ("xs:long", "long", "int64"):
                return "q"
            if t in ("xs:double", "double", "float64"):
                return "d"
            raise ValueError(f"Tipo não suportado: {vtype}")

        # Busca compatível com os arquivos SSTDataVariable
        for var in xr.findall(".//SSTDataVariable"):
            name = (var.findtext("VarName") or "").strip()
            vtype = (var.findtext("VarType") or "").strip()
            vlen_text = var.findtext("VarLength") or "1"

            try:
                vlen = int(vlen_text)
            except ValueError:
                vlen = 1

            if not name or not vtype:
                continue

            code = map_type(vtype)

            if vlen == 1:
                fmt_parts.append(code)
                names.append(name)
            else:
                fmt_parts.append(f"{vlen}{code}")
                for k in range(1, vlen + 1):
                    names.append(f"{name}_{k}")

        # Se não encontrar nada, tentar compatibilidade com o formato antigo
        if not names:
            for var in xr.findall(".//Variable"):
                name = (var.findtext("Name") or "").strip()
                vtype = (var.findtext("VarType") or "").strip()
                vlen = int(var.findtext("VarLength") or "1")
                code = map_type(vtype)
                if vlen == 1:
                    fmt_parts.append(code)
                    names.append(name)
                else:
                    fmt_parts.append(f"{vlen}{code}")
                    for k in range(1, vlen + 1):
                        names.append(f"{name}_{k}")

        fmt = "<" + "".join(fmt_parts)
        return names, fmt
