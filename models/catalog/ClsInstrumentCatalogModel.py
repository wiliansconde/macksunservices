from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class ClsInstrumentCatalogModel:
    instrument: str
    db_name: str
    status: str
    host: str
    port: int
    params: Dict[str, Any]

    @staticmethod
    def from_document(doc: Dict[str, Any]) -> "ClsInstrumentCatalogModel":
        instrument = str(doc.get("instrument", "")).strip().upper()
        db_name = str(doc.get("db_name", "")).strip()
        status = str(doc.get("status", "active")).strip().lower()

        conn = doc.get("connection") or {}

        host = str(conn.get("host", "")).strip()

        port_raw = conn.get("port", None)
        try:
            port = int(port_raw) if port_raw is not None else 0
        except Exception:
            port = 0

        params = conn.get("params") or {}

        if not instrument:
            raise ValueError("instrument_catalog invalido: instrument vazio")

        if not db_name:
            raise ValueError(f"instrument_catalog invalido: db_name vazio para {instrument}")

        if not host:
            raise ValueError(f"instrument_catalog invalido: connection.host vazio para {instrument}")

        if port < 1 or port > 65535:
            raise ValueError(f"instrument_catalog invalido: connection.port invalido para {instrument}: {port}")

        return ClsInstrumentCatalogModel(
            instrument=instrument,
            db_name=db_name,
            status=status,
            host=host,
            port=port,
            params=params,
        )
