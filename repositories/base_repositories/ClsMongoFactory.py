from typing import Optional

from config.ClsSettings import ClsSettings
from enums.ClsMongoScopeEnum import ClsMongoScopeEnum
from repositories.base_repositories.ClsMongoClientProvider import ClsMongoClientProvider
from repositories.catalog.ClsInstrumentCatalogRepository import ClsInstrumentCatalogRepository


class ClsMongoFactory:
    @staticmethod
    def get_db(scope: ClsMongoScopeEnum, instrument_name: Optional[str] = None):
        if scope == ClsMongoScopeEnum.MASTER:
            client = ClsMongoClientProvider.get_master_client()
            return client[ClsSettings.MONGO_DB_MASTER]

        if scope == ClsMongoScopeEnum.PORTAL:
            client = ClsMongoClientProvider.get_master_client()
            return client[ClsSettings.MONGO_DB_PORTAL]

        if scope == ClsMongoScopeEnum.INSTRUMENT:
            entry = ClsInstrumentCatalogRepository.get_by_instrument(instrument_name or "")
            if not entry:
                raise ValueError(f"Instrumento nao encontrado no catalog: {instrument_name}")

            if entry.status != "active":
                raise ValueError(f"Instrumento inativo no catalog: {entry.instrument}")

            auth_source = "admin"
            params = entry.params or {}
            if isinstance(params, dict) and params.get("authSource"):
                auth_source = str(params.get("authSource"))

            client = ClsMongoClientProvider.get_instrument_client(entry.host, entry.port, auth_source=auth_source)
            return client[entry.db_name]

        raise ValueError(f"Scope nao suportado: {scope}")

    @staticmethod
    def get_collection(collection_name: str, scope: ClsMongoScopeEnum, instrument_name: Optional[str] = None):
        db = ClsMongoFactory.get_db(scope=scope, instrument_name=instrument_name)
        return db[collection_name]
