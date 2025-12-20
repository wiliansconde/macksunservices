# src/repositories/base_repositories/ClsMongoDbRouter.py

from typing import Optional
from config.ClsSettings import ClsSettings
from enums.ClsMongoScopeEnum import ClsMongoScopeEnum
from repositories.catalog.ClsInstrumentCatalogRepository import ClsInstrumentCatalogRepository

class ClsMongoDbRouter:
    _instrument_db_cache = None

    @staticmethod
    def warmup_cache() -> None:
        if ClsMongoDbRouter._instrument_db_cache is None:
            ClsMongoDbRouter._instrument_db_cache = ClsInstrumentCatalogRepository.get_all_active()

    @staticmethod
    def resolve_db_name(scope: ClsMongoScopeEnum, instrument_name: Optional[str]) -> str:
        if scope == ClsMongoScopeEnum.MASTER:
            return ClsSettings.MONGO_DB_MASTER

        if scope == ClsMongoScopeEnum.PORTAL:
            return ClsSettings.MONGO_DB_PORTAL

        if scope == ClsMongoScopeEnum.INSTRUMENT:
            if not instrument_name:
                raise ValueError("instrument_name obrigatorio para scope INSTRUMENT")

            ClsMongoDbRouter.warmup_cache()
            instrument_norm = instrument_name.strip().upper()
            entry = ClsMongoDbRouter._instrument_db_cache.get(instrument_norm) if ClsMongoDbRouter._instrument_db_cache else None

            if not entry:
                raise ValueError(f"Instrumento nao encontrado no catalog: {instrument_norm}")

            return entry.db_name

        raise ValueError(f"Scope nao suportado: {scope}")
