from typing import Optional

from config.ClsSettings import ClsSettings
from models.catalog.ClsInstrumentCatalogModel import ClsInstrumentCatalogModel
from repositories.base_repositories.ClsMongoClientProvider import ClsMongoClientProvider


class ClsInstrumentCatalogRepository:
    COLLECTION = "instrument_catalog"

    @staticmethod
    def get_by_instrument(instrument_name: str) -> Optional[ClsInstrumentCatalogModel]:
        name = (instrument_name or "").strip().upper()
        if not name:
            return None

        client = ClsMongoClientProvider.get_master_client()
        db = client[ClsSettings.MONGO_DB_MASTER]

        doc = db[ClsInstrumentCatalogRepository.COLLECTION].find_one(
            {"instrument": name, "status": "active"}
        )
        if not doc:
            return None

        return ClsInstrumentCatalogModel.from_document(doc)
