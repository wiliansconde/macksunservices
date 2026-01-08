from typing import Dict, Optional
from pymongo import MongoClient

from config.ClsSettings import ClsSettings


class ClsMongoClientProvider:
    _master_client: Optional[MongoClient] = None
    _instrument_clients: Dict[str, MongoClient] = {}

    @staticmethod
    def get_master_client() -> MongoClient:
        if ClsMongoClientProvider._master_client is None:
            ClsMongoClientProvider._master_client = MongoClient(ClsSettings.get_mongo_data_uri())
        return ClsMongoClientProvider._master_client

    @staticmethod
    def get_instrument_client(host: str, port: int) -> MongoClient:
        key = f"{host}:{port}"
        cached = ClsMongoClientProvider._instrument_clients.get(key)
        if cached is not None:
            return cached

        if ClsSettings.MONGO_USER and ClsSettings.MONGO_PASSWORD:
            uri = (
                f"mongodb://{ClsSettings.MONGO_USER}:{ClsSettings.MONGO_PASSWORD}"
                f"@{host}:{port}"
            )
        else:
            uri = f"mongodb://{host}:{port}"

        client = MongoClient(uri)
        ClsMongoClientProvider._instrument_clients[key] = client
        return client

