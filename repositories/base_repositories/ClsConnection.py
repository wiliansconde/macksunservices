from pymongo import MongoClient

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoClientProvider import ClsMongoClientProvider


class ClsConnection:
    @staticmethod
    @staticmethod
    def get_mongo_data_client():
        uri = ClsSettings.get_mongo_data_uri()
        print("[DEBUG][Connection] criando client do master")
        return MongoClient(uri)

    @staticmethod
    def get_mongo_data_db_name():
        print(f"[DEBUG][Connection] master db_name={ClsSettings.MONGO_DB_MASTER}")
        return ClsSettings.MONGO_DB_MASTER
    @staticmethod
    def get_mongo_portal_client() -> MongoClient:
        return ClsMongoClientProvider.get_master_client()


    @staticmethod
    def get_mongo_portal_db_name() -> str:
        return ClsSettings.MONGO_DB_PORTAL
