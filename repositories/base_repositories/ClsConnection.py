from pymongo import MongoClient

from config.ClsSettings import ClsSettings


class ClsConnection:
    @staticmethod
    def get_mongo_data_client():
        # uri='mongodb://localhost:27018/'
        uri = ClsSettings.get_mongo_data_uri()
        return MongoClient(uri)

    @staticmethod
    def get_mongo_portal_client():
        # uri='mongodb://localhost:27018/'
        uri = ClsSettings.get_mongo_portal_uri()
        return MongoClient(uri)

    @staticmethod
    def get_mongo_data_db_name():
        return ClsSettings.MONGO_DB_DATA

    @staticmethod
    def get_mongo_portal_db_name():
        return ClsSettings.MONGO_DB_PORTAL

    # ---------------------
    # Cloud (Azure Cosmos DB)
    # ---------------------
    @staticmethod
    def get_mongo_cloud_client():
        return MongoClient(ClsSettings.get_mongo_azure_uri())

    @staticmethod
    def get_mongo_cloud_db_name():
        return ClsSettings.MONGO_AZURE_DB