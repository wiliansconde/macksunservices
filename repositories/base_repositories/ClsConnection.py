from pymongo import MongoClient

from config.ClsSettings import ClsSettings


class ClsConnection:
    @staticmethod
    def get_mongo_client():
        # uri='mongodb://localhost:27018/'
        uri = ClsSettings.get_mongo_uri()
        return MongoClient(uri)

    @staticmethod
    def get_mongo_db_name():
        return ClsSettings.MONGO_DB_NAME
