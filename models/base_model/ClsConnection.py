from pymongo import MongoClient

class ClsConnection:
    @staticmethod
    def get_mongo_client(uri='mongodb://localhost:27017/'):
        return MongoClient(uri)

    @staticmethod
    def get_mongo_db_name():
        return "db_craam"
