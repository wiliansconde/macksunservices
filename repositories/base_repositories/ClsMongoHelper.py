import time
from datetime import datetime
from typing import List
from pymongo import errors, ASCENDING
from pymongo.errors import PyMongoError

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsConnection import ClsConnection
from repositories.base_repositories.ClsProcessingResult import ClsProcessingResult


class ClsMongoHelper:
    @staticmethod
    def get_collection(collection_name):
        client = ClsConnection.get_mongo_data_client()
        db_name = ClsConnection.get_mongo_data_db_name()
        db = client[db_name]
        return db[collection_name]

    @staticmethod
    def get_portal_collection(collection_name):
        client = ClsConnection.get_mongo_portal_client()
        db_name = ClsConnection.get_mongo_portal_db_name()
        db = client[db_name]
        return db[collection_name]

    @staticmethod
    def get_azure_portal_collection(collection_name):
        client = ClsConnection.get_mongo_cloud_client() # ClsConnection.get_mongo_portal_client()
        db_name = ClsConnection.get_mongo_portal_db_name()
        db = client[db_name]
        return db[collection_name]

    @staticmethod
    def get_mongo_client():
        return ClsConnection.get_mongo_data_client()

    @staticmethod
    def insert_vo_to_mongodb(vo, collection_name):
        client = ClsConnection.get_mongo_data_client()
        db = client[ClsConnection.get_mongo_data_db_name()]
        collection = db[collection_name]
        record_dict = vo.to_dict()

        collection.insert_one(record_dict)

    @staticmethod
    def get_data_collection(collection_name: str):
        client = ClsConnection.get_mongo_data_client()
        db = client[ClsSettings.MONGO_DB_DATA]
        return db[collection_name]

    @staticmethod
    def find_records_by_time_range(mongo_collection_name, date_to_generate_file, limit=10000000):

        """
        Busca documentos que correspondem ao intervalo especificado de tempo, com um limite opcional.
        O intervalo é definido por UTC_TIME_YEAR, UTC_TIME_MONTH, UTC_TIME_DAY e UTC_TIME_HOUR.
        """
        # Define o início do dia (00:00:00)
        start_time = datetime.combine(date_to_generate_file, datetime.min.time())

        # Define o fim do dia (23:59:59)
        end_time = datetime.combine(date_to_generate_file, datetime.max.time())

        query = {
            "UTC_TIME": {
                "$gte": start_time,
                "$lte": end_time
            }
        }

        # Executa a consulta com o limite de documentos
        collection = ClsMongoHelper.get_data_collection(mongo_collection_name)

        start = time.time()
        records = list(collection.find(query).sort("UTC_TIME", ASCENDING))  # .limit(limit)
        duration = time.time() - start

        print(f"[QUERY] {len(records)} documentos encontrados em {duration:.2f} segundos.")
        return records

    @staticmethod
    def find_records_by_time_range_sst_type(mongo_collection_name, date_to_generate_file, sst_type):

        """
        Busca documentos que correspondem ao intervalo especificado de tempo, com um limite opcional.
        O intervalo é definido por UTC_TIME_YEAR, UTC_TIME_MONTH, UTC_TIME_DAY e UTC_TIME_HOUR.
        """
        # Define o início do dia (00:00:00)
        start_time = datetime.combine(date_to_generate_file, datetime.min.time())

        # Define o fim do dia (23:59:59)
        end_time = datetime.combine(date_to_generate_file, datetime.max.time())

        query = {
            "UTC_TIME": {
                "$gte": start_time,
                "$lte": end_time
            },
            "SSTType": sst_type  # "FAST" ou "INTG"
        }

        # Executa a consulta com o limite de documentos
        collection = ClsMongoHelper.get_data_collection(mongo_collection_name)

        start = time.time()
        records = list(collection.find(query).sort("UTC_TIME", ASCENDING))  # .limit(limit)
        duration = time.time() - start

        print(f"[QUERY] {len(records)} documentos encontrados em {duration:.2f} segundos.")
        return records

    @staticmethod
    def insert_vos_to_mongodb(vos: List, collection_name: str, file_path: str) -> ClsProcessingResult:
        client = ClsConnection.get_mongo_data_client()
        db = client[ClsConnection.get_mongo_data_db_name()]
        collection = db[collection_name]

        records = [vo.to_dict() for vo in vos]

        inserted_count = 0
        duplicate_count = 0
        failed_count = 0

        try:
            # Inserir todos os documentos únicos de uma vez
            result = collection.insert_many(records, ordered=False)
            inserted_count = len(result.inserted_ids)
            print(f"Registros inseridos com sucesso na coleção {collection_name}!")
        except errors.BulkWriteError as bwe:
            # Tratar erros de escrita em massa
            write_errors = bwe.details['writeErrors']
            for error in write_errors:
                if error['code'] == 11000:  # Código de erro para duplicatas
                    duplicate_count += 1
                    print(f"Erro de duplicação na coleção {collection_name}: {error['errmsg']}")
                else:
                    failed_count += 1
                    print(f"Erro desconhecido na coleção {collection_name}: {error['errmsg']}")

            # Calculando os registros inseridos e falhados
            inserted_count = len(records) - duplicate_count - failed_count
            failed_count = failed_count

        except Exception as e:
            print(f"Erro ao inserir registros na coleção {collection_name}: {str(e)}")
            failed_count = len(records)

        return ClsProcessingResult(inserted_count, duplicate_count, failed_count, file_path)

    @staticmethod
    def delete_records(file_path, collection_name):
        collection = ClsMongoHelper.get_collection(collection_name)
        try:
            result = collection.delete_many({"FILEPATH": file_path})
            return result.deleted_count

        except PyMongoError as e:
            print(f"Erro ao deletar registros da coleção {collection_name} para o arquivo {file_path}: {str(e)}")
            raise