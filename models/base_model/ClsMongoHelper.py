from datetime import datetime
import re

from Common.ClsFormat import ClsFormat
from typing import List
from MongoDB.ClsConnection import ClsConnection
from pymongo import errors

from MongoDB.ClsProcessingResult import ClsProcessingResult


class ClsMongoHelper:
    @staticmethod
    def get_collection(collection_name):
        client = ClsConnection.get_mongo_client()
        db_name = ClsConnection.get_mongo_db_name()
        db = client[db_name]
        return db[collection_name]

    @staticmethod
    def insert_vo_to_mongodb(vo, collection_name):
        client = ClsConnection.get_mongo_client()
        db = client[ClsConnection.get_mongo_db_name()]
        collection = db[collection_name]
        record_dict = vo.to_dict()

        # Definir os campos que devem ser únicos
        unique_fields = {"TIME": record_dict["TIME"], "AZIPOS": record_dict["AZIPOS"], "ELEPOS": record_dict["ELEPOS"]}

        # Verificar a existência do documento
        existing = collection.find_one(unique_fields)

        if not existing:
            collection.insert_one(record_dict)

    @staticmethod
    def insert_vos_to_mongodb(vos: List, collection_name: str, file_path: str) -> ClsProcessingResult:
        client = ClsConnection.get_mongo_client()
        db = client[ClsConnection.get_mongo_db_name()]
        collection = db[collection_name]

        # Converter lista de VOs para uma lista de dicionários
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
