import os
import numpy as np

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsPoemasFileRepository:
    @staticmethod
    def insert_records(records, file_path, mongo_collection):
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT
        ##mongo_collection = ClsSettings.get_mongo_collection_name_by_file_type(file_path)
        res = None
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            res = ClsMongoHelper.insert_vos_to_mongodb(batch, mongo_collection, file_path)
        return res

    @staticmethod
    def delete_records(file_path, mongo_collection):
        #mongo_collection = ClsSettings.get_mongo_collection_name_by_file_type(file_path)
        # Deletar registros da coleção de dados baseada no file_path
        deleted_count = ClsMongoHelper.delete_records(file_path, mongo_collection)
        print(f"{deleted_count} documentos deletados da coleção {mongo_collection} para o arquivo: {file_path}")

    @staticmethod
    def get_records_by_time_range(date_to_generate_file, mongo_collection_name, limit=1000):
        """
        Obtém os registros com base no intervalo de tempo fornecido e aplica um limite opcional.
        """
        #mongo_collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_DATA_POEMAS_FILE_10ms)
        records = ClsMongoHelper.find_records_by_time_range(mongo_collection_name, date_to_generate_file, limit)

        if records:
            print(f"{len(records)} documentos encontrados na coleção {mongo_collection_name}")
        else:
            print(f"Nenhum documento encontrado na coleção {mongo_collection_name} para o intervalo de tempo especificado.")

        return records

