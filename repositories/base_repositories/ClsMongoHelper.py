# src/repositories/base_repositories/ClsMongoHelper.py

from typing import List, Optional
from datetime import datetime
import time

from pymongo import ASCENDING, errors
from pymongo.errors import PyMongoError

from enums.ClsMongoScopeEnum import ClsMongoScopeEnum
from repositories.base_repositories.ClsMongoFactory import ClsMongoFactory
from repositories.base_repositories.ClsProcessingResult import ClsProcessingResult


class ClsMongoHelper:
    # =========================
    # Collections helpers
    # =========================
    @staticmethod
    def get_collection(collection_name: str):
        # MASTER
        return ClsMongoFactory.get_collection(
            collection_name=collection_name,
            scope=ClsMongoScopeEnum.MASTER,
        )

    @staticmethod
    def get_portal_collection(collection_name: str):
        # PORTAL
        return ClsMongoFactory.get_collection(
            collection_name=collection_name,
            scope=ClsMongoScopeEnum.PORTAL,
        )

    @staticmethod
    def get_instrument_collection(collection_name: str, instrument_name: str):
        # INSTRUMENT
        return ClsMongoFactory.get_collection(
            collection_name=collection_name,
            scope=ClsMongoScopeEnum.INSTRUMENT,
            instrument_name=instrument_name,
        )

    # =========================
    # DB client helpers
    # =========================
    @staticmethod
    def get_mongo_client_master():
        return ClsMongoFactory.get_db(scope=ClsMongoScopeEnum.MASTER)

    @staticmethod
    def get_mongo_client_portal():
        return ClsMongoFactory.get_db(scope=ClsMongoScopeEnum.PORTAL)

    @staticmethod
    def get_mongo_client_instrument(instrument_name: str):
        return ClsMongoFactory.get_db(scope=ClsMongoScopeEnum.INSTRUMENT, instrument_name=instrument_name)

    # =========================
    # Inserts
    # =========================
    @staticmethod
    def insert_vo_to_mongodb(vo, collection_name: str, instrument_name: Optional[str] = None) -> bool:
        """
        Metodo legado. Insere 1 VO se ainda nao existir (quando o chamador fizer essa checagem).
        Se instrument_name for informado grava no DB do instrumento, senao grava no master.
        """
        if instrument_name:
            collection = ClsMongoHelper.get_instrument_collection(collection_name, instrument_name)
        else:
            collection = ClsMongoHelper.get_collection(collection_name)

        record_dict = vo.to_dict()
        collection.insert_one(record_dict)
        return True

    @staticmethod
    def insert_vos_to_mongodb(
        vos: List,
        collection_name: str,
        file_path: str,
        instrument_name: Optional[str] = None
    ) -> ClsProcessingResult:
        """
        Metodo principal usado pelos repositories de dados.
        Se instrument_name vier preenchido, grava no DB do instrumento.
        Caso contrario, grava no master.
        """
        if instrument_name:
            collection = ClsMongoHelper.get_instrument_collection(collection_name, instrument_name)
        else:
            collection = ClsMongoHelper.get_collection(collection_name)

        records = [vo.to_dict() for vo in vos]

        inserted_count = 0
        duplicate_count = 0
        failed_count = 0

        try:
            result = collection.insert_many(records, ordered=False)
            inserted_count = len(result.inserted_ids)
        except errors.BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            for error in write_errors:
                if error.get("code") == 11000:
                    duplicate_count += 1
                else:
                    failed_count += 1

            inserted_count = len(records) - duplicate_count - failed_count
        except Exception:
            failed_count = len(records)

        return ClsProcessingResult(inserted_count, duplicate_count, failed_count, file_path)

    # =========================
    # Queries
    # =========================
    @staticmethod
    def find_records_by_time_range(
        mongo_collection_name: str,
        date_to_generate_file: datetime,
        instrument_name: str,
        limit: int = 10000000
    ):
        start_time = datetime.combine(date_to_generate_file, datetime.min.time())
        end_time = datetime.combine(date_to_generate_file, datetime.max.time())

        query = {"UTC_TIME": {"$gte": start_time, "$lte": end_time}}

        collection = ClsMongoHelper.get_instrument_collection(mongo_collection_name, instrument_name)

        start = time.time()
        cursor = collection.find(query).sort("UTC_TIME", ASCENDING)
        if limit and limit > 0:
            cursor = cursor.limit(limit)

        records = list(cursor)
        duration = time.time() - start

        print(f"[QUERY] {len(records)} documentos encontrados em {duration:.2f} segundos.")
        return records

    @staticmethod
    def find_records_by_time_range_sst_type(
        mongo_collection_name: str,
        date_to_generate_file: datetime,
        instrument_name: str,
        sst_type: str,
        limit: int = 10000000
    ):
        start_time = datetime.combine(date_to_generate_file, datetime.min.time())
        end_time = datetime.combine(date_to_generate_file, datetime.max.time())

        query = {
            "UTC_TIME": {"$gte": start_time, "$lte": end_time},
            "SSTType": sst_type
        }

        collection = ClsMongoHelper.get_instrument_collection(mongo_collection_name, instrument_name)

        start = time.time()
        cursor = collection.find(query).sort("UTC_TIME", ASCENDING)
        if limit and limit > 0:
            cursor = cursor.limit(limit)

        records = list(cursor)
        duration = time.time() - start

        print(f"[QUERY] {len(records)} documentos encontrados em {duration:.2f} segundos.")
        return records

    # =========================
    # Deletes
    # =========================
    @staticmethod
    def delete_records(file_path: str, collection_name: str, instrument_name: str):
        collection = ClsMongoHelper.get_instrument_collection(collection_name, instrument_name)
        try:
            result = collection.delete_many({"FILEPATH": file_path})
            return result.deleted_count
        except PyMongoError as e:
            print(f"Erro ao deletar registros da colecao {collection_name} para o arquivo {file_path}: {str(e)}")
            raise
