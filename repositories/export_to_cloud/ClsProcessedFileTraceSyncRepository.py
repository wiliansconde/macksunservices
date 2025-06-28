from datetime import datetime
from bson import ObjectId
from pymongo import ASCENDING

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsProcessedFileTraceSyncRepository:
    _cloud_collection = None  # cache da collection

    @staticmethod
    def get_local_collection():
        """
        Obtém a referência da collection local.
        """
        return ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PROCESSED_FILE_TRACE)

    @staticmethod
    def get_cloud_collection():
        """
        Obtém a referência da collection no ambiente de nuvem (Cosmos DB).
        Usa cache para evitar reconexão em chamadas subsequentes.
        """
        if ClsProcessedFileTraceSyncRepository._cloud_collection is None:
            ClsProcessedFileTraceSyncRepository._cloud_collection = ClsMongoHelper.get_azure_portal_collection(
                ClsSettings.MONGO_COLLECTION_PROCESSED_FILE_TRACE
            )
        return ClsProcessedFileTraceSyncRepository._cloud_collection

    @staticmethod
    def find_unsynchronized_records(limit=1000, ignore_flag=False):
        """
        Retorna registros locais ainda não sincronizados com a nuvem.

        :param limit: Quantidade máxima de registros a retornar
        :param ignore_flag: Se True, ignora o campo cloud_synchronized e retorna tudo
        """
        collection = ClsProcessedFileTraceSyncRepository.get_local_collection()
        if ignore_flag:
            return list(collection.find().sort("CREATED_TIMESTAMP", ASCENDING).limit(limit))
        else:
            return list(collection.find({
                "$or": [
                    {"cloud_synchronized": {"$exists": False}},
                    {"cloud_synchronized": False}
                ]
            }).sort("CREATED_TIMESTAMP", ASCENDING).limit(limit))

    @staticmethod
    def insert_into_cloud(record: dict):
        """
        Insere ou atualiza o registro na collection da nuvem com base no FILEPATH.
        Remove o _id original para evitar conflito.
        """
        cloud_collection = ClsProcessedFileTraceSyncRepository.get_cloud_collection()
        record_copy = dict(record)
        record_copy.pop("_id", None)
        record_copy["synchronized_at"] = datetime.utcnow()

        cloud_collection.update_one(
            {"FILEPATH": record_copy.get("FILEPATH")},
            {"$set": record_copy},
            upsert=True
        )

    @staticmethod
    def mark_as_synchronized(_id: ObjectId):
        """
        Marca o registro local como sincronizado com a nuvem.
        """
        collection = ClsProcessedFileTraceSyncRepository.get_local_collection()
        collection.update_one({"_id": _id}, {"$set": {"cloud_synchronized": True}})
