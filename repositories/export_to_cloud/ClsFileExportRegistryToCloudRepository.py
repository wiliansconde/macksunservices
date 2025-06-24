from datetime import datetime
from pymongo import ASCENDING

from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsFileExportRegistryToCloudRepository:

    @staticmethod
    def get_collection():
        """
        Retorna a referência para a coleção de registro de exportação.
        """
        return ClsMongoHelper.get_data_collection(ClsSettings.MONGO_COLLECTION_FILE_EXPORT_REGISTRY_TO_CLOUD)

    @staticmethod
    def insert_export_record(record: dict):
        """
        Insere ou atualiza o registro de exportação. Se já existir um registro para o mesmo
        instrument + resolution + date + format, ele será sobrescrito.
        """
        collection = ClsMongoHelper.get_data_collection(ClsSettings.MONGO_COLLECTION_FILE_EXPORT_REGISTRY_TO_CLOUD)

        filter_query = {
            "instrument": record["instrument"],
            "resolution": record["resolution"],
            "date": record["date"],
            "format": record["format"]
        }

        collection.delete_many(filter_query)  # Clean up previous versions ()

        collection.insert_one({
            **record,
            "created_at": datetime.utcnow()
        })

        print(
            f"[EXPORT-REGISTRY] Registro inserido para {record['instrument']} - {record['resolution']} - {record['date']} - {record['format']}")

    @staticmethod
    def update_status(request_id, new_status, error_message=None):
        """
        Atualiza o status do registro de exportação.

        :param request_id: ID do MongoDB (_id)
        :param new_status: Novo status ("COMPLETED", "FAILED", etc)
        :param error_message: Texto do erro (opcional, só usado para status FAILED)
        """
        collection = ClsFileExportRegistryToCloudRepository.get_collection()
        update_fields = {
            "status": new_status,
            "lastUpdated": datetime.utcnow()
        }
        if error_message:
            update_fields["error_message"] = error_message

        result = collection.update_one(
            {"_id": request_id},
            {"$set": update_fields}
        )

        print(f"[EXPORT-REGISTRY] Status atualizado para {new_status} para _id: {request_id}")

    @staticmethod
    def find_pending_exports():
        """
        Retorna todos os registros com status 'PENDING'.
        """
        collection = ClsFileExportRegistryToCloudRepository.get_collection()
        return list(collection.find({"status": "PENDING"}).sort("createdAt", ASCENDING))
