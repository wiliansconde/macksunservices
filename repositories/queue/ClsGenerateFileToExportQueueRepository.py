from datetime import datetime
from pymongo.errors import DuplicateKeyError
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from config.ClsSettings import ClsSettings
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from bson.objectid import ObjectId


class ClsGenerateFileToExportQueueRepository:

    @staticmethod
    def get_all_stats():
        """
        Retorna todas as estatísticas da coleção de stats para alimentar a fila de geração.
        """
        stats_collection = ClsMongoHelper.get_data_collection(ClsSettings.MONGO_COLLECTION_DATA_AVAILABILITY_STATS)
        stats = list(stats_collection.find({}, {"instrument": 1, "resolution": 1, "date": 1}))
        return stats

    @staticmethod
    def insert_if_not_exists(instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, target_date: datetime):
        """
        Insere uma nova entrada na fila de geração, apenas se ainda não existir para o mesmo
        instrumento, resolução e data.
        """
        queue_collection = ClsMongoHelper.get_data_collection(ClsSettings.MONGO_COLLECTION_GENERATE_FILE_QUEUE)

        query = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "date": target_date
        }

        existing = queue_collection.find_one(query)

        if existing:
            print(f"[QUEUE] Já existe uma requisição para {instrument.value} - {resolution.value} - {target_date.date()}")
            return

        doc = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "date": target_date,
            "status": "PENDING",
            "createdAt": datetime.utcnow(),
            "lastUpdated": datetime.utcnow()
        }

        try:
            queue_collection.insert_one(doc)
            print(f"[QUEUE] Nova requisição enfileirada: {instrument.value} - {resolution.value} - {target_date.date()}")
        except DuplicateKeyError:
            print(f"[QUEUE] DuplicateKeyError ao tentar inserir para {instrument.value} - {resolution.value} - {target_date.date()}")

    @staticmethod
    def get_next_pending_request():
        """
        Busca a próxima requisição pendente para geração de arquivo.
        """
        queue_collection = ClsMongoHelper.get_data_collection(ClsSettings.MONGO_COLLECTION_GENERATE_FILE_QUEUE)

        request = queue_collection.find_one_and_update(
            {"status": "PENDING"},
            {"$set": {"status": "IN_PROCESS", "lastUpdated": datetime.utcnow()}},
            sort=[("createdAt", 1)],
            return_document=True
        )

        return request

    @staticmethod
    def update_status(request_id: ObjectId, new_status: str, error_message: str = ""):
        """
        Atualiza o status de uma requisição da fila.
        """
        queue_collection = ClsMongoHelper.get_data_collection(ClsSettings.MONGO_COLLECTION_GENERATE_FILE_QUEUE)

        update_fields = {
            "status": new_status,
            "lastUpdated": datetime.utcnow()
        }

        if new_status == "FAILED" and error_message:
            update_fields["errorMessage"] = error_message

        queue_collection.update_one(
            {"_id": request_id},
            {"$set": update_fields}
        )
