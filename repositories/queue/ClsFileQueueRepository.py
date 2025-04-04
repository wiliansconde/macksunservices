from typing import Optional
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError

from config.ClsSettings import ClsSettings
from enums.ProcessStatus import ProcessStatus
from models.queue.ClsFileQueueVO import ClsFileQueueVO
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from utils.ClsFormat import ClsFormat
from utils.ClsGet import ClsGet


class ClsFileQueueRepository:
    def insert(file_queue_vo: ClsFileQueueVO) -> bool:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        record = file_queue_vo.to_dict()
        try:
            collection.insert_one(record)
            return True
        except DuplicateKeyError as err:
            print(f"Erro de chave duplicada: {err}")
            return False
        except Exception as err:
            raise

    @staticmethod
    def get_next_pending_file() -> Optional[ClsFileQueueVO]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)

        next_file = collection.find_one_and_update(
            {'STATUS': ProcessStatus.PENDING},
            {'$set': {'STATUS': ProcessStatus.IN_PROCESSING,
                      'LAST_UPDATED_TIMESTAMP': ClsGet.current_time()}},
            sort=[('START_TIMESTAMP', ASCENDING)],
            return_document=True
        )

        if next_file:
            file_path = next_file.get('FILEPATH', '')
            try:
                collection_name = ClsSettings.get_mongo_collection_name_by_file_type(file_path)
                collection.update_one(
                    {'_id': next_file['_id']},
                    {'$set': {'COLLECTION_NAME': collection_name}}
                )
                next_file['COLLECTION_NAME'] = collection_name
            except ValueError as e:
                print(f"Error determining collection name: {str(e)}")
                return None

            return ClsFileQueueVO.from_dict(next_file)

        return None

    @staticmethod
    def update_file_status_completed(file_path: str):
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        update_fields = {
            'STATUS': ProcessStatus.COMPLETED,
            'LAST_UPDATED_TIMESTAMP': ClsGet.current_time(),
            'FINISHED_TIMESTAMP': ClsGet.current_time()
        }
        collection.update_one({'FILEPATH': file_path}, {'$set': update_fields})


    @staticmethod
    def update_file_status_failed(file_path: str, error_message: str = ''):
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        update_fields = {
            'STATUS': ProcessStatus.FAILED,
            'LAST_UPDATED_TIMESTAMP': ClsGet.current_time(),
            'FINISHED_TIMESTAMP': ClsGet.current_time(),
            'ERROR_MESSAGE': error_message
        }
        collection.update_one({'FILEPATH': file_path}, {'$set': update_fields})

    @staticmethod
    def update_file_status_pending(file_path: str):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        update_fields = {
            'STATUS': ProcessStatus.PENDING,
            'LAST_UPDATED_TIMESTAMP': ClsGet.current_time()
        }
        collection.update_one({'FILEPATH': file_path}, {'$set': update_fields})
    @staticmethod
    def update_collection_name(file_path: str, collection_name: str):
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        collection.update_one(
            {'FILEPATH': file_path},
            {'$set': {'COLLECTION_NAME': collection_name}}
        )

    @staticmethod
    def update_file_size(file_path: str, file_size_mb):
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        update_fields = {
            'FILE_SIZE': f'{file_size_mb:.2f} MB',
            'LAST_UPDATED_TIMESTAMP': ClsGet.current_time()
        }
        collection.update_one({'FILEPATH': file_path}, {'$set': update_fields})


    @staticmethod
    def update_file_lines_qty(file_path: str, lines_qty):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)
        update_fields = {
            'FILE_LINES_QTY': str(lines_qty),
            'LAST_UPDATED_TIMESTAMP': ClsGet.current_time()
        }
        collection.update_one({'FILEPATH': file_path}, {'$set': update_fields})

    @staticmethod
    def get_files_in_processing() -> list:
        # Obtém a coleção file_queue
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_FILE_QUEUE)

        # Busca todos os registros com STATUS "IN_PROCESSING"
        files_in_processing = collection.find({"STATUS": "IN_PROCESSING"}, {"FILEPATH": 1})

        # Cria uma lista com os paths dos arquivos em processamento
        file_paths = [file["FILEPATH"] for file in files_in_processing]

        return file_paths
