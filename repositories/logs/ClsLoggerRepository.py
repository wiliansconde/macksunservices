from config.ClsSettings import ClsSettings
from enums.ProcessActions import ProcessActions
from enums.ProcessStatus import ProcessStatus
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from utils.ClsFormat import ClsFormat
from utils.ClsGet import ClsGet

class ClsLoggerRepository:
    @staticmethod
    def get_collection():
        return ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PROCESSED_FILE_TRACE)

    @staticmethod
    def insert_record(record):
        record['FILEPATH'] = ClsFormat.format_and_sanitize_path_and_remove_prefix(record['FILEPATH'])
        return ClsLoggerRepository.get_collection().insert_one(record).inserted_id

    @staticmethod
    def get_record(file_path):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        return ClsLoggerRepository.get_collection().find_one({'FILEPATH': file_path})

    @staticmethod
    def write_action(file_path, action, **kwargs):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        timestamp = ClsGet.current_time()
        try:
            action_text = action.value.format(**kwargs)
        except:
            action_text = action

        ClsLoggerRepository.get_collection().update_one(
            {'FILEPATH': file_path},
            {'$push': {'ACTIONS': {'CREATED_ON': timestamp, 'ACTION': action_text}}},
            upsert=True
        )

    @staticmethod
    def update_status(file_path, status):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        timestamp = ClsGet.current_time()
        ClsLoggerRepository.get_collection().update_one(
            {'FILEPATH': file_path},
            {'$set': {
                'LAST_UPDATED_TIMESTAMP': timestamp,
                'STATUS': status
            }}
        )

    @staticmethod
    def update_collection_name(file_path, collection_name):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        timestamp = ClsGet.current_time()
        ClsLoggerRepository.get_collection().update_one(
            {'FILEPATH': file_path},
            {'$set': {
                'LAST_UPDATED_TIMESTAMP': timestamp,
                'COLLECTION_NAME': collection_name
            }}
        )

    @staticmethod
    def update_failed_process(file_path, error_message, processing_time):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        timestamp = ClsGet.current_time()
        ClsLoggerRepository.get_collection().update_one(
            {'FILEPATH': file_path},
            {'$set': {
                'LAST_UPDATED_TIMESTAMP': timestamp,
                'STATUS': ProcessStatus.FAILED.value,
                'ERROR_MESSAGE': error_message,
                'PROCESSING_TIME': processing_time
            }}
        )

    @staticmethod
    def write_initial_log_pending_status(file_path, user):
        timestamp = ClsGet.current_time()
        record = {
            'FILEPATH': file_path,
            'USER': user,
            'CREATED_TIMESTAMP': timestamp,
            'LAST_UPDATED_TIMESTAMP': None,
            'STATUS': ProcessStatus.PENDING,
            'ACTIONS': [{'CREATED_ON': timestamp,
                         'ACTION': ProcessActions.FILE_ADDED_TO_QUEUE.value.format(file_path=file_path)}]
        }
        try:
            return ClsLoggerRepository.insert_record(record)
        except Exception as e:
            print(f"ERRO ao inserir: {str(e)}")

    @staticmethod
    def write_generic_error(file_path, error_message, stack_trace):
        timestamp = ClsGet.current_time()
        record = {
            'GENERIC_ERROR': 'GENERIC_ERROR',
            'FILEPATH': file_path,
            'CREATED_TIMESTAMP': timestamp,
            'ERROR_MESSAGE': error_message,
            'STACK_TRACE': stack_trace,
            'STATUS': 'GENERIC_ERROR',
        }
        #print('2 ' + record)
        return ClsLoggerRepository.insert_record(record)
