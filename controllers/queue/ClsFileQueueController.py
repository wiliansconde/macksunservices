from services.ClsFileQueueService import ClsFileQueueService


class ClsFileQueueController:
    @staticmethod
    def insert(file_path: str):
        ClsFileQueueService.insert(file_path)

    @staticmethod
    def process_next_file():
        ClsFileQueueService.process_next_file()

    @staticmethod
    def update_file_status_completed(file_path: str):
        ClsFileQueueService.update_file_status_completed(file_path)

    @staticmethod
    def update_file_status_failed(file_path: str, error_message: str):
        ClsFileQueueService.update_file_status_failed(file_path, error_message)

    @staticmethod
    def update_collection_name(file_path: str, collection_name: str):
        ClsFileQueueService.update_collection_name(file_path, collection_name)

    @staticmethod
    def update_file_size(file_path: str, file_size_mb):
        ClsFileQueueService.update_file_size(file_path, file_size_mb)

    @staticmethod
    def update_file_lines_qty(file_path: str, lines_qty):
        ClsFileQueueService.update_file_lines_qty(file_path, lines_qty)

    @staticmethod
    def delete_incomplete_records_and_reset_queue_status():
        ClsFileQueueService.delete_incomplete_records_and_reset_queue_status()

    @staticmethod
    def count_pending_files() -> int:
        return ClsFileQueueService.count_pending_files()

