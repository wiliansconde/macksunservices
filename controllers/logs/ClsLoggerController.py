from services.ClsLoggerService import ClsLoggerService


class ClsLoggerController:
    @staticmethod
    def write_started_process(file_path, user):
        return ClsLoggerService.write_initial_log_pending_status(file_path, user)

    @staticmethod
    def write_file_read(file_path, total_lines):
        ClsLoggerService.write_file_read(file_path, total_lines)

    @staticmethod
    def write_file_selected_for_processing(file_path):
        ClsLoggerService.write_file_selected_for_processing(file_path)

    @staticmethod
    def write_lines_inserted(file_path, inserted_lines):
        ClsLoggerService.write_lines_inserted(file_path, inserted_lines)

    @staticmethod
    def write_unziped_file(original_path, new_path):
        ClsLoggerService.write_unziped_file(original_path, new_path)

    @staticmethod
    def write_duplicate_lines(file_path, duplicate_lines):
        ClsLoggerService.write_duplicate_lines(file_path, duplicate_lines)

    @staticmethod
    def write_failed_lines(file_path, failed_lines):
        ClsLoggerService.write_failed_lines(file_path, failed_lines)

    @staticmethod
    def write_processing_batch(file_path, batch_size, collection_name):
        ClsLoggerService.write_processing_batch(file_path, batch_size, collection_name)

    @staticmethod
    def write_complete_process_success(file_path):
        ClsLoggerService.write_complete_process_success(file_path)

    @staticmethod
    def write_complete_process_failed(file_path, error_message):
        ClsLoggerService.write_complete_process_failed(file_path, error_message)

    @staticmethod
    def write_fail_process(file_path, error_message):
        ClsLoggerService.write_fail_process(file_path, error_message)
