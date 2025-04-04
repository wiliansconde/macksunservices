import os
from enums.ProcessActions import ProcessActions
from enums.ProcessStatus import ProcessStatus
from repositories.logs.ClsLoggerRepository import ClsLoggerRepository
from utils.ClsFormat import ClsFormat
from utils.ClsGet import ClsGet


class ClsLoggerService:
    @staticmethod
    def write_initial_log_pending_status(file_path, user):
        file_path = ClsFormat.format_and_sanitize_path_and_remove_prefix(file_path)
        ClsLoggerRepository.write_initial_log_pending_status(file_path, user)

    @staticmethod
    def write_file_lines_qty(file_path, total_lines):
        ClsLoggerService._write_action(file_path, ProcessActions.FILE_LINES_FOUND, total_lines=total_lines)

    @staticmethod
    def write_file_selected_for_processing(file_path):
        ClsLoggerService._write_action(file_path, f"ChangeStatus: {ProcessStatus.IN_PROCESSING}")
        ClsLoggerService._update_status_and_write_action(file_path, ProcessStatus.IN_PROCESSING,
                                                         ProcessActions.SELECTED_FOR_PROCESSING)

    @staticmethod
    def write_file_reset_to_pending(file_path):
        ClsLoggerService._write_action(file_path, f"ChangeStatus: {ProcessStatus.PENDING}")
        ClsLoggerService._update_status_and_write_action(file_path, ProcessStatus.PENDING,
                                                         ProcessActions.RESET_TO_PENDING)

    @staticmethod
    def write_lines_inserted(file_path, inserted_lines):
        ClsLoggerService._write_action(file_path, ProcessActions.LINES_INSERTED, inserted_lines=inserted_lines)

    @staticmethod
    def write_unziped_file(original_path, new_path):
        ClsLoggerService._write_action(original_path, ProcessActions.UNZIPPED_FILE, original_path=original_path,
                                       new_path=new_path)

    @staticmethod
    def write_duplicate_lines(file_path, duplicate_lines):
        ClsLoggerService._write_action(file_path, ProcessActions.DUPLICATE_LINES, duplicate_lines=duplicate_lines)

    @staticmethod
    def write_failed_lines(file_path, failed_lines):
        ClsLoggerService._write_action(file_path, ProcessActions.FAILED_LINES, failed_lines=failed_lines)

    @staticmethod
    def write_processing_batch(file_path, batch_size, collection_name):
        ClsLoggerService._write_action(file_path, ProcessActions.BATCH_SIZE, batch_size=batch_size)

    @staticmethod
    def write_complete_process_success(file_path):
        ClsLoggerService._write_action(file_path, f"ChangeStatus: {ProcessStatus.COMPLETED}")
        ClsLoggerService._update_status_and_write_action(file_path, ProcessStatus.COMPLETED,
                                                         ProcessActions.PROCESSING_COMPLETED)

    @staticmethod
    def write_file_size(file_path, file_size):
        ClsLoggerService._write_action(file_path, ProcessActions.FILE_SIZE, file_size=file_size)

    @staticmethod
    def write_complete_process_failed(file_path, error_message, error_trace):
        ClsLoggerService._write_action(file_path,f"ChangeStatus: {ProcessStatus.FAILED}")
        ClsLoggerService._update_status_and_write_action(file_path, ProcessStatus.FAILED,
                                                         ProcessActions.PROCESSING_ERROR, error_message=error_message, stack_trace=error_trace)

    @staticmethod
    def _write_action(file_path, action, **kwargs):
        ClsLoggerRepository.write_action(file_path, action, **kwargs)

    @staticmethod
    def _update_status_and_write_action(file_path, status, action, **kwargs):
        ClsLoggerService._write_action(file_path, action, **kwargs)
        ClsLoggerRepository.update_status(file_path, status)

    @staticmethod
    def write_generic_error(file_full_path, error_message, error_trace):
        ClsLoggerRepository.write_generic_error(file_full_path, error_message, error_trace)
