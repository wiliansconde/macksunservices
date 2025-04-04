from enum import Enum


class ProcessActions(Enum):
    FILE_ADDED_TO_QUEUE = "File added to queue: {file_path}"
    SELECTED_FOR_PROCESSING = "Selected for processing"
    FILE_LINES_FOUND = "Lines found in file: {total_lines}"
    LINES_INSERTED = "Lines successfully inserted in database: {inserted_lines}"
    BATCH_SIZE = "Processing batch with: {batch_size} lines"
    PROCESSING_COMPLETED = "Processing completed"
    PROCESSING_ERROR = "Processing error: {error_message} trace: {stack_trace}"
    UNZIPPED_FILE = "Unzipped file: {original_path} to file: {new_path}"
    FILE_SIZE = "File size: {file_size} MB"
    RESET_TO_PENDING = (
        "Processing was halted, records deleted from collection, "
        "and queue status reset to PENDING for file: {file_path}")