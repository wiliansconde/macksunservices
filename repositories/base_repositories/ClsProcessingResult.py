class ClsProcessingResult:
    def __init__(self, inserted_count, duplicate_count, failed_count, file_path):
        self.inserted_count = inserted_count
        self.duplicate_count = duplicate_count
        self.failed_count = failed_count
        self.file_path = file_path

    def __str__(self):
        return (f"Processing Result - File: {self.file_path}\n"
                f"Inserted: {self.inserted_count}, Duplicates: {self.duplicate_count}, "
                f"Failed: {self.failed_count}")
