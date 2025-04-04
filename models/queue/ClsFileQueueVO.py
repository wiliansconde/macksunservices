from datetime import datetime
from typing import Optional

from enums.ProcessStatus import ProcessStatus
from models.base_model.ClsBaseVO import ClsBaseVO

from utils.ClsGet import ClsGet


class ClsFileQueueVO(ClsBaseVO):
    def __init__(self, file_path: str, file_full_path: str, zip_file_type: str, file_size, file_lines_qty, status: str,
                 created_by: str = 'system_initial_load',
                 created_on: Optional[datetime] = None, updated_on: Optional[datetime] = None,
                 end_timestamp: Optional[datetime] = None, collection_name: str = ''):
        self.FILEPATH = file_path
        self.FILE_FULL_PATH = file_full_path
        if zip_file_type is not None and zip_file_type != "":
            self.ZIP_FILE_TYPE = zip_file_type
        self.FILE_SIZE = file_size
        self.FILE_LINES_QTY = file_lines_qty
        self.STATUS = status
        self.USER = created_by
        self.COLLECTION_NAME = collection_name
        self.START_TIMESTAMP = created_on if created_on else ClsGet.current_time()
        self.LAST_UPDATED_TIMESTAMP = updated_on
        self.FINISHED_TIMESTAMP = end_timestamp

    def to_dict(self) -> dict:
        data_dict = {
            'FILEPATH': self.FILEPATH,
            'FILE_FULL_PATH': self.FILE_FULL_PATH,
            'FILE_SIZE': self.FILE_SIZE,
            'FILE_LINES_QTY': self.FILE_LINES_QTY,
            'USER': self.USER,
            'STATUS': self.STATUS,
            'COLLECTION_NAME': self.COLLECTION_NAME,
            'START_TIMESTAMP': self.START_TIMESTAMP,
            'LAST_UPDATED_TIMESTAMP': self.LAST_UPDATED_TIMESTAMP,
            'FINISHED_TIMESTAMP': self.FINISHED_TIMESTAMP,
        }

        if hasattr(self, 'ZIP_FILE_TYPE') and self.ZIP_FILE_TYPE:
            data_dict['ZIP_FILE_TYPE'] = self.ZIP_FILE_TYPE
        return data_dict

    @staticmethod
    def from_dict(data: dict):
        return ClsFileQueueVO(
            file_path=data['FILEPATH'],
            file_full_path=data['FILE_FULL_PATH'],
            zip_file_type=data.get('ZIP_FILE_TYPE', ""),  # Use .get() to handle missing key
            file_size=data['FILE_SIZE'],
            file_lines_qty=data['FILE_LINES_QTY'],
            created_by=data.get('USER', 'system_initial_load'),
            created_on=data.get('START_TIMESTAMP', ClsGet.current_time()),
            updated_on=data.get('LAST_UPDATED_TIMESTAMP'),
            end_timestamp=data.get('FINISHED_TIMESTAMP'),
            status=data.get('STATUS', ProcessStatus.PENDING),
            collection_name=data.get('COLLECTION_NAME', '')
        )
