from datetime import datetime
from typing import Optional

from enums.ProcessStatus import ProcessStatus
from models.base_model.ClsBaseVO import ClsBaseVO

from utils.ClsGet import ClsGet

from datetime import datetime
from typing import Optional, Any, Dict

from enums.ProcessStatus import ProcessStatus
from models.base_model.ClsBaseVO import ClsBaseVO
from utils.ClsGet import ClsGet


class ClsFileQueueVO(ClsBaseVO):
    def __init__(
        self,
        file_path: str,
        file_full_path: str,
        zip_file_type: str,
        file_size,
        file_lines_qty,
        status: str,
        created_by: str = "system_initial_load",
        created_on: Optional[datetime] = None,
        updated_on: Optional[datetime] = None,
        end_timestamp: Optional[datetime] = None,
        collection_name: str = "",
        instrument_name: str = "",
    ):
        self.FILEPATH = file_path or ""
        self.FILE_FULL_PATH = file_full_path or ""

        self.ZIP_FILE_TYPE = zip_file_type or ""

        self.FILE_SIZE = file_size if file_size is not None else 0
        self.FILE_LINES_QTY = file_lines_qty if file_lines_qty is not None else 0

        self.STATUS = status if status else ProcessStatus.PENDING
        self.USER = created_by if created_by else "system_initial_load"

        self.COLLECTION_NAME = collection_name if collection_name else ""

        self.START_TIMESTAMP = created_on if created_on else ClsGet.current_time()
        self.LAST_UPDATED_TIMESTAMP = updated_on
        self.FINISHED_TIMESTAMP = end_timestamp

        self.INSTRUMENT_NAME = (instrument_name or "").strip().upper()

    def to_dict(self) -> dict:
        data_dict = {
            "FILEPATH": self.FILEPATH,
            "FILE_FULL_PATH": self.FILE_FULL_PATH,
            "ZIP_FILE_TYPE": self.ZIP_FILE_TYPE,
            "FILE_SIZE": self.FILE_SIZE,
            "FILE_LINES_QTY": self.FILE_LINES_QTY,
            "USER": self.USER,
            "STATUS": self.STATUS,
            "COLLECTION_NAME": self.COLLECTION_NAME,
            "START_TIMESTAMP": self.START_TIMESTAMP,
            "LAST_UPDATED_TIMESTAMP": self.LAST_UPDATED_TIMESTAMP,
            "FINISHED_TIMESTAMP": self.FINISHED_TIMESTAMP,
            "INSTRUMENT_NAME": self.INSTRUMENT_NAME,
        }
        return data_dict

    @staticmethod
    def _get(data: Dict[str, Any], key: str, default: Any) -> Any:
        value = data.get(key, default)
        return default if value is None else value

    @staticmethod
    def from_dict(data: dict):
        file_path = ClsFileQueueVO._get(data, "FILEPATH", "")
        file_full_path = ClsFileQueueVO._get(data, "FILE_FULL_PATH", "")

        zip_file_type = ClsFileQueueVO._get(data, "ZIP_FILE_TYPE", "")

        file_size = ClsFileQueueVO._get(data, "FILE_SIZE", 0)
        file_lines_qty = ClsFileQueueVO._get(data, "FILE_LINES_QTY", 0)

        created_by = ClsFileQueueVO._get(data, "USER", "system_initial_load")
        created_on = ClsFileQueueVO._get(data, "START_TIMESTAMP", ClsGet.current_time())
        updated_on = data.get("LAST_UPDATED_TIMESTAMP")
        end_timestamp = data.get("FINISHED_TIMESTAMP")

        status = ClsFileQueueVO._get(data, "STATUS", ProcessStatus.PENDING)
        collection_name = ClsFileQueueVO._get(data, "COLLECTION_NAME", "")

        instrument_name = (
            data.get("INSTRUMENT_NAME")
            or data.get("instrument_name")
            or ""
        )

        return ClsFileQueueVO(
            file_path=file_path,
            file_full_path=file_full_path,
            zip_file_type=zip_file_type,
            file_size=file_size,
            file_lines_qty=file_lines_qty,
            status=status,
            created_by=created_by,
            created_on=created_on,
            updated_on=updated_on,
            end_timestamp=end_timestamp,
            collection_name=collection_name,
            instrument_name=instrument_name,
        )
