from datetime import datetime

class ClsPartitionMapModel:

    def __init__(self, instrument: str, resolution: str, collection_name: str, start_date: datetime,
                 end_date: datetime, storage_backend: str, status: str, created_at: datetime, updated_at: datetime):
        self.instrument = instrument
        self.resolution = resolution
        self.collection_name = collection_name
        self.start_date = start_date
        self.end_date = end_date
        self.storage_backend = storage_backend
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at

    def to_document(self) -> dict:
        return {
            "instrument": self.instrument,
            "resolution": self.resolution,
            "collection_name": self.collection_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "storage_backend": self.storage_backend,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    @staticmethod
    def from_document(doc: dict):
        return ClsPartitionMapModel(
            instrument=doc.get("instrument"),
            resolution=doc.get("resolution"),
            collection_name=doc.get("collection_name"),
            start_date=doc.get("start_date"),
            end_date=doc.get("end_date"),
            storage_backend=doc.get("storage_backend"),
            status=doc.get("status"),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at")
        )
