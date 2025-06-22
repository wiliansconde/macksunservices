from typing import Optional, List
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError, CollectionInvalid
from config.ClsSettings import ClsSettings
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from models.partitioning.ClsPartition_map_model import ClsPartitionMapModel
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from datetime import datetime

class ClsPartitionMapRepository:

    @staticmethod
    def find_partitions(instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, start_date: datetime, end_date: datetime) -> List[ClsPartitionMapModel]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        query = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "start_date": {"$lte": end_date},
            "end_date": {"$gte": start_date}
        }

        documents = collection.find(query)
        return [ClsPartitionMapModel.from_document(doc) for doc in documents]

    @staticmethod
    def insert_partition(partition: ClsPartitionMapModel):
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)
        collection.insert_one(partition.to_document())

    @staticmethod
    def check_overlap(instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, start_date: datetime, end_date: datetime) -> bool:
        partitions = ClsPartitionMapRepository.find_partitions(instrument, resolution, start_date, end_date)
        return len(partitions) > 0

    @staticmethod
    def create_time_series_collection_if_not_exists(collection_name: str, resolution: ClsResolutionEnum):
        mongo_client = ClsMongoHelper.get_mongo_client()
        db_name = ClsSettings.MONGO_DB_DATA
        db = mongo_client[db_name]

        if collection_name not in db.list_collection_names():
            try:
                granularity = ClsPartitionMapRepository._get_granularity_from_resolution(resolution)

                db.create_collection(
                    collection_name,
                    timeseries={
                        "timeField": "UTC_TIME",
                        "granularity": granularity,
                        "bucketMaxSpanSeconds": 3600
                    }
                )

                # Criação dos índices adicionais
                db[collection_name].create_index({"DATE": 1})
                db[collection_name].create_index({"UTC_TIME": 1})

                print(f"[REPOSITORY] Collection '{collection_name}' criada como Time Series com granularidade '{granularity}' e bucketMaxSpanSeconds 3600.")

            except CollectionInvalid:
                print(f"[REPOSITORY] Collection '{collection_name}' já existe (erro ao criar).")

    @staticmethod
    def _get_granularity_from_resolution(resolution: ClsResolutionEnum) -> str:
        """
        Define a granularidade MongoDB com base na resolução.
        """
        resolution_value = resolution.value.lower()
        if resolution_value.endswith("ms") or resolution_value.endswith("s"):
            return "seconds"
        elif resolution_value.endswith("m"):
            return "minutes"
        elif resolution_value.endswith("h"):
            return "hours"
        else:
            raise ValueError(f"Resolução '{resolution_value}' não suportada para granularidade de time-series.")
