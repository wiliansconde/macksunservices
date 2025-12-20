# src/repositories/partitioning/ClsPartition_map_repository.py

from datetime import datetime
from typing import List, Optional

from pymongo.errors import CollectionInvalid

from config.ClsSettings import ClsSettings
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from enums.ClsMongoScopeEnum import ClsMongoScopeEnum

from models.partitioning.ClsPartition_map_model import ClsPartitionMapModel
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from repositories.base_repositories.ClsMongoFactory import ClsMongoFactory


class ClsPartitionMapRepository:
    def find_partitions(
        self,
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        start_date: datetime,
        end_date: datetime
    ) -> List[ClsPartitionMapModel]:
        """
        Retorna uma lista de partitions que abrangem o intervalo informado.
        """
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        query = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "start_date": {"$lte": end_date},
            "end_date": {"$gte": start_date},
            "status": "active",
        }

        try:
            documents = collection.find(query).sort("start_date", 1)
            partitions: List[ClsPartitionMapModel] = []

            for doc in documents:
                try:
                    partitions.append(ClsPartitionMapModel.from_document(doc))
                except Exception as parse_error:
                    print(f"[PartitionMap] Erro ao parsear documento {doc.get('_id', 'sem_id')}: {parse_error}")

            return partitions

        except Exception as db_error:
            print(f"[PartitionMap] Erro ao consultar partition map: {db_error}")
            return []

    @staticmethod
    def insert_partition(partition: ClsPartitionMapModel) -> None:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)
        collection.insert_one(partition.to_document())

    def check_overlap(
        self,
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        start_date: datetime,
        end_date: datetime
    ) -> bool:
        partitions = self.find_partitions(instrument, resolution, start_date, end_date)
        return len(partitions) > 0

    def find_prev_partition(
        self,
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        day_start: datetime
    ) -> Optional[ClsPartitionMapModel]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        doc = collection.find_one(
            {
                "instrument": instrument.value,
                "resolution": resolution.value,
                "status": "active",
                "end_date": {"$lt": day_start},
            },
            sort=[("end_date", -1)],
        )

        return ClsPartitionMapModel.from_document(doc) if doc else None

    def find_next_partition(
        self,
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        day_end: datetime
    ) -> Optional[ClsPartitionMapModel]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        doc = collection.find_one(
            {
                "instrument": instrument.value,
                "resolution": resolution.value,
                "status": "active",
                "start_date": {"$gt": day_end},
            },
            sort=[("start_date", 1)],
        )

        return ClsPartitionMapModel.from_document(doc) if doc else None

    @staticmethod
    def create_time_series_collection_if_not_exists(
        collection_name: str,
        resolution: ClsResolutionEnum,
        instrument: ClsInstrumentEnum
    ) -> None:
        """
        Cria a collection de time series no banco do instrumento.
        O partition_map continua no master, mas os dados ficam no banco do instrumento.
        """
        db = ClsMongoFactory.get_db(
            scope=ClsMongoScopeEnum.INSTRUMENT,
            instrument_name=instrument.value,
        )

        if collection_name in db.list_collection_names():
            return

        try:
            granularity = ClsPartitionMapRepository._get_granularity_from_resolution(resolution)

            db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "UTC_TIME",
                    "granularity": granularity,
                    "bucketMaxSpanSeconds": 3600,
                },
            )

            db[collection_name].create_index({"DATE": 1})
            db[collection_name].create_index({"UTC_TIME": 1})

            print(f"[REPOSITORY] Collection '{collection_name}' criada como Time Series com granularidade '{granularity}'.")

        except CollectionInvalid:
            print(f"[REPOSITORY] Collection '{collection_name}' ja existe.")
        except Exception as e:
            print(f"[REPOSITORY] Erro ao criar time series collection '{collection_name}': {e}")
            raise

    @staticmethod
    def _get_granularity_from_resolution(resolution: ClsResolutionEnum) -> str:
        resolution_value = str(resolution.value).strip().lower()

        if resolution_value.endswith("ms") or resolution_value.endswith("s"):
            return "seconds"
        if resolution_value.endswith("m"):
            return "minutes"
        if resolution_value.endswith("h"):
            return "hours"

        raise ValueError(f"Resolucao '{resolution_value}' nao suportada para granularidade de time series.")
