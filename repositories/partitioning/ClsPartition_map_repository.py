from typing import List, Optional
from datetime import datetime

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid

from config.ClsSettings import ClsSettings
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from enums.ClsMongoScopeEnum import ClsMongoScopeEnum
from models.partitioning.ClsPartition_map_model import ClsPartitionMapModel
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from repositories.base_repositories.ClsMongoFactory import ClsMongoFactory


class ClsPartitionMapRepository:
    @staticmethod
    def find_partitions(
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        start_date: datetime,
        end_date: datetime
    ) -> List[ClsPartitionMapModel]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        query = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "start_date": {"$lte": end_date},
            "end_date": {"$gte": start_date},
            "status": "active",
        }

        try:
            documents = collection.find(query).sort("start_date", ASCENDING)
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
    def find_prev_partition(
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        day_start: datetime
    ) -> Optional[ClsPartitionMapModel]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        query = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "status": "active",
            "end_date": {"$lt": day_start},
        }

        try:
            doc = collection.find_one(query, sort=[("end_date", DESCENDING)])
            if not doc:
                return None
            return ClsPartitionMapModel.from_document(doc)
        except Exception as e:
            print(f"[PartitionMap] Erro ao buscar prev partition: {e}")
            return None

    @staticmethod
    def find_next_partition(
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        day_end: datetime
    ) -> Optional[ClsPartitionMapModel]:
        collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_PARTITION_MAP)

        query = {
            "instrument": instrument.value,
            "resolution": resolution.value,
            "status": "active",
            "start_date": {"$gt": day_end},
        }

        try:
            doc = collection.find_one(query, sort=[("start_date", ASCENDING)])
            if not doc:
                return None
            return ClsPartitionMapModel.from_document(doc)
        except Exception as e:
            print(f"[PartitionMap] Erro ao buscar next partition: {e}")
            return None

    @staticmethod
    def insert_partition(partition: ClsPartitionMapModel):
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

    @staticmethod
    def create_time_series_collection_if_not_exists(
            collection_name: str,
            resolution: ClsResolutionEnum,
            instrument: ClsInstrumentEnum
    ):
        print("[REPOSITORY][TS] inicio create_time_series_collection_if_not_exists")
        print("[REPOSITORY][TS] collection_name:", collection_name)
        print("[REPOSITORY][TS] instrument:", instrument)
        print("[REPOSITORY][TS] resolution:", resolution)

        print("[REPOSITORY][TS] chamando ClsMongoFactory.get_db")
        db = ClsMongoFactory.get_db(
            scope=ClsMongoScopeEnum.INSTRUMENT,
            instrument_name=instrument.value,
        )

        print("[REPOSITORY][TS] db obtido com sucesso")
        print("[REPOSITORY][TS] type(db):", type(db))
        print("[REPOSITORY][TS] repr(db):", repr(db))
        print("[REPOSITORY][TS] db.name:", getattr(db, "name", None))

        client = getattr(db, "client", None)
        print("[REPOSITORY][TS] db.client:", client)
        print("[REPOSITORY][TS] type(client):", type(client))

        if client is not None:
            try:
                print("[REPOSITORY][TS] client.address:", getattr(client, "address", None))
            except Exception as e:
                print("[REPOSITORY][TS] erro client.address:", e)

            try:
                print("[REPOSITORY][TS] client.nodes:", getattr(client, "nodes", None))
            except Exception as e:
                print("[REPOSITORY][TS] erro client.nodes:", e)

            try:
                print("[REPOSITORY][TS] client.primary:", getattr(client, "primary", None))
            except Exception as e:
                print("[REPOSITORY][TS] erro client.primary:", e)

            try:
                print("[REPOSITORY][TS] client.options:", getattr(client, "options", None))
            except Exception as e:
                print("[REPOSITORY][TS] erro client.options:", e)

            try:
                address = getattr(client, "address", None)
                if address:
                    print("[REPOSITORY][TS] mongo_host:", address[0])
                    print("[REPOSITORY][TS] mongo_port:", address[1])
            except Exception as e:
                print("[REPOSITORY][TS] erro host porta:", e)

            try:
                print("[REPOSITORY][TS] listando databases visiveis")
                db_names = client.list_database_names()
                print("[REPOSITORY][TS] databases:", db_names)
            except Exception as e:
                print("[REPOSITORY][TS] erro list_database_names:", e)

        try:
            print("[REPOSITORY][TS] listando collections existentes no db")
            existing_collections = db.list_collection_names()
            print("[REPOSITORY][TS] collections existentes:", existing_collections)
        except Exception as e:
            print("[ERRO][TS] falha list_collection_names:", e)
            raise

        if collection_name in existing_collections:
            print("[REPOSITORY][TS] collection ja existe, retorno imediato")
            return

        try:
            print("[REPOSITORY][TS] resolvendo granularidade")
            granularity = ClsPartitionMapRepository._get_granularity_from_resolution(resolution)
            print("[REPOSITORY][TS] granularidade resolvida:", granularity)

            print("[REPOSITORY][TS] criando collection time series")
            print("[REPOSITORY][TS] parametros:")
            print("  timeField = UTC_TIME")
            print("  granularity =", granularity)
            print("  bucketMaxSpanSeconds = 3600")

            db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "UTC_TIME",
                    "granularity": granularity,
                    "bucketMaxSpanSeconds": 3600,
                },
            )

            print("[REPOSITORY][TS] collection criada com sucesso")

            print("[REPOSITORY][TS] criando indice DATE")
            db[collection_name].create_index({"DATE": 1})
            print("[REPOSITORY][TS] indice DATE criado")

            print("[REPOSITORY][TS] criando indice UTC_TIME")
            db[collection_name].create_index({"UTC_TIME": 1})
            print("[REPOSITORY][TS] indice UTC_TIME criado")

            print(
                "[REPOSITORY][TS] collection criada como time series:",
                collection_name,
                "granularidade:",
                granularity,
            )

        except CollectionInvalid:
            print("[REPOSITORY][TS] CollectionInvalid collection ja existe")

        except Exception as e:
            print("[ERRO][TS] excecao inesperada durante criacao da collection")
            print("[ERRO][TS] tipo:", type(e))
            print("[ERRO][TS] erro:", e)
            raise

    @staticmethod
    def _get_granularity_from_resolution(resolution: ClsResolutionEnum) -> str:
        resolution_value = resolution.value.lower()

        if resolution_value.endswith("ms") or resolution_value.endswith("s"):
            return "seconds"
        if resolution_value.endswith("m"):
            return "minutes"
        if resolution_value.endswith("h"):
            return "hours"

        raise ValueError(f"Resolucao {resolution_value} nao suportada para granularidade de time series")
