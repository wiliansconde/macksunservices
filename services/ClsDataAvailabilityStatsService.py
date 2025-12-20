from datetime import datetime, timedelta

from config.ClsSettings import ClsSettings
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsDataAvailabilityStatsService:

    @staticmethod
    def recalculate_for_day(instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, target_date: datetime, collection_name: str):
        #collection = ClsMongoHelper.get_data_collection(collection_name)
        collection = ClsMongoHelper.get_instrument_collection(
            collection_name=collection_name,
            instrument_name=instrument.value,
        )

        start_day = datetime(target_date.year, target_date.month, target_date.day)
        end_day = start_day + timedelta(days=1)

        query = {"UTC_TIME": {"$gte": start_day, "$lt": end_day}}

        count = collection.count_documents(query)

        if count > 0:
            stat_doc = {
                "instrument": instrument.value,
                "resolution": resolution.value,
                "date": start_day,
                "collection_name": collection_name,
                "documents_count": count,
                "last_calculated": datetime.utcnow()
            }

            stats_collection = ClsMongoHelper.get_collection(ClsSettings.MONGO_COLLECTION_DATA_AVAILABILITY_STATS)
            stats_collection.update_one(
                {"instrument": instrument.value, "resolution": resolution.value, "date": start_day},
                {"$set": stat_doc},
                upsert=True
            )

            print(f"[STATS] Estatística atualizada para {instrument.value} - {resolution.value} - {start_day.date()}")

        else:
            print(f"[STATS] Nenhum documento encontrado em {collection_name} para o dia {start_day.date()}. Nenhuma stat criada.")
