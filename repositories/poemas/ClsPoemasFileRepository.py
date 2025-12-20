from datetime import datetime

from config.ClsSettings import ClsSettings
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsPoemasFileRepository:
    INSTRUMENT = ClsInstrumentEnum.POEMAS

    @staticmethod
    def insert_records(records, file_path: str, mongo_collection: str):
        batch_size = ClsSettings.MONGO_BATCH_SIZE_TO_INSERT
        res = None

        instrument_name = ClsPoemasFileRepository.INSTRUMENT.value

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            res = ClsMongoHelper.insert_vos_to_mongodb(
                vos=batch,
                collection_name=mongo_collection,
                file_path=file_path,
                instrument_name=instrument_name,
            )

        return res

    @staticmethod
    def delete_records(file_path: str, mongo_collection: str):
        instrument_name = ClsPoemasFileRepository.INSTRUMENT.value

        deleted_count = ClsMongoHelper.delete_records(
            file_path=file_path,
            collection_name=mongo_collection,
            instrument_name=instrument_name,
        )

        print(f"{deleted_count} documentos deletados da colecao {mongo_collection} para o arquivo: {file_path}")

    @staticmethod
    def get_records_by_time_range(date_to_generate_file: datetime, mongo_collection_name: str, limit: int = 1000):
        instrument_name = ClsPoemasFileRepository.INSTRUMENT.value

        records = ClsMongoHelper.find_records_by_time_range(
            mongo_collection_name=mongo_collection_name,
            date_to_generate_file=date_to_generate_file,
            instrument_name=instrument_name,
            limit=limit,
        )

        if records:
            print(f"{len(records)} documentos encontrados na colecao {mongo_collection_name}")
        else:
            print(f"Nenhum documento encontrado na colecao {mongo_collection_name} para o intervalo de tempo especificado.")

        return records
