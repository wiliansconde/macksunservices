from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum

from datetime import datetime

from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from repositories.partitioning.ClsPartition_map_repository import ClsPartitionMapRepository
from repositories.queue.ClsTimeSeriesExportQueueRepository import ClsTimeSeriesExportQueueRepository


class ClsTimeSeriesExportQueueService:

    @staticmethod
    def process_next_pending_request():
        request = ClsTimeSeriesExportQueueRepository.get_next_pending_request()
        if not request:
            print("[QUEUE] Nenhuma requisição pendente.")
            return

        try:
            start_date = request["startDate"]
            end_date = request["endDate"]
            instruments = request["selectedInstruments"]
            output_formats = request["outputFormats"]

            for instrument_str in instruments:
                instrument = ClsInstrumentEnum[instrument_str]  # Convertendo de string para Enum

                resolutions_by_channel = request["selectedResolutionsByChannel"].get(instrument_str, {})

                for channel, resolution_list in resolutions_by_channel.items():
                    for resolution_str in resolution_list:
                        resolution_clean = ClsTimeSeriesExportQueueService._extract_resolution_from_string(
                            resolution_str)
                        resolution = ClsResolutionEnum(resolution_clean)  # Convertendo de string para Enum

                        partitions = ClsPartitionMapRepository.find_partitions(instrument, resolution, start_date,
                                                                               end_date)

                        for partition in partitions:
                            collection = ClsMongoHelper.get_data_collection(partition.collection_name)
                            records = ClsMongoHelper.find_records_by_time_range(collection, start_date, end_date)

                            # Exemplo: só POEMAS implementado por enquanto
                            if instrument == ClsInstrumentEnum.POEMAS:
                                print(
                                    f"[EXPORT] Instrumento: {instrument.value} | Resolution: {resolution.value} | Channel: {channel}")

            #ClsTimeSeriesExportQueueRepository.update_status(request["_id"], "COMPLETED")

        except Exception as e:
            print(f"[QUEUE] Erro ao processar: {str(e)}")
            #ClsTimeSeriesExportQueueRepository.update_status(request["_id"], "FAILED")

    @staticmethod
    def _extract_resolution_from_string(resolution_str: str) -> str:
        parts = resolution_str.split("_")
        return parts[-1]
