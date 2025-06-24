from datetime import datetime
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from repositories.queue.ClsGenerateFileToExportQueueRepository import ClsGenerateFileToExportQueueRepository


class ClsGenerateFileToExportQueueController:

    @staticmethod
    def enqueue_generation_request(instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, target_date: datetime):
        ClsGenerateFileToExportQueueRepository.insert_if_not_exists(instrument, resolution, target_date)

    @staticmethod
    def get_next_pending_request():
        return ClsGenerateFileToExportQueueRepository.get_next_pending_request()

    @staticmethod
    def update_status_completed(request_id):
        ClsGenerateFileToExportQueueRepository.update_status(request_id, "COMPLETED")

    @staticmethod
    def update_status_failed(request_id, error_message=""):
        ClsGenerateFileToExportQueueRepository.update_status(request_id, "FAILED", error_message)

    @staticmethod
    def populate_queue_from_stats():
        """
        Varre a coleção de estatísticas (data_availability_stats) e para cada dia/instrumento/resolução
        insere uma entrada na fila (se ainda não existir).

        Esse método é útil para garantir que todos os dias com dados disponíveis
        tenham requisições pendentes para geração de arquivos.
        """
        stats_list = ClsGenerateFileToExportQueueRepository.get_all_stats()

        total_inserted = 0
        for stat in stats_list:
            try:
                instrument = ClsInstrumentEnum(stat["instrument"])
                resolution = ClsResolutionEnum(stat["resolution"])
                target_date = stat["date"]
                ClsGenerateFileToExportQueueRepository.insert_if_not_exists(instrument, resolution, target_date)
                total_inserted += 1
            except Exception as e:
                print(f"[QUEUE-POPULATE] Falha ao processar stat {stat}: {str(e)}")

        print(f"[QUEUE-POPULATE] Fila de geração populada. Total de inserções tentadas: {total_inserted}")
