from services.ClsTimeSeriesExportQueueService import ClsTimeSeriesExportQueueService


class ClsTimeSeriesExportQueueController:

    @staticmethod
    def process_next_pending_request():
        ClsTimeSeriesExportQueueService.process_next_pending_request()
