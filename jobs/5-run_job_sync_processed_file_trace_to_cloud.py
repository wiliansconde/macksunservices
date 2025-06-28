# jobs/run_job_sync_processed_file_trace_to_cloud.py

import sys
import traceback
from datetime import datetime

from controllers.export_to_cloud.ClsProcessedFileTraceSyncController import ClsProcessedFileTraceSyncController


class run_job_sync_processed_file_trace_to_cloud:
    @staticmethod
    def run():
        try:
            # :param ignore_sync_flag: Se True, envia todos os documentos, mesmo os já sincronizados.
            #:param batch_limit: Número máximo de documentos a sincronizar por execução.
            ignore_sync_flag = True
            batch_limit = 1000

            print(f"[{datetime.now()}] [SyncJob] Iniciando sincronização da coleção 'processed_file_trace'...")
            total_synced = ClsProcessedFileTraceSyncController.sync_local_data_to_cloud_data(ignore_sync_flag,batch_limit)
            print(f"[{datetime.now()}] [SyncJob] Total de documentos sincronizados: {total_synced}")
        except Exception:
            print(f"[{datetime.now()}] [SyncJob] Erro durante a sincronização:")
            traceback.print_exc()
            sys.exit(2)

if __name__ == "__main__":
    run_job_sync_processed_file_trace_to_cloud.run()
