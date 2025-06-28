# src/jobs/run_job_sync_exported_files_to_cloud.py

import sys
import traceback
from datetime import datetime

from controllers.export_to_cloud.ClsFileExportRegistryToCloudController import ClsFileExportRegistryToCloudController

"""
Job: run_job_sync_exported_files_to_cloud.py

Descrição:
    Sincroniza registros da coleção 'exported_files_to_cloud' do MongoDB local para o MongoDB na nuvem (Azure Cosmos).
    Apenas documentos com {"cloud_synchronized": False} são considerados para envio.

Recomendação de uso:
    ➤ Executar periodicamente via cron ou manualmente em ambiente controlado.
    ➤ Ideal para manter consistência entre os dois ambientes.

Uso manual:
    python -m jobs.run_job_sync_exported_files_to_cloud

Agendamento via cron:
    */30 * * * * root python /app/run_job_sync_exported_files_to_cloud.py >> /var/log/cron.log 2>&1
"""

class run_job_sync_exported_files_to_cloud:

    @staticmethod
    def run():
        print(f"[{datetime.now()}] [SyncJob] Iniciando sincronização da collection 'exported_files_to_cloud'...")

        try:
            # :param ignore_sync_flag: Se True, envia todos os documentos, mesmo os já sincronizados.
            #:param batch_limit: Número máximo de documentos a sincronizar por execução.
            limit = 1000 # fara com lotes de 1000
            ignore_synchronized_flag = False #True para reprocessar tudo

            total_synced = ClsFileExportRegistryToCloudController.sync_local_data_to_cloud_data(limit,ignore_synchronized_flag)
            print(f"[{datetime.now()}] [SyncJob] Total de documentos sincronizados: {total_synced}")
        except Exception as e:
            print(f"[{datetime.now()}] [SyncJob] Erro durante a sincronização:")
            print(f"[SyncJob] Exception: {str(e)}")
            traceback.print_exc()
            sys.exit(2)


if __name__ == "__main__":
    run_job_sync_exported_files_to_cloud.run()
