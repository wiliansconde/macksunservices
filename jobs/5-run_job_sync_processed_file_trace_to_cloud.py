# jobs/run_job_sync_processed_file_trace_to_cloud.py

import sys
import traceback
from datetime import datetime

from controllers.export_to_cloud.ClsProcessedFileTraceSyncController import ClsProcessedFileTraceSyncController

"""
Job: run_job_sync_processed_file_trace_to_cloud.py

Descrição:
    Sincroniza de forma incremental os registros da coleção 'processed_file_trace' do ambiente local
    para o ambiente de nuvem (Cosmos DB), garantindo que não haja duplicações. O processo respeita o
    campo de controle 'cloud_synchronized' para identificar registros já sincronizados, a menos que
    o parâmetro ignore_sync_flag esteja ativo.

Recomendação de uso:
    ➤ Esse job deve ser executado periodicamente para manter o ambiente de nuvem atualizado com os registros locais.
    ➤ Ideal para agendamento via cron em containers ou servidores, ou para execução sob demanda com flag de forçar envio total.

Uso manual:
    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute com:
       python -m jobs.5-run_job_sync_processed_file_trace_to_cloud

Uso em cron (dentro de container):
    */30 * * * * root python /app/jobs/run_job_sync_processed_file_trace_to_cloud.py >> /var/log/cron.log 2>&1

Saída:
    Log detalhado com horários e IDs dos registros sincronizados, além do total de documentos processados.

Requisitos:
    - Python 3.7+
    - Executar a partir da raiz do projeto com `-m`
    - ClsProcessedFileTraceSyncController acessível em controllers.export_to_cloud
"""

class run_job_sync_processed_file_trace_to_cloud:
    @staticmethod
    def run():
        try:
            # :param ignore_sync_flag: Se True, envia todos os documentos, mesmo os já sincronizados.
            #:param batch_limit: Número máximo de documentos a sincronizar por execução.
            ignore_sync_flag = True
            batch_limit = 100000000

            print(f"[{datetime.now()}] [SyncJob] Iniciando sincronização da coleção 'processed_file_trace'...")
            total_synced = ClsProcessedFileTraceSyncController.sync_local_data_to_cloud_data(ignore_sync_flag,batch_limit)
            print(f"[{datetime.now()}] [SyncJob] Total de documentos sincronizados: {total_synced}")
        except Exception:
            print(f"[{datetime.now()}] [SyncJob] Erro durante a sincronização:")
            traceback.print_exc()
            sys.exit(2)

if __name__ == "__main__":
    run_job_sync_processed_file_trace_to_cloud.run()
