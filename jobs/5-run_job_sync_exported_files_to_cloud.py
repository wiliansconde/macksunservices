# src/jobs/run_job_sync_exported_files_to_cloud.py

import sys
import traceback
from datetime import datetime

from controllers.export_to_cloud.ClsFileExportRegistryToCloudController import ClsFileExportRegistryToCloudController

"""
Job: run_job_sync_exported_files_to_cloud.py

Descrição:
    Sincroniza de forma incremental os registros da coleção 'exported_files_to_cloud' do ambiente local
    para o ambiente de nuvem (Cosmos DB). O processo verifica o campo 'cloud_synchronized' para evitar
    duplicações e permite, opcionalmente, forçar o reenvio de todos os registros.

Recomendação de uso:
    ➤ Esse job deve ser executado regularmente para manter a base de dados da nuvem alinhada com os dados exportados localmente.
    ➤ Ideal para agendamento automático ou execução manual em casos de reprocessamento.

Uso manual:
    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute com:
       python -m jobs.run_job_sync_exported_files_to_cloud

Uso em cron (dentro de container):
    */30 * * * * root python /app/jobs/run_job_sync_exported_files_to_cloud.py >> /var/log/cron.log 2>&1

Saída:
    Log detalhado com timestamp, IDs dos registros sincronizados e mensagens de erro, se ocorrerem.

Requisitos:
    - Python 3.7+
    - Executar a partir da raiz do projeto com `-m`
    - ClsFileExportRegistryToCloudController acessível em controllers.export_to_cloud
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
