import sys
import traceback
from datetime import datetime
from controllers.queue.ClsFileQueueController import ClsFileQueueController
from controllers.queue.ClsGenerateFileToExportQueueController import ClsGenerateFileToExportQueueController

"""
Job: run_job_process_queue.py

Descrição:
    Processa todos os arquivos em fila com status PENDING, um a um,
    chamando ClsFileQueueController.process_next_file() em loop.

Recomendação de uso:
    ➤ Esse job deve ser executado após a fila ser preenchida por outro processo.
    ➤ Ideal para agendamentos periódicos em containers ou servidores.


Uso manual:
    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute com:
       python -m jobs.2-run_job_file_ingestion_queue_processor

Uso em cron (dentro de container):
    */30 * * * * root python /app/run_job_process_queue.py >> /var/log/cron.log 2>&1

Saída:
    Log indicando a quantidade total de arquivos processados e o andamento.

Requisitos:
    - Python 3.7+
    - Executar a partir da raiz do projeto com `-m`
    - ClsFileQueueController acessível em controllers.queue
"""

class run_job_file_ingestion_queue_processor:
    @staticmethod
    def run():
        try:

            total = ClsFileQueueController.count_pending_files()
            print(f"[{datetime.now()}] [Processor] Iniciando processamento de {total} arquivos na fila...")

            for i in range(total):

                print(f"[Processor] {i+1}/{total} processando...")
                ClsFileQueueController.process_next_file()

            print(f"[{datetime.now()}] [Processor] Atualizando fila de geração de arquivos (Generate File Queue)...")
            ClsGenerateFileToExportQueueController.populate_queue_from_stats()

            print(f"[{datetime.now()}] [Processor] Processamento concluído.")

        except Exception:
            print("[Erro] Exceção inesperada ao processar fila:")
            traceback.print_exc()
            sys.exit(2)


if __name__ == "__main__":
    run_job_file_ingestion_queue_processor.run()
