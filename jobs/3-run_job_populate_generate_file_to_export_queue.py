import sys
import traceback
from datetime import datetime
from controllers.queue.ClsGenerateFileToExportQueueController import ClsGenerateFileToExportQueueController

"""
Job: 3-run_job_populate_generate_file_to_export_queue.py

Descrição:
    Popula a fila de geração de arquivos (Generate File Queue) com base nas estatísticas existentes (data_availability_stats).
    Esse processo identifica datas que possuem dados disponíveis mas que ainda não estão na fila de exportação.

Recomendação de uso:
    ➤ Pode ser agendado para execução periódica (ex: cron, task scheduler).
    ➤ Útil como tarefa de recuperação caso alguma execução anterior falhe.

Uso manual:
    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute com:
       python -m jobs.3-run_job_populate_generate_file_to_export_queue

Uso em cron (dentro de container):
    */60 * * * * root python /app/4-run_job_populate_generate_file_queue.py >> /var/log/cron.log 2>&1

Saída:
    Log informando a quantidade de entradas inseridas na fila.

Requisitos:
    - Python 3.7+
    - Executar a partir da raiz do projeto com `-m`
    - ClsGenerateFileQueueController acessível em controllers.queue
"""

class run_job_populate_generate_file_queue:
    @staticmethod
    def run():
        try:
            print(f"[{datetime.now()}] [GenerateQueueJob] Iniciando atualização da fila de geração de arquivos...")
            ClsGenerateFileToExportQueueController.populate_queue_from_stats()
            print(f"[{datetime.now()}] [GenerateQueueJob] Fila de geração de arquivos atualizada com sucesso.")

        except Exception:
            print("[Erro] Exceção inesperada ao popular a Generate File Queue:")
            traceback.print_exc()
            sys.exit(2)


if __name__ == "__main__":
    run_job_populate_generate_file_queue.run()
