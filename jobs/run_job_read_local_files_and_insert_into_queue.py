import os
import sys
import traceback
from datetime import datetime
from controllers.queue.ClsFileQueueController import ClsFileQueueController
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


"""
Job: run_job_read_local_files_and_insert_into_queue.py

Descrição:
    Varre recursivamente um diretório, identifica arquivos válidos (bi/rs/rf/*.trk),
    e insere na fila de processamento via ClsFileQueueController.

Recomendação de uso:
    ➤ Cada diretório que deve ser varrido precisa ter uma chamada independente deste script.
    ➤ Ideal para configurar um agendamento (cron ou task scheduler) por **diretório monitorado**.

Uso manual:
    python run_job_read_local_files_and_insert_into_queue.py "C:/w/y/POEMAS"
    python run_job_read_local_files_and_insert_into_queue.py "C:/w/y/SST"

    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute com:
       python -m jobs.run_job_read_local_files_and_insert_into_queue "C:/Y/WConde/Estudo/DoutoradoMack/Disciplinas/_PesquisaFinal/Dados/_FINAL/POEMAS/_Carga_teste/wilians-daniel/2012/M07/D09"

Uso em cron (dentro de container):
    */30 * * * * root python /app/run_job_read_local_files_and_insert_into_queue.py "C:/w/y/POEMAS" >> /var/log/cron.log 2>&1
    */30 * * * * root python /app/run_job_read_local_files_and_insert_into_queue.py "C:/w/y/SST" >> /var/log/cron.log 2>&1

Critérios de arquivo válido:
    - Começa com: bi, rs ou rf
    - OU termina com: .trk ou zip

Saída:
    Log com arquivos inseridos na fila e contagem final.

Requisitos:
    - Python 3.7+
    - ClsFileQueueController acessível em controllers.queue
"""


class run_job_read_local_files_and_insert_into_queue:
    @staticmethod
    def is_valid_file(file_path: str) -> bool:
        file_name = os.path.basename(file_path).lower()
        return (
            os.path.isfile(file_path)
            and (file_name.startswith(('bi', 'rs', 'rf')) or file_name.endswith('.trk'))
            and not file_name.endswith('.txt')
        )

    @staticmethod
    def run(directory: str) -> None:
        print(f"[{datetime.now()}] [Ingestor] Iniciando varredura: {directory}")

        if not os.path.exists(directory):
            print(f"[Erro] Diretório não encontrado: {directory}")
            return

        inserted_count = 0
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                if run_job_read_local_files_and_insert_into_queue.is_valid_file(file_path):
                    try:
                        ClsFileQueueController.insert(file_path)
                        inserted_count += 1
                        print(f"[Ingestor] Inserido: {file_path}")
                    except Exception as e:
                        print(f"[Erro] Falha ao inserir {file_path}: {e}")

        print(f"[{datetime.now()}] [Ingestor] Total inserido: {inserted_count} arquivos.\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python run_job_read_local_files_and_insert_into_queue.py <diretório>")
        sys.exit(1)

    directory = sys.argv[1]

    try:
        run_job_read_local_files_and_insert_into_queue.run(directory)
    except Exception:
        print("[Erro] Exceção inesperada:")
        traceback.print_exc()
        sys.exit(2)
