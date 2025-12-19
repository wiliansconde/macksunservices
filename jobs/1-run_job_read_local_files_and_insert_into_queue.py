import os
import sys
import traceback
from datetime import datetime
from controllers.queue.ClsFileQueueController import ClsFileQueueController
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


"""
**********************************************************************************
*******************  RODE ESSE ANTES DO PROCESS_QUEUE ****************************
**********************************************************************************

Job: 1-run_job_read_local_files_and_insert_into_queue.py


Descrição:
    Varre recursivamente um diretório, identifica arquivos válidos (bi/rs/rf/*.trk),
    e insere na fila de processamento via ClsFileQueueController.

Recomendação de uso:
    ➤ Cada diretório que deve ser varrido precisa ter uma chamada independente deste script.
    ➤ Ideal para configurar um agendamento (cron ou task scheduler) por **diretório monitorado**.

Uso manual:
    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader


       SST
       python -m jobs.1-run_job_read_local_files_and_insert_into_queue "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2022\"
    
    2. Execute com:
       POEMAS
       python -m jobs.1-run_job_read_local_files_and_insert_into_queue "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2020"
       
       SST
       python -m jobs.1-run_job_read_local_files_and_insert_into_queue "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2022\M10\M10"
       
       
       FAST
       python -m jobs.1-run_job_read_local_files_and_insert_into_queue "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\DadosSST\2016\M01\D01\fast"

       INTG
       python -m jobs.1-run_job_read_local_files_and_insert_into_queue "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\DadosSST\2016\M01\D01\intg"

       BI FILES
       python -m jobs.1-run_job_read_local_files_and_insert_into_queue "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\DadosSST\2016\M01\D01\instr"

    Main.read_local_files_and_insert_into_queue(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\')

       
       
       
       
Uso em cron (dentro de container):
    */30 * * * * root python /app/1-run_job_read_local_files_and_insert_into_queue.py "C:/w/y/POEMAS" >> /var/log/cron.log 2>&1
    */30 * * * * root python /app/1-run_job_read_local_files_and_insert_into_queue.py "C:/w/y/SST" >> /var/log/cron.log 2>&1

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
        try:
            file_name = os.path.basename(file_path).lower()

            # Print para ver qual arquivo está sendo testado (pode gerar muito log se tiver milhares de arquivos)
            # print(f"[DEBUG] Analisando: {file_name}")

            # 1. Verifica se o caminho existe e é um arquivo
            if not os.path.isfile(file_path):
                # print(f"[DEBUG] IGNORADO (Não é arquivo/Diretório): {file_name}")
                return False

            # 2. Regra de EXCLUSÃO explícita
            if file_name.endswith('.txt'):
                print(f"[DEBUG] IGNORADO (Extensão .txt): {file_name}")
                return False

            # 3. Regra de INCLUSÃO (Prefixos ou Sulfixo .trk)
            has_valid_pattern = (file_name.startswith(('bi', 'rs', 'rf')) or file_name.endswith('.trk'))

            if has_valid_pattern:
                print(f"[DEBUG] >>> ARQUIVO VÁLIDO: {file_name}")
                return True
            else:
                print(f"[DEBUG] IGNORADO (Padrão de nome inválido): {file_name}")
                return False

        except Exception as e:
            print(f"[ERRO] Erro ao validar {file_path}: {e}")
            return False

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
        print("Uso: python 1-run_job_read_local_files_and_insert_into_queue.py <diretório>")
        sys.exit(1)

    directory = sys.argv[1]

    try:
        run_job_read_local_files_and_insert_into_queue.run(directory)
    except Exception:
        print("[Erro] Exceção inesperada:")
        traceback.print_exc()
        sys.exit(2)
