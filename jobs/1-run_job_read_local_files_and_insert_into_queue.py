
import os
import sys
import traceback
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from controllers.queue.ClsFileQueueController import ClsFileQueueController


"""
**********************************************************************************
*******************  RODE ESSE ANTES DO PROCESS_QUEUE ****************************
**********************************************************************************

Job: run_job_read_local_files_and_insert_into_queue.py


Descrição:
    Varre recursivamente um diretório, identifica arquivos válidos conforme regras
    do instrumento informado, e insere na fila de processamento via ClsFileQueueController.

Decisão de projeto:
    Este job NÃO tenta inferir o instrumento a partir do path ou do nome do arquivo.
    O instrumento deve ser informado explicitamente como parâmetro de execução.
    Recomenda se fortemente um agendamento dedicado por instrumento e por diretório.

Importante para integradores:
    As regras de inclusão devem ser sempre o critério principal.
    Regras de exclusão existem apenas como proteção adicional quando a inclusão
    é propositalmente ampla.

    Exemplo clássico:
    SST inclui arquivos que começam com bi rs rf.
    Nesse caso é possível existir bi.txt rs.txt rf.txt no diretório.
    A exclusão explícita de .txt evita que arquivos auxiliares ou de log
    sejam enfileirados incorretamente.

    Se a regra de inclusão for estrita por extensão, a exclusão pode ser redundante,
    mas é mantida aqui como mecanismo defensivo.

Parâmetros:
    1 directory
       Diretório raiz a ser varrido recursivamente

    2 instrument_name
       Nome lógico do instrumento, exemplo SST POEMAS FAST

    3 debug opcional
       0 ou 1
       Também pode ser controlado por variável de ambiente INGESTOR_DEBUG

Uso manual:

    Parâmetros do job:
    1 diretório
       Caminho raiz que será varrido recursivamente para descoberta de arquivos
    
    2 instrument_name
       Nome lógico do instrumento cujas regras de inclusão e exclusão serão aplicadas
    
    3 debug opcional
       Habilita logs detalhados
       Valores aceitos 1 ou 0
       Se omitido, pode ser controlado pela variável de ambiente INGESTOR_DEBUG

    SST
    python -m jobs.run_job_read_local_files_and_insert_into_queue "C:\...\SST\2022" SST 1

    POEMAS
    python -m jobs.run_job_read_local_files_and_insert_into_queue "C:\...\POEMAS\2020" POEMAS 1

Uso em cron:

    */30 * * * * root python /app/run_job_read_local_files_and_insert_into_queue.py "/data/SST" SST 0 >> /var/log/cron.log 2>&1
    */30 * * * * root python /app/run_job_read_local_files_and_insert_into_queue.py "/data/POEMAS" POEMAS 0 >> /var/log/cron.log 2>&1

Requisitos:
    Python 3.7 ou superior
    ClsFileQueueController acessível em controllers.queue
"""


@dataclass(frozen=True)
class InstrumentRule:
    name: str

    # Critérios de inclusão
    include_prefixes: Tuple[str, ...] = ()
    include_suffixes: Tuple[str, ...] = ()
    include_extensions: Tuple[str, ...] = ()

    # Critérios de exclusão
    # Estes critérios existem apenas como salvaguarda quando a inclusão é ampla.
    # Exemplo: incluir tudo que começa com "bi" pode capturar "bi.txt".
    # A exclusão explícita evita enfileirar arquivos auxiliares ou de log.
    exclude_extensions: Tuple[str, ...] = (".txt",)
    exclude_suffixes: Tuple[str, ...] = ()


INSTRUMENT_RULES: Dict[str, InstrumentRule] = {
    "SST": InstrumentRule(
        name="SST",
        include_prefixes=("bi", "rs", "rf"),
        exclude_extensions=(".txt",),
    ),
    "POEMAS": InstrumentRule(
        name="POEMAS",
        include_suffixes=(".trk",),
        exclude_extensions=(".txt",),
    ),
    "HALPHA_SAMPLE": InstrumentRule(
        name="CCCC",
        include_extensions=(".fits", ".fit"),
        exclude_extensions=(".txt",),
    ),
}


class run_job_read_local_files_and_insert_into_queue:
    @staticmethod
    def _to_bool(value: Optional[str]) -> bool:
        if value is None:
            return False
        return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

    @staticmethod
    def _debug_print(debug: bool, message: str) -> None:
        if debug:
            print(message)

    @staticmethod
    def is_valid_file(file_path: str, instrument_name: str, debug: bool = False) -> bool:
        try:
            if not os.path.isfile(file_path):
                return False

            instrument_name_norm = instrument_name.strip().upper()
            rule = INSTRUMENT_RULES.get(instrument_name_norm)

            if rule is None:
                print(f"[ERRO] Instrumento desconhecido: {instrument_name_norm}")
                return False

            file_name = os.path.basename(file_path).lower()
            _, ext = os.path.splitext(file_name)
            ext = ext.lower()

            run_job_read_local_files_and_insert_into_queue._debug_print(
                debug, f"[DEBUG] Analisando ({instrument_name_norm}): {file_name}"
            )

            if ext in rule.exclude_extensions:
                run_job_read_local_files_and_insert_into_queue._debug_print(
                    debug, f"[DEBUG] IGNORADO ({instrument_name_norm}, extensao excluida): {file_name}"
                )
                return False

            if rule.exclude_suffixes and any(file_name.endswith(sfx) for sfx in rule.exclude_suffixes):
                run_job_read_local_files_and_insert_into_queue._debug_print(
                    debug, f"[DEBUG] IGNORADO ({instrument_name_norm}, sufixo excluido): {file_name}"
                )
                return False

            matched = False

            if rule.include_prefixes and file_name.startswith(rule.include_prefixes):
                matched = True

            if rule.include_suffixes and any(file_name.endswith(sfx) for sfx in rule.include_suffixes):
                matched = True

            if rule.include_extensions and ext in rule.include_extensions:
                matched = True

            if matched:
                run_job_read_local_files_and_insert_into_queue._debug_print(
                    debug, f"[DEBUG] ARQUIVO VALIDO ({instrument_name_norm}): {file_name}"
                )
                return True

            run_job_read_local_files_and_insert_into_queue._debug_print(
                debug, f"[DEBUG] IGNORADO ({instrument_name_norm}, sem correspondencia): {file_name}"
            )
            return False

        except Exception as e:
            print(f"[ERRO] Erro ao validar {file_path}: {e}")
            return False

    @staticmethod
    def run(directory: str, instrument_name: str, debug: bool = False) -> None:
        instrument_name_norm = instrument_name.strip().upper()
        print(
            f"[{datetime.now()}] [Ingestor] Iniciando varredura: {directory} "
            f"| Instrumento: {instrument_name_norm} | Debug: {int(debug)}"
        )

        if instrument_name_norm not in INSTRUMENT_RULES:
            print(f"[Erro] Instrumento invalido: {instrument_name_norm}")
            print(f"[Erro] Instrumentos validos: {', '.join(sorted(INSTRUMENT_RULES.keys()))}")
            return

        if not os.path.exists(directory):
            print(f"[Erro] Diretorio nao encontrado: {directory}")
            return

        inserted_count = 0
        skipped_count = 0
        error_count = 0

        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)

                if not run_job_read_local_files_and_insert_into_queue.is_valid_file(
                    file_path=file_path,
                    instrument_name=instrument_name_norm,
                    debug=debug,
                ):
                    skipped_count += 1
                    continue

                try:
                    ClsFileQueueController.insert(file_path)
                    inserted_count += 1
                    print(f"[Ingestor] Inserido: {file_path}")
                except Exception as e:
                    error_count += 1
                    print(f"[Erro] Falha ao inserir {file_path}: {e}")

        print(f"[{datetime.now()}] [Ingestor] Total inserido: {inserted_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total ignorado: {skipped_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total com erro: {error_count} arquivos.\n")


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Uso: python run_job_read_local_files_and_insert_into_queue.py <diretorio> <instrument_name> <debug opcional 0 ou 1>")
        sys.exit(1)

    directory = sys.argv[1]
    instrument_name = sys.argv[2]

    debug_env = os.getenv("INGESTOR_DEBUG", "0")
    debug_arg = sys.argv[3] if len(sys.argv) == 4 else None

    debug = (
        run_job_read_local_files_and_insert_into_queue._to_bool(debug_arg)
        or run_job_read_local_files_and_insert_into_queue._to_bool(debug_env)
    )

    try:
        run_job_read_local_files_and_insert_into_queue.run(directory, instrument_name, debug)
    except Exception:
        print("[Erro] Excecao inesperada:")
        traceback.print_exc()
        sys.exit(2)
