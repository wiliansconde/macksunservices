import os
import sys
import shutil
import traceback
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from controllers.queue.ClsFileQueueController import ClsFileQueueController


PROCESSED_DIR_NAME = "processed"


"""
**********************************************************************************
*******************  RODE ESSE ANTES DO PROCESS_QUEUE ****************************
**********************************************************************************

Job: run_job_read_local_files_and_insert_into_queue.py


Descrição:
    Varre recursivamente um diretório, identifica arquivos válidos conforme regras
    do instrumento informado, insere na fila via ClsFileQueueController e em seguida
    move o arquivo para uma pasta processed no mesmo nível em que o arquivo estava.

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

Pasta processed:
    Este job ignora qualquer pasta chamada processed durante a varredura.
    Após inserir um arquivo na fila com sucesso, ele é movido para processed
    no mesmo nível do arquivo.

    Atenção:
    O movimento para processed indica que o arquivo foi enfileirado com sucesso,
    não que ele já foi processado pelo consumidor.

Parâmetros:
    1 directory
       Diretório raiz a ser varrido recursivamente

    2 instrument_name
       Nome lógico do instrumento, exemplo SST POEMAS

    3 debug opcional
       0 ou 1
       Também pode ser controlado pela variável de ambiente INGESTOR_DEBUG

    4 reprocess_all opcional
       0 ou 1
       Quando 1, o job move todos os itens encontrados em pastas processed de volta
       para o diretório pai correspondente e em seguida executa a varredura completa
       novamente, reenfileirando tudo.
       Também pode ser controlado pela variável de ambiente INGESTOR_REPROCESS_ALL

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

    4 reprocess_all opcional
       Reenfileira tudo a partir de processed
       Valores aceitos 1 ou 0
       Se omitido, pode ser controlado pela variável de ambiente INGESTOR_REPROCESS_ALL

    SST com debug
    python jobs\\run_job_read_local_files_and_insert_into_queue.py "C:\\...\\SST\\2022" SST 1

    SST com debug e reprocess_all
    python jobs\\run_job_read_local_files_and_insert_into_queue.py "C:\\...\\SST\\2022" SST 1 1

    POEMAS sem debug e sem reprocess_all
    python jobs\\run_job_read_local_files_and_insert_into_queue.py "C:\\...\\POEMAS\\2020" POEMAS

Uso em cron:

    */30 * * * * root python /app/run_job_read_local_files_and_insert_into_queue.py "/data/SST" SST 0 0 >> /var/log/cron.log 2>&1
    */30 * * * * root python /app/run_job_read_local_files_and_insert_into_queue.py "/data/POEMAS" POEMAS 0 0 >> /var/log/cron.log 2>&1

Requisitos:
    Python 3.7 ou superior
    ClsFileQueueController acessível em controllers.queue
"""


@dataclass(frozen=True)
class InstrumentRule:
    name: str

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
        name="HALPHA_SAMPLE",
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
    def _unique_destination_path(dest_dir: str, base_name: str) -> str:
        dest_path = os.path.join(dest_dir, base_name)
        if not os.path.exists(dest_path):
            return dest_path

        name, ext = os.path.splitext(base_name)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        counter = 1
        while True:
            candidate = os.path.join(dest_dir, f"{name}_{ts}_{counter}{ext}")
            if not os.path.exists(candidate):
                return candidate
            counter += 1

    @staticmethod
    def move_to_processed(file_path: str, debug: bool = False) -> None:
        parent_dir = os.path.dirname(file_path)
        processed_dir = os.path.join(parent_dir, PROCESSED_DIR_NAME)
        os.makedirs(processed_dir, exist_ok=True)

        base_name = os.path.basename(file_path)
        dest_path = run_job_read_local_files_and_insert_into_queue._unique_destination_path(processed_dir, base_name)

        shutil.move(file_path, dest_path)

        run_job_read_local_files_and_insert_into_queue._debug_print(
            debug, f"[DEBUG] Movido para processed: {dest_path}"
        )

    @staticmethod
    def restore_from_processed(directory: str, debug: bool = False) -> int:
        moved_count = 0

        for root, dirs, _ in os.walk(directory, topdown=True):
            processed_dir_actual = None
            for d in dirs:
                if d.lower() == PROCESSED_DIR_NAME:
                    processed_dir_actual = d
                    break

            if processed_dir_actual is None:
                continue

            processed_path = os.path.join(root, processed_dir_actual)

            try:
                entries = os.listdir(processed_path)
            except Exception as e:
                print(f"[Erro] Falha ao listar {processed_path}: {e}")
                dirs[:] = [d for d in dirs if d.lower() != PROCESSED_DIR_NAME]
                continue

            for entry in entries:
                src = os.path.join(processed_path, entry)
                dest = run_job_read_local_files_and_insert_into_queue._unique_destination_path(root, entry)

                try:
                    shutil.move(src, dest)
                    moved_count += 1
                    run_job_read_local_files_and_insert_into_queue._debug_print(
                        debug, f"[DEBUG] Reprocess_all moveu: {src} para {dest}"
                    )
                except Exception as e:
                    print(f"[Erro] Falha ao mover {src} para {dest}: {e}")

            try:
                if not os.listdir(processed_path):
                    os.rmdir(processed_path)
                    run_job_read_local_files_and_insert_into_queue._debug_print(
                        debug, f"[DEBUG] Pasta removida por estar vazia: {processed_path}"
                    )
            except Exception:
                pass

            dirs[:] = [d for d in dirs if d.lower() != PROCESSED_DIR_NAME]

        return moved_count

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
    def run(directory: str, instrument_name: str, debug: bool = False, reprocess_all: bool = False) -> None:
        instrument_name_norm = instrument_name.strip().upper()
        print(
            f"[{datetime.now()}] [Ingestor] Iniciando varredura: {directory} "
            f"| Instrumento: {instrument_name_norm} | Debug: {int(debug)} | Reprocess_all: {int(reprocess_all)}"
        )

        if instrument_name_norm not in INSTRUMENT_RULES:
            print(f"[Erro] Instrumento invalido: {instrument_name_norm}")
            print(f"[Erro] Instrumentos validos: {', '.join(sorted(INSTRUMENT_RULES.keys()))}")
            return

        if not os.path.exists(directory):
            print(f"[Erro] Diretorio nao encontrado: {directory}")
            return

        if reprocess_all:
            print(f"[{datetime.now()}] [Ingestor] Reprocess_all ativo, restaurando conteudo de pastas processed")
            moved_back = run_job_read_local_files_and_insert_into_queue.restore_from_processed(directory, debug)
            print(f"[{datetime.now()}] [Ingestor] Reprocess_all moveu {moved_back} itens de volta para os diretorios pais")

        inserted_count = 0
        skipped_count = 0
        error_count = 0
        moved_to_processed_count = 0

        for root, dirs, files in os.walk(directory, topdown=True):
            dirs[:] = [d for d in dirs if d.lower() != PROCESSED_DIR_NAME]

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

                    run_job_read_local_files_and_insert_into_queue.move_to_processed(file_path, debug)
                    moved_to_processed_count += 1

                except Exception as e:
                    error_count += 1
                    print(f"[Erro] Falha ao inserir ou mover {file_path}: {e}")

        print(f"[{datetime.now()}] [Ingestor] Total inserido: {inserted_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total movido para processed: {moved_to_processed_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total ignorado: {skipped_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total com erro: {error_count} arquivos.\n")


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4, 5):
        print("Uso: python run_job_read_local_files_and_insert_into_queue.py <diretorio> <instrument_name> <debug opcional 0 ou 1> <reprocess_all opcional 0 ou 1>")
        sys.exit(1)

    directory = sys.argv[1]
    instrument_name = sys.argv[2]

    debug_env = os.getenv("INGESTOR_DEBUG", "0")
    reprocess_env = os.getenv("INGESTOR_REPROCESS_ALL", "0")

    debug_arg = sys.argv[3] if len(sys.argv) >= 4 else None
    reprocess_arg = sys.argv[4] if len(sys.argv) == 5 else None

    debug = (
        run_job_read_local_files_and_insert_into_queue._to_bool(debug_arg)
        or run_job_read_local_files_and_insert_into_queue._to_bool(debug_env)
    )

    reprocess_all = (
        run_job_read_local_files_and_insert_into_queue._to_bool(reprocess_arg)
        or run_job_read_local_files_and_insert_into_queue._to_bool(reprocess_env)
    )

    try:
        run_job_read_local_files_and_insert_into_queue.run(directory, instrument_name, debug, reprocess_all)
    except Exception:
        print("[Erro] Excecao inesperada:")
        traceback.print_exc()
        sys.exit(2)
