import os
import sys
import shutil
import traceback
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from controllers.queue.ClsFileQueueController import ClsFileQueueController


PROCESSED_DIR_NAME = "00_processed"
IGNORED_DIR_NAME = "00_ignored"


"""
**********************************************************************************
*******************  RODE ESSE ANTES DO PROCESS_QUEUE ****************************
**********************************************************************************

Job: run_job_read_local_files_and_insert_into_queue.py


Descrição:
    Varre recursivamente um diretório, identifica arquivos válidos conforme regras
    do instrumento informado, move o arquivo para uma pasta 00_processed no mesmo nível
    em que o arquivo estava e somente então insere na fila via ClsFileQueueController
    usando o path já dentro de 00_processed.

    Arquivos inválidos conforme a regra do instrumento são movidos para uma pasta
    00_ignored no mesmo nível em que o arquivo estava.

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

Pasta 00_processed:
    Este job ignora qualquer pasta chamada 00_processed durante a varredura.
    O arquivo válido é movido para 00_processed e então enfileirado usando o path final.

    Atenção:
    O movimento para 00_processed indica que o arquivo foi enfileirado com sucesso,
    não que ele já foi processado pelo consumidor.

    Consistência com a fila:
    O enqueue ocorre após o move. Em caso de falha no enqueue, o job tenta rollback
    movendo o arquivo de volta para o diretório original.

Pasta 00_ignored:
    Este job ignora qualquer pasta chamada 00_ignored durante a varredura.
    Arquivos que não atendem a regra do instrumento são movidos para 00_ignored,
    evitando nova varredura do mesmo item em execuções futuras.

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
       Quando 1, o job move todos os itens encontrados em pastas 00_processed e 00_ignored
       de volta para o diretório pai correspondente e em seguida executa a varredura
       completa novamente, reenfileirando e reclasificando tudo.
       Também pode ser controlado pela variável de ambiente INGESTOR_REPROCESS_ALL

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
    def _move_to_named_dir(file_path: str, target_dir_name: str, debug: bool = False) -> str:
        parent_dir = os.path.dirname(file_path)
        target_dir = os.path.join(parent_dir, target_dir_name)
        os.makedirs(target_dir, exist_ok=True)

        base_name = os.path.basename(file_path)
        dest_path = run_job_read_local_files_and_insert_into_queue._unique_destination_path(target_dir, base_name)

        shutil.move(file_path, dest_path)

        run_job_read_local_files_and_insert_into_queue._debug_print(
            debug, f"[DEBUG] Movido para {target_dir_name}: {dest_path}"
        )

        return dest_path

    @staticmethod
    def move_to_processed(file_path: str, debug: bool = False) -> str:
        return run_job_read_local_files_and_insert_into_queue._move_to_named_dir(
            file_path=file_path,
            target_dir_name=PROCESSED_DIR_NAME,
            debug=debug,
        )

    @staticmethod
    def move_to_ignored(file_path: str, debug: bool = False) -> str:
        return run_job_read_local_files_and_insert_into_queue._move_to_named_dir(
            file_path=file_path,
            target_dir_name=IGNORED_DIR_NAME,
            debug=debug,
        )

    @staticmethod
    def restore_from_named_dir(directory: str, target_dir_name: str, debug: bool = False) -> int:
        moved_count = 0
        target_dir_name_lc = target_dir_name.lower()

        for root, dirs, _ in os.walk(directory, topdown=True):
            target_dir_actual = None
            for d in dirs:
                if d.lower() == target_dir_name_lc:
                    target_dir_actual = d
                    break

            if target_dir_actual is None:
                continue

            target_path = os.path.join(root, target_dir_actual)

            try:
                entries = os.listdir(target_path)
            except Exception as e:
                print(f"[Erro] Falha ao listar {target_path}: {e}")
                dirs[:] = [d for d in dirs if d.lower() != target_dir_name_lc]
                continue

            for entry in entries:
                src = os.path.join(target_path, entry)
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
                if not os.listdir(target_path):
                    os.rmdir(target_path)
                    run_job_read_local_files_and_insert_into_queue._debug_print(
                        debug, f"[DEBUG] Pasta removida por estar vazia: {target_path}"
                    )
            except Exception:
                pass

            dirs[:] = [d for d in dirs if d.lower() != target_dir_name_lc]

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
            print(f"[{datetime.now()}] [Ingestor] Reprocess_all ativo, restaurando conteudo de pastas 00_processed e 00_ignored")
            moved_processed = run_job_read_local_files_and_insert_into_queue.restore_from_named_dir(
                directory=directory,
                target_dir_name=PROCESSED_DIR_NAME,
                debug=debug,
            )
            moved_ignored = run_job_read_local_files_and_insert_into_queue.restore_from_named_dir(
                directory=directory,
                target_dir_name=IGNORED_DIR_NAME,
                debug=debug,
            )
            print(f"[{datetime.now()}] [Ingestor] Reprocess_all moveu {moved_processed} itens de 00_processed")
            print(f"[{datetime.now()}] [Ingestor] Reprocess_all moveu {moved_ignored} itens de 00_ignored")

        inserted_count = 0
        invalid_count = 0
        error_count = 0
        moved_to_processed_count = 0
        moved_to_ignored_count = 0

        ignored_dir_lc = IGNORED_DIR_NAME.lower()
        processed_dir_lc = PROCESSED_DIR_NAME.lower()

        for root, dirs, files in os.walk(directory, topdown=True):
            dirs[:] = [d for d in dirs if d.lower() not in (processed_dir_lc, ignored_dir_lc)]

            for file in files:
                file_path = os.path.join(root, file)

                is_valid = run_job_read_local_files_and_insert_into_queue.is_valid_file(
                    file_path=file_path,
                    instrument_name=instrument_name_norm,
                    debug=debug,
                )

                if not is_valid:
                    invalid_count += 1
                    try:
                        run_job_read_local_files_and_insert_into_queue.move_to_ignored(file_path, debug)
                        moved_to_ignored_count += 1
                    except Exception as e:
                        error_count += 1
                        print(f"[Erro] Falha ao mover para 00_ignored {file_path}: {e}")
                    continue

                try:
                    original_parent_dir = os.path.dirname(file_path)
                    moved_path = run_job_read_local_files_and_insert_into_queue.move_to_processed(file_path, debug)

                    try:
                        ClsFileQueueController.insert(moved_path, instrument_name_norm)
                        inserted_count += 1
                        moved_to_processed_count += 1
                        print(f"[Ingestor] Inserido: {moved_path}")
                    except Exception as e:
                        base_name = os.path.basename(moved_path)
                        rollback_path = run_job_read_local_files_and_insert_into_queue._unique_destination_path(
                            original_parent_dir, base_name
                        )
                        try:
                            shutil.move(moved_path, rollback_path)
                            run_job_read_local_files_and_insert_into_queue._debug_print(
                                debug, f"[DEBUG] Rollback efetuado, arquivo voltou para: {rollback_path}"
                            )
                        except Exception as rollback_error:
                            run_job_read_local_files_and_insert_into_queue._debug_print(
                                debug, f"[DEBUG] Falha no rollback: {rollback_error}"
                            )
                        raise e

                except Exception as e:
                    error_count += 1
                    print(f"[Erro] Falha ao mover ou inserir {file_path}: {e}")

        print(f"[{datetime.now()}] [Ingestor] Total inserido: {inserted_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total movido para 00_processed: {moved_to_processed_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total invalidos: {invalid_count} arquivos.")
        print(f"[{datetime.now()}] [Ingestor] Total movido para 00_ignored: {moved_to_ignored_count} arquivos.")
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
