import os
import traceback
from dataclasses import dataclass
from typing import Optional, Callable, Dict, List

from enums.ProcessStatus import ProcessStatus
from exceptions.GenericException import GenericException
from services.ClsLoggerService import ClsLoggerService
from models.queue.ClsFileQueueVO import ClsFileQueueVO
from services.ClsPoemasFileService import ClsPoemasFileService
from services.ClsRFandRSFileService import ClsRFandRSFileService
from services.ClsSSTBIFileService import ClsSSTBIFileService

from repositories.queue.ClsFileIngestionQueueRepository import ClsFileIngestionQueueRepository
from utils.ClsConsolePrint import CLSConsolePrint

from utils.ClsFormat import ClsFormat
from utils.ClsGet import ClsGet
from utils.ZipHelper import ZipHelper

@dataclass(frozen=True)
class ProcessorRule:
    prefixes: tuple = ()
    suffixes: tuple = ()
    extensions: tuple = ()
    handler: Callable[[str], int] = None

class ClsFileQueueService:
    # PROCESSING_RULES define o roteamento de processamento por instrumento.
    #
    # Objetivo:
    # Centralizar toda a configuracao de como identificar o tipo de arquivo e qual service
    # deve processar cada arquivo. Assim evitamos logica espalhada e evitamos alterar o
    # metodo central _process_file_by_telescope_type sempre que um novo instrumento ou
    # um novo subtipo de arquivo surgir.
    #
    # Como funciona:
    # 1 o job 1 grava INSTRUMENT_NAME junto com o FILEPATH na fila.
    # 2 o job 2 le o proximo item e chama _process_file_by_telescope_type passando
    #   file_name, full_path e instrument_name.
    # 3 _process_file_by_telescope_type busca as regras do instrumento em PROCESSING_RULES
    #   e testa cada ProcessorRule em ordem.
    # 4 a primeira regra que casar define o handler executado.
    #
    # Regras e prioridade:
    # A lista de ProcessorRule de cada instrumento e avaliada em ordem.
    # Por isso a ordem importa quando existem padroes sobrepostos.
    # Exemplo se um prefixo generico casar com varios casos, coloque as regras mais
    # especificas primeiro e as mais genericas depois.
    #
    # Cada ProcessorRule pode usar 3 tipos de criterio
    # prefixes: tupla de prefixos que o nome do arquivo pode iniciar
    # suffixes: tupla de sufixos que o nome do arquivo pode terminar
    # extensions: tupla de extensoes validas detectadas via splitext
    #
    # O handler deve ser uma funcao que recebe full_path e retorna a quantidade de linhas
    # processadas, mantendo o contrato atual do pipeline.
    #
    # Como adicionar um novo instrumento:
    # 1 crie ou reutilize um service com a funcao process_file(full_path) -> int
    # 2 adicione uma nova chave no dicionario com o nome do instrumento exatamente como
    #   sera gravado no banco, exemplo "HALPHA" ou "AR30T"
    # 3 inclua uma lista de ProcessorRule definindo como reconhecer cada tipo de arquivo
    # 4 nao altere _process_file_by_telescope_type. A unica mudanca necessaria deve ser
    #   aqui em PROCESSING_RULES.
    #
    # Exemplo de novo instrumento:
    # "HALPHA": [
    #     ProcessorRule(extensions=(".fits", ".fit"), handler=ClsHalphaFileService.process_file),
    # ]
    #
    # Exemplo de instrumento com multiplos tipos de arquivo:
    # "SST": [
    #     ProcessorRule(prefixes=("rf", "rs"), handler=ClsRFandRSFileService.process_file),
    #     ProcessorRule(prefixes=("bi",), handler=ClsSSTBIFileService.process_file),
    # ]
    PROCESSING_RULES: Dict[str, List[ProcessorRule]] = {
        "POEMAS": [
            ProcessorRule(
                suffixes=(".trk",),
                handler=ClsPoemasFileService.process_file,
            ),
        ],
        "SST": [
            ProcessorRule(
                prefixes=("rf", "rs"),
                handler=ClsRFandRSFileService.process_file,
            ),
            ProcessorRule(
                prefixes=("bi",),
                handler=ClsSSTBIFileService.process_file,
            ),
        ],
    }

    @staticmethod
    def insert(file_full_path: str, instrument_name: str):
        CLSConsolePrint.debug('iniciando insert')
        try:
            file_path = ClsFormat.format_file_path(file_full_path)
            extension = ""
            if file_full_path.lower().endswith(('.zip', '.gz')):
                extension = os.path.splitext(file_path)[1]
                file_path = os.path.splitext(file_path)[0]

            print('extension: ' + extension)
            print('file_path: ' + file_path)

            file_queue_vo = ClsFileQueueVO(
                file_path=file_path,
                file_full_path=file_full_path,
                zip_file_type=extension,
                file_size=0,
                file_lines_qty=0,
                status=ProcessStatus.PENDING,
                created_on=ClsGet.current_time(),
                created_by='system_initial_load',
                instrument_name=instrument_name
            )

            r = ClsFileIngestionQueueRepository.insert(file_queue_vo)
            if r:
                ClsLoggerService.write_initial_log_pending_status(file_path, 'system_initial_load')

            CLSConsolePrint.debug('fim insert')
        except Exception as e:
            error_message = str(e)
            error_trace = traceback.format_exc()
            ClsLoggerService.write_generic_error(file_full_path, error_message, error_trace)

    @staticmethod
    def process_next_file():
        CLSConsolePrint.debug('iniciando process_next_file')
        file_path = ""
        try:
            file_vo = ClsFileIngestionQueueRepository.get_next_pending_file()
            if not file_vo:
                print("No pending files in the queue.")
                return

            CLSConsolePrint.debug(f'Processando arquivo {file_path}')

            file_path = file_vo.FILEPATH
            ClsLoggerService.write_file_selected_for_processing(file_path)
            full_path = ClsFileQueueService._handle_compressed_file(file_vo)

            file_name = os.path.basename(file_path)
            file_size = ClsGet.get_file_size(full_path)

            ClsFileQueueService.update_file_size(file_path, file_size)
            file_lines_qty = ClsFileQueueService._process_file_by_telescope_type(file_name, full_path, instrument_name=file_vo.INSTRUMENT_NAME)

            ClsFileQueueService.update_file_lines_qty(file_path, file_lines_qty)
            ClsFileQueueService.update_file_status_completed(file_path)
            CLSConsolePrint.debug('fim process_next_file')
        except Exception as e:
            error_message = str(e)
            error_trace = traceback.format_exc()
            try:
                ClsFileQueueService.update_file_status_failed(file_path, error_message, error_trace)
            except Exception as e:
                error_message = str(e)
                error_trace = traceback.format_exc()
                ClsLoggerService.write_generic_error(file_path, error_message, error_trace)

    @staticmethod
    def _handle_compressed_file(file_vo: ClsFileQueueVO) -> str:
        if file_vo.FILE_FULL_PATH.endswith('.gz'):
            full_path = ZipHelper.decompress_gz(file_vo.FILE_FULL_PATH)
            original_file_name=f"{file_vo.FILEPATH}{file_vo.ZIP_FILE_TYPE}"
            ClsLoggerService.write_unziped_file(original_file_name, ClsFormat.format_file_path(full_path))
        else:
            full_path = file_vo.FILE_FULL_PATH
        return full_path

    @staticmethod
    def _process_file_by_telescope_type(
            file_name: str,
            full_path: str,
            instrument_name: Optional[str] = None,
    ) -> int:
        instrument_norm = (instrument_name or "").strip().upper()
        file_name_lc = (file_name or "").lower()

        rules = ClsFileQueueService.PROCESSING_RULES.get(instrument_norm)
        if rules:
            _, ext = os.path.splitext(file_name_lc)
            ext = ext.lower()

            for rule in rules:
                matched = False

                if rule.prefixes and file_name_lc.startswith(rule.prefixes):
                    matched = True

                if rule.suffixes and any(file_name_lc.endswith(sfx) for sfx in rule.suffixes):
                    matched = True

                if rule.extensions and ext in rule.extensions:
                    matched = True

                if matched and rule.handler:
                    return rule.handler(full_path)
            raise ValueError(f"Unsupported file type for instrument {instrument_norm}: {file_name}")
        raise ValueError("Unsupported file type")

    """@staticmethod
        def _process_file_by_telescope_type(file_name: str, full_path: str) -> int:
        file_name = file_name.lower()
        if file_name.startswith(('rf', 'rs')):
            return ClsRFandRSFileService.process_file(full_path)
        elif file_name.endswith('.trk'):
            return ClsPoemasFileService.process_file(full_path)
        elif file_name.startswith('bi'):
            return ClsSSTBIFileService.process_file(full_path)
        else:
            raise ValueError("Unsupported file type")"""

    @staticmethod
    def update_file_status_completed(file_path: str):
        ClsFileIngestionQueueRepository.update_file_status_completed(file_path)
        ClsLoggerService.write_complete_process_success(file_path)

    @staticmethod
    def update_file_reset_status_to_pending(file_path: str):
        ClsFileIngestionQueueRepository.update_file_status_pending(file_path)
        ClsLoggerService.write_file_reset_to_pending(file_path)


    @staticmethod
    def update_file_status_failed(file_path: str, error_message: str, error_trace: str):
        ClsFileIngestionQueueRepository.update_file_status_failed(file_path, error_message)
        ClsLoggerService.write_complete_process_failed(file_path, error_message, error_trace)

    @staticmethod
    def update_collection_name(file_path: str, collection_name: str):
        ClsFileIngestionQueueRepository.update_collection_name(file_path, collection_name)

    @staticmethod
    def update_file_size(file_path: str, file_size_mb):
        ClsFileIngestionQueueRepository.update_file_size(file_path, file_size_mb)
        ClsLoggerService.write_file_size(file_path, file_size_mb)

    @staticmethod
    def update_file_lines_qty(file_path: str, lines_qty):
        ClsFileIngestionQueueRepository.update_file_lines_qty(file_path, lines_qty)
        ClsLoggerService.write_file_lines_qty(file_path, lines_qty)

    @staticmethod
    def delete_incomplete_records_and_reset_queue_status():
        #todos os telescopios terão seus dados que estao na fila como in_processing
        #deletados de suas colecoes e atualizados na fila para PENDING

        #deleta das colecoes
        # Lista todos os arquivos com status "IN_PROCESSING"

        # Busca a lista de arquivos em processamento usando o método do repositório
        file_paths = ClsFileIngestionQueueRepository.get_files_in_processing()

        for file_path in file_paths:
            # Deleta os registros da coleção apropriada
            ClsPoemasFileService.delete_records(file_path)
            #volta o status da fila para pending
            ClsFileQueueService.update_file_reset_status_to_pending(file_path)

    @staticmethod
    def count_pending_files() -> int:
        return ClsFileIngestionQueueRepository.count_pending_files()
