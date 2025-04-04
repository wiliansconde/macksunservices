from enum import Enum


class ProcessActions(Enum):
    FILE_ADDED_TO_QUEUE = "Arquivo adicionado à fila: {file_path}"
    SELECTED_FOR_PROCESSING = "Selecionado para processamento: {file_path}"
    FILE_READ = "Arquivo lido - Linhas encontradas: {total_lines}"
    DATA_INSERTION_STARTED = "Inserção de dados iniciada: {file_path}"
    LINES_INSERTED = "Linhas inseridas com sucesso: {inserted_lines}"
    DUPLICATE_LINES = "Linhas duplicadas: {duplicate_lines}"
    FAILED_LINES = "Linhas falharam: {failed_lines}"
    BATCH_SIZE = "Processando lote com: {batch_size} linha"
    PROCESSING_COMPLETED = "Processamento concluído: {file_path}"
    PROCESSING_ERROR = "Erro no processamento: {error_message}"
    UNZIPED_FILE = "Descompactando arquivo: {original_path} para o arquivo: {new_path}"
