from datetime import datetime

from azure.storage.blob import BlobServiceClient, ContentSettings
from config.ClsSettings import ClsSettings
import os

from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum


class ClsAzureBlobHelper:

    @staticmethod
    def upload_file_to_blob(container_name: str, blob_path: str, local_file_path: str, content_type: str = 'application/zip') -> str:
        """
        Faz upload de um arquivo local para o Azure Blob Storage.

        :param container_name: Nome do container (ex: 'poemas', 'sst')
        :param blob_path: Caminho dentro do container, ex: '2025/06/arquivo.zip'
        :param local_file_path: Caminho completo do arquivo local
        :param content_type: Tipo MIME (default: application/zip)
        :return: URL pública do blob
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(ClsSettings.AZURE_BLOB_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(container_name)

            # Garante que o container exista (se desejar, pode remover se os containers já estiverem prontos)
            try:
                container_client.create_container(public_access='blob')
            except Exception:
                pass  # Container já existe

            blob_client = container_client.get_blob_client(blob_path)

            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(
                    data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type=content_type)
                )

            public_url = f"{ClsSettings.AZURE_BLOB_BASE_URL}/{container_name}/{blob_path}"
            print(f"[AZURE] Upload concluído: {public_url}")

            return public_url

        except Exception as e:
            print(f"[AZURE] Erro ao fazer upload para Blob: {str(e)}")
            raise

    @staticmethod
    def build_blob_path(instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, target_date: datetime,
                        file_extension: str, file_type: str) -> str:
        """
        Gera o caminho padronizado para salvar o arquivo no Azure Blob, no formato:
        yyyy/mm/dd/instrument_resolution_yyyy-mm-dd.ext

        Exemplo de saída:
        2025/06/27/poemas_10ms_2025-06-27_CSV.zip
        """
        instr=instrument.value.lower()
        res=resolution.value
        folder = f"{target_date.year}/{str(target_date.month).zfill(2)}/{str(target_date.day).zfill(2)}"
        filename = f"{instr}_{res}_{target_date.date()}_{file_type.lower()}.{file_extension}"
        return f"{folder}/{filename}"
