import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from config.ClsSettings import ClsSettings

class ClsAzureBlobRepository:

    @staticmethod
    def upload_file(file_path: str, container_name: str, blob_path: str) -> str:
        """
        Faz upload de um arquivo local para o Azure Blob Storage.

        :param file_path: Caminho local do arquivo.
        :param container_name: Nome do container (ex: 'poemas', 'sst', etc).
        :param blob_path: Caminho do blob dentro do container (ex: '2025/06/19/arquivo.zip').
        :return: URL pública do blob.
        """

        try:
            connection_string = ClsSettings.AZURE_BLOB_CONNECTION_STRING
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)

            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

            content_type = ClsAzureBlobRepository._get_content_type(file_path)

            with open(file_path, "rb") as data:
                blob_client.upload_blob(
                    data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type=content_type)
                )

            public_url = f"{ClsSettings.AZURE_BLOB_BASE_URL}/{container_name}/{blob_path}"

            print(f"[AZURE] Upload concluído: {public_url}")
            return public_url

        except Exception as e:
            print(f"[AZURE] Erro ao fazer upload para o Azure Blob: {str(e)}")
            raise

    @staticmethod
    def _get_content_type(file_path: str) -> str:
        """
        Retorna o content-type HTTP adequado com base na extensão do arquivo.
        """
        if file_path.endswith(".zip"):
            return "application/zip"
        elif file_path.endswith(".fits"):
            return "application/fits"
        elif file_path.endswith(".csv"):
            return "text/csv"
        elif file_path.endswith(".json"):
            return "application/json"
        else:
            return "application/octet-stream"
