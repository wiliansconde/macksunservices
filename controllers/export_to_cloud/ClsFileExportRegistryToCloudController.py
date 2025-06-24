from datetime import datetime

from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from repositories.base_repositories.ClsAzureBlobHelper import ClsAzureBlobHelper
from repositories.export_to_cloud.ClsFileExportRegistryToCloudRepository import ClsFileExportRegistryToCloudRepository

class ClsFileExportRegistryToCloudController:

    @staticmethod
    def register_export(document: dict):
        """
        Registra a exportação de um arquivo para a nuvem.
        Faz um delete preventivo para evitar duplicações antes do insert.

        :param document: Dicionário contendo:
            - instrument
            - resolution
            - date
            - format
            - container_name
            - blob_path
            - public_url
        """


        ClsFileExportRegistryToCloudRepository.insert_export_record(document)


