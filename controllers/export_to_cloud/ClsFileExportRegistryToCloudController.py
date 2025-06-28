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

    @staticmethod
    def sync_local_data_to_cloud_data(limit=1000, ignore_synchronized_flag=False) -> int:
        """
        Sincroniza os documentos ainda não enviados (cloud_synchronized=False).
        Atualiza o campo cloud_synchronized=True após sucesso.

        :return: Quantidade de documentos sincronizados com sucesso.
        """
        unsynced_records = ClsFileExportRegistryToCloudRepository.find_unsynchronized_records(limit,ignore_synchronized_flag)
        if not unsynced_records:
            return 0

        synced_count = 0
        for record in unsynced_records:
            try:
                now_str = datetime.now().strftime('%H:%M:%S')
                print(f"[debug {now_str}] Enviando registro com _id: {record['_id']}")

                ClsFileExportRegistryToCloudRepository.insert_into_cloud(record)
                ClsFileExportRegistryToCloudRepository.mark_as_synchronized(record["_id"])
                synced_count += 1
            except Exception as e:
                print(f"[SyncController] Falha ao sincronizar registro ID {record['_id']}: {e}")

        return synced_count
