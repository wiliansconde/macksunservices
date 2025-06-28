from datetime import datetime

from repositories.export_to_cloud.ClsProcessedFileTraceSyncRepository import ClsProcessedFileTraceSyncRepository


class ClsProcessedFileTraceSyncController:

    @staticmethod
    def sync_local_data_to_cloud_data(ignore_sync_flag=False, batch_limit=1000) -> int:
        """
        Sincroniza registros da coleção 'processed_file_trace' do ambiente local para a nuvem.

        :param ignore_sync_flag: Se True, envia todos os documentos, mesmo os já sincronizados.
        :param batch_limit: Número máximo de documentos a sincronizar por execução.
        :return: Total de documentos sincronizados.
        """
        synced_count = 0
        records = ClsProcessedFileTraceSyncRepository.find_unsynchronized_records(
            batch_limit,
            ignore_sync_flag
        )

        for record in records:
            now_str = datetime.utcnow().strftime("%H:%M:%S")

            print(f"[debug {now_str}] Enviando registro com _id: {record.get('_id')}")
            # Remove o _id e insere na nuvem
            ClsProcessedFileTraceSyncRepository.insert_into_cloud(record)

            # Marca como sincronizado no local
            ClsProcessedFileTraceSyncRepository.mark_as_synchronized(record["_id"])
            synced_count += 1

        return synced_count
