import sys
import traceback
import os
import uuid
from datetime import datetime
from controllers.partitioning.ClsPartition_map_controller import ClsPartitionMapController
from controllers.queue.ClsGenerateFileToExportQueueController import ClsGenerateFileToExportQueueController
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from repositories.base_repositories.ClsAzureBlobHelper import ClsAzureBlobHelper
from repositories.export_to_cloud.ClsFileExportRegistryToCloudRepository import ClsFileExportRegistryToCloudRepository
from repositories.poemas.ClsPoemasFileRepository import ClsPoemasFileRepository
from repositories.sst.ClsRFandRSFileRepository import ClsRFandRSFileRepository
from services.ClsPoemasExportFileService import ClsPoemasExportFileService
from services.ClsRFandRSExportFileService import ClsRFandRSExportFileService
from utils.ZipHelper import ZipHelper

"""
Job: run_job_generate_file_export.py

Descrição:
    Processa requisições da fila 'queue_generate_file_to_export_to_cloud', gerando arquivos FITS e CSV a partir
    dos dados científicos exportáveis, compactando, realizando upload para o Azure Blob Storage e registrando os
    metadados do processo na coleção 'exported_files_to_cloud'.

Recomendação de uso:
    ➤ Esse job deve ser executado de forma periódica para garantir o fluxo contínuo de exportação e publicação de dados.
    ➤ Pode ser executado manualmente ou via agendamento com cron, preferencialmente após o preenchimento da fila.

Instrumentos suportados:
    - POEMAS
    - SST (modos FAST/INTG)

Uso manual:
    No command line:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute com:
       python -m jobs.run_job_generate_file_export

Uso em cron (dentro de container):
    */30 * * * * root python /app/jobs/run_job_generate_file_export.py >> /var/log/cron.log 2>&1

Saída:
    Log detalhado indicando a geração de arquivos, caminhos de destino, URLs públicas e status final de cada requisição.

Requisitos:
    - Python 3.7+
    - Executar a partir da raiz do projeto com `-m`
    - Dependências: ClsGenerateFileToExportQueueController, ClsPartitionMapController, ClsAzureBlobHelper, repositórios e serviços de exportação
"""

class run_job_generate_file_export:

    @staticmethod
    def generate_filename_prefix(instrument: str, resolution: str, target_date: datetime) -> str:
        short_id = uuid.uuid4().hex[:8]
        date_str = target_date.strftime("%Y%m%d")
        return f"{instrument.lower()}_{resolution}_{date_str}_{short_id}"

    @staticmethod
    def export_and_upload(single_file_path: str, container_name: str, blob_path: str, instrument_enum, resolution_enum,
                          target_date, format_name: str):
        """
        Faz a compressão, upload para o Azure Blob Storage e registra o export de um único arquivo.

        :param single_file_path: Caminho completo do arquivo a ser exportado
        :param container_name: Nome do container no Azure Blob
        :param blob_path: Caminho de destino no Blob Storage
        :param instrument_enum: Enum do instrumento
        :param resolution_enum: Enum da resolução
        :param target_date: Data alvo da exportação
        :param format_name: Nome do formato do arquivo (ex: 'fits', 'csv')
        """
        zip_file_path = ZipHelper.compress_single_file(single_file_path)
        public_url = ClsAzureBlobHelper.upload_file_to_blob(container_name, blob_path, zip_file_path)

        ClsFileExportRegistryToCloudRepository.insert_export_record({
            "instrument": instrument_enum.name,
            "resolution": resolution_enum.value,
            "date": target_date,
            "format": format_name,
            "container_name": container_name,
            "blob_path": blob_path,
            "public_url": public_url
        })

        os.remove(single_file_path)
        os.remove(zip_file_path)
        print(f"[EXPORT] Arquivo {single_file_path} exportado, compactado e registrado com sucesso.")

    @staticmethod
    def process_sst(target_date, mongo_collection, file_name, output_folder):
        records = ClsRFandRSFileRepository.get_records_by_time_range(target_date, mongo_collection)
        if not records:
            return None, "No SST records found."

        fits_path = ClsRFandRSExportFileService.generate_fits_file(file_name, output_folder, records)
        #csv_path = ClsPoemasExportFileService.generate_csv_file(file_name, output_folder, records)

        return {"fits": fits_path, "csv": "csv_path"}, None

    @staticmethod
    def run():
        print(f"[{datetime.now()}] [ExportJob] Iniciando geração de arquivos exportados a partir da fila...")

        while True:
            request = ClsGenerateFileToExportQueueController.get_next_pending_request()
            if not request:
                print(f"[{datetime.now()}] [ExportJob] Nenhuma requisição pendente. Encerrando job.")
                break

            try:
                instrument_str = request["instrument"]
                resolution_str = request["resolution"]
                target_date = request["date"]
                request_id = request["_id"]

                instrument_enum = ClsInstrumentEnum[instrument_str]
                resolution_enum = ClsResolutionEnum.from_value(resolution_str)

                file_name = run_job_generate_file_export.generate_filename_prefix(instrument_str, resolution_str, target_date)
                container_name = instrument_str.lower()
                blob_path_fits = ClsAzureBlobHelper.build_blob_path(instrument_enum, resolution_enum, target_date, "zip", "FITS")
                blob_path_csv = ClsAzureBlobHelper.build_blob_path(instrument_enum, resolution_enum, target_date,"zip", "CSV")
                output_folder = os.path.join(os.getcwd(), "temp")
                os.makedirs(output_folder, exist_ok=True)

                controller = ClsPartitionMapController()
                mongo_collection = controller.get_target_collection(instrument_enum, resolution_enum, target_date)

                print(f"[ExportJob] Processando export: Instrument={instrument_str}, Resolution={resolution_str}, Date={target_date}")


                if instrument_enum == ClsInstrumentEnum.POEMAS:
                    records = ClsPoemasFileRepository.get_records_by_time_range(target_date, mongo_collection)

                    fits_path = ClsPoemasExportFileService.generate_fits_file(file_name, output_folder, records)
                    #csv_path = ClsPoemasExportFileService.generate_csv_file(file_name, output_folder, records)

                    run_job_generate_file_export.export_and_upload(
                        fits_path,
                        container_name,
                        blob_path_fits,
                        instrument_enum,
                        resolution_enum,
                        target_date,
                        "FITS"
                    )
                    """run_job_generate_file_export.export_and_upload(
                        csv_path,
                        container_name,
                        blob_path_csv,
                        instrument_enum,
                        resolution_enum,
                        target_date,
                        "CSV"
                    )"""


                elif instrument_enum == ClsInstrumentEnum.SST:
                    records = ClsRFandRSFileRepository.get_records_by_time_range(target_date, mongo_collection)

                    fits_path = ClsRFandRSExportFileService.generate_fits_file(file_name, output_folder, records)
                    csv_path = ClsRFandRSExportFileService.generate_csv_file(file_name, output_folder, records)

                    run_job_generate_file_export.export_and_upload(
                        fits_path,
                        container_name,
                        blob_path_fits,
                        instrument_enum,
                        resolution_enum,
                        target_date,
                        "FITS"
                    )
                    run_job_generate_file_export.export_and_upload(
                        csv_path,
                        container_name,
                        blob_path_csv,
                        instrument_enum,
                        resolution_enum,
                        target_date,
                        "CSV"
                    )

                else:
                    print(f"[ExportJob] Instrumento {instrument_str} não suportado.")
                    error_message = f"Instrument {instrument_str} not supported."

                ClsGenerateFileToExportQueueController.update_status_completed(request_id)

            except Exception as e:
                print(f"[ExportJob] Erro ao processar exportação: {str(e)}")
                traceback.print_exc()
                ClsGenerateFileToExportQueueController.update_status_failed(request["_id"], str(e))


if __name__ == "__main__":
    run_job_generate_file_export.run()
