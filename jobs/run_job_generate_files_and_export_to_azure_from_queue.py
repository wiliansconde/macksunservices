import sys
import traceback
import os
from datetime import datetime
from controllers.partitioning.ClsPartition_map_controller import ClsPartitionMapController
from controllers.queue.ClsGenerateFileQueueController import ClsGenerateFileQueueController
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from repositories.base_repositories.ClsAzureBlobHelper import ClsAzureBlobHelper
from repositories.poemas.ClsPoemasFileRepository import ClsPoemasFileRepository
from services.ClsPoemasExportFileService import ClsPoemasExportFileService
from utils.ZipHelper import ZipHelper
import uuid
from datetime import datetime


class run_job_generate_file_export:


    @staticmethod
    def generate_filename_prefix(instrument: str, resolution: str, target_date: datetime) -> str:
        short_id = uuid.uuid4().hex[:8]  # Gera um hash curto de 7 caracteres
        date_str = target_date.strftime("%Y%m%d")
        return f"{instrument.lower()}_{resolution}_{date_str}_{short_id}"

    @staticmethod
    def run():
        print(f"[{datetime.now()}] [ExportJob] Iniciando geração de arquivos exportados a partir da fila...")

        while True:
            request = ClsGenerateFileQueueController.get_next_pending_request()
            if not request:
                print(f"[{datetime.now()}] [ExportJob] Nenhuma requisição pendente. Encerrando job.")
                break

            instrument_str = request["instrument"]
            resolution_str = request["resolution"]
            target_date = request["date"]
            request_id = request["_id"]
            # Converte para enums
            instrument_enum = ClsInstrumentEnum[instrument_str]
            resolution_enum = ClsResolutionEnum.from_value(resolution_str)

            blob_path = ClsAzureBlobHelper.build_blob_path(instrument_enum, resolution_enum, target_date, ".zip")
            container_name = instrument_str.lower()
            file_name_to_export = run_job_generate_file_export.generate_filename_prefix(instrument_str, resolution_str, target_date)

            try:

                controller = ClsPartitionMapController()
                mongo_collection = controller.get_target_collection(instrument_enum, resolution_enum, target_date)

                print(f"[ExportJob] Processando export para: Instrument={instrument_str}, Resolution={resolution_str}, Date={target_date}")

                if instrument_enum == ClsInstrumentEnum.POEMAS:
                    records_to_generate_files = ClsPoemasFileRepository.get_records_by_time_range(target_date, mongo_collection)

                    if not records_to_generate_files:
                        print(
                            f"[ExportJob] Nenhum dado encontrado para {target_date.date()} na coleção {mongo_collection}.")
                        ClsGenerateFileQueueController.update_status_failed(request_id, "No records found for export.")
                        continue

                    # Gera os arquivos
                    output_folder = os.path.join(os.getcwd(), "temp")
                    os.makedirs(output_folder, exist_ok=True)

                    fits_file_path = ClsPoemasExportFileService.generate_fits_file(file_name_to_export, output_folder, records_to_generate_files)
                    csv_file_path = ClsPoemasExportFileService.generate_csv_file(file_name_to_export, output_folder, records_to_generate_files)
                    #json_file_path = ClsPoemasExportFileService.generate_json_file(file_name_to_export, output_folder, records_to_generate_files)

                    # Compacta os dois em um único ZIP

                    zip_fits_file_path = ZipHelper.compress_single_file(fits_file_path)
                    zip_csv_file_path = ZipHelper.compress_single_file(csv_file_path)


                    # Upload para o Azure
                    ClsAzureBlobHelper.upload_file_to_blob(container_name, blob_path, zip_fits_file_path)
                    ClsAzureBlobHelper.upload_file_to_blob(container_name, blob_path, zip_csv_file_path)

                    # Limpeza
                    os.remove(fits_file_path)
                    os.remove(csv_file_path)
                    #os.remove(zip_file_path)

                    ClsGenerateFileQueueController.update_status_completed(request_id)

                else:
                    print(f"[ExportJob] Instrumento {instrument_str} ainda não suportado para exportação.")
                    ClsGenerateFileQueueController.update_status_failed(request_id, f"Instrument {instrument_str} not supported.")

            except Exception as e:
                print(f"[ExportJob] Erro ao processar exportação: {str(e)}")
                traceback.print_exc()
                ClsGenerateFileQueueController.update_status_failed(request_id, str(e))

if __name__ == "__main__":
    run_job_generate_file_export.run()
