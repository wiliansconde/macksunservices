import os
import time
from datetime import datetime

from pymongo import MongoClient

from controllers.partitioning.ClsPartition_map_controller import ClsPartitionMapController
from controllers.queue.ClsFileQueueController import ClsFileQueueController
from controllers.queue.ClsTimeSeriesExportQueueController import ClsTimeSeriesExportQueueController
from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from services.ClsPoemasExportFileService import ClsPoemasExportFileService
from utils.ClsConsolePrint import CLSConsolePrint
from utils.ClsFormat import ClsFormat
from utils.FileManager import FileManager
from utils.test import PoemasDataPlotter
from datetime import datetime
from datetime import datetime, timedelta

class Main:
    @staticmethod
    def initialize_process():
        CLSConsolePrint.debug('iniciando')

    @staticmethod
    def read_local_files_and_insert_into_queue(initial_directory):
        def is_valid_file(file_path):
            file_name = os.path.basename(file_path).lower()

            return (
                    os.path.isfile(file_path) and
                    (file_name.startswith(('bi', 'rs', 'rf')) or file_name.endswith('.trk'))
                    and not file_name.endswith('.txt')
                    )

        def process_directory(directory):

            user = 'system_initial_load'

            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if is_valid_file(file_path):
                        process_id = ClsFileQueueController.insert(str(file_path))
                        print(f"File {file_path} added to processing queue and logged.")

        process_directory(initial_directory)

    @staticmethod
    def process_queue(show_prompt=True):
        if show_prompt:
            totalLines=3000
            for i in range(1, totalLines):
                print("Sua chance de cancelar... Vai até 15" )
                for j in range(15):
                    print(j, end=' ', flush=True)
                    time.sleep(1)

            print(f'\n\n\nProcessando linha {i} de {totalLines}')
        ret = ClsFileQueueController.process_next_file()

    @staticmethod
    def delete_incomplete_records_and_reset_queue_status():
        ret = ClsFileQueueController.delete_incomplete_records_and_reset_queue_status()



    @staticmethod
    def create_fits_file_by_time_range(date_to_generate_file):

        # Criando uma instância da classe e chamando o método para converter JSONs em arquivos FITS
        fits_service = ClsPoemasExportFileService()
        fits_service.generate_fits_fits_file_by_time_range(date_to_generate_file)

    @staticmethod
    def create_json_file_from_fits_file():
        # Definindo o caminho da pasta de entrada com JSONs e a pasta de saída para os arquivos FITS
        fits_file_path = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\xxFits\2013-12-04.fits'  # Exemplo: "C:/dados/fits/2023-03-15.fits"
        output_json_folder = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\xxJsonFromFits'  # Exemplo: "C:/dados/jsons/"
        FileManager.delete_all_in_directory(output_json_folder)

        # Criando uma instância da classe e chamando o método para converter FITS em JSONs
        fits_service = ClsPoemasExportFileService(None, None)  # Pastas de entrada e saída para JSONs não são necessárias aqui
        fits_service.convert_fits_to_json_snapshot(fits_file_path, output_json_folder)

    @staticmethod
    def create_txt_file_from_fits_file():
        # Definindo o caminho da pasta de entrada com JSONs e a pasta de saída para os arquivos FITS
        fits_file_path = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\FITS_gerados\2024-07-27.fits'  # Exemplo: "C:/dados/fits/2023-03-15.fits"
        output_json_folder = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\FITS_gerados\2024-07-27.txt'  # Exemplo: "C:/dados/jsons/"

        # Criando uma instância da classe e chamando o método para converter FITS em JSONs
        fits_service = ClsPoemasExportFileService()  # Pastas de entrada e saída para JSONs não são necessárias aqui
        fits_service.export_fits_to_txt(fits_file_path, output_json_folder)

    @staticmethod
    def discovery_file_from_fits_file_structure():
        # Definindo o caminho da pasta de entrada com JSONs e a pasta de saída para os arquivos FITS
        fits_file_path = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\_Carga_teste\a.fits'  # Exemplo: "C:/dados/fits/2023-03-15.fits"
        ClsPoemasExportFileService.discover_fits_structure(fits_file_path)

    @staticmethod
    def renomear_pdfs_em_sequencia(pasta: str):
        arquivos = [f for f in os.listdir(pasta) if f.lower().endswith('.pdf')]
        arquivos.sort()  # ordena alfabeticamente (opcional)

        for i, nome_antigo in enumerate(arquivos, start=1):
            nome_novo = f"{i}_{nome_antigo}"
            caminho_antigo = os.path.join(pasta, nome_antigo)
            caminho_novo = os.path.join(pasta, nome_novo)

            if not os.path.exists(caminho_novo):  # evita sobrescrever
                os.rename(caminho_antigo, caminho_novo)
                print(f"Renomeado: {nome_antigo} → {nome_novo}")
            else:
                print(f"Já existe: {nome_novo}, pulando...")

    @staticmethod
    def contar_documentos():
        try:
            client = MongoClient("mongodb://localhost:27027")
            db = client["craam_data"]
            collection = db["data_POEMAS_file_10ms"]

            # Início e fim do intervalo
            start = datetime(2010, 12, 1)
            end = datetime(2025, 5, 1)
            delta = timedelta(days=1)

            print("Contagem de documentos a cada 10 dias (UTC_TIME):\n")

            current = start
            while current < end:
                next_ = current + delta
                try:
                    count = collection.count_documents({
                        "UTC_TIME": {
                            "$gte": current,
                            "$lt": next_
                        }
                    })

                    print(f"{current.strftime('%Y-%m-%d')} a {next_.strftime('%Y-%m-%d')}: {count}")
                except Exception as e:
                    print(f"{current.strftime('%Y-%m-%d')} a {next_.strftime('%Y-%m-%d')}: ERRO - {e}")
                current = next_

        except Exception as conn_error:
            print(f"Erro de conexão com MongoDB: {conn_error}")

    @staticmethod
    def test_get_target_collection():
        controller = ClsPartitionMapController()
        start_date = datetime(2011, 12, 1, 0, 0, 0)
        end_date = datetime(2011, 12, 1, 23, 59, 59)
#2011-12-01T10:19:46.000Z
        try:
            target_collection = controller.get_collections_for_range(ClsInstrumentEnum.POEMAS, ClsResolutionEnum.Seconds_01, start_date, end_date)
            print(f"[TESTE] Collection resolvida: {target_collection}")
        except Exception as e:
            print(f"[TESTE] Erro ao resolver collection: {e}")

    @staticmethod
    def test_TimeSeriesExportQueueController():
        ClsTimeSeriesExportQueueController.process_next_pending_request()

    #
    """
    POEMAS
    Main.read_local_files_and_insert_into_queue(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M11\D19'
            )

    ClsFileQueueController.insert(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M01\SunTrack_111130_173036.TRK')

    Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS1\2011\M01')

    FAST
    Main.read_local_files_and_insert_into_queue(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\fast')

    INTG
    Main.read_local_files_and_insert_into_queue(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\intg')

    # BI FILES
    Main.read_local_files_and_insert_into_queue(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\instr')

    # Funcionou para inserir na fila usando ClsFileQueueController:
    ClsFileQueueController.insert(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2024\2024\M01\D01\instr'
        r'\bi1240101'
    )

    ClsFileQueueController.insert(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\1999\M09\D07\instr'
        r'\bi990907.1458'
    )

    ClsFileQueueController.insert(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\M12\D22\instr'
        r'\bi1021222'
    )

    ClsFileQueueController.insert(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\M12\D02\instr'
        r'\bi1021202'
    )

    ClsFileQueueController.insert(
        r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\M10\D13\instr'
        r'\bi1021013'
    )
    """

if __name__ == "__main__":
    #Main.test_get_target_collection()
    Main.test_TimeSeriesExportQueueController()

    # Exemplo de uso:
    #file_path = r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\_Carga_teste\a.txt'
    #poemas =  PoemasDataPlotter(file_path)
    #poemas.plot_temperature_vs_time()

    #Main.contar_documentos()


    #Main.initialize_process()
    #Main.delete_incomplete_records_and_reset_queue_status()
    #Main.create_fits_file_by_json_directory()
    #Main.create_json_file_from_fits_file()
    #Main.discovery_file_from_fits_file_structure()

    #GERALLLLL
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST')


    #1
    #POEMAS
    #15M
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M11\D27')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M11\D28')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M11\D29')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M11\D30')

    #150M
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2024\M08')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2019\M10')


    #500
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2019\M10')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M12')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2012\M04')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2013\M06')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2019\M09')

    #1 B
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2012\M01')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2013\M02')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2019\M11')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2024\M05')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2024\M06')
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2012\M07')



    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M12\D06')
    
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\POEMAS\2011\M12\D06')
    
    """XXXX """

    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST')

    #SST - FAST
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\fast')

    #SST - INTG
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\intg')

    #SST - BI FILES
    #Main.read_local_files_and_insert_into_queue(r'C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\_FINAL\SST\2002\2002\M02\D18\instr')

    #2
    #Processr lote de 3k
    #Main.process_queue(False)

    #Loop por 15x para pegar da fila
    """ XX 
    for zz in range(5050):
         print()
         ClsFileQueueController.process_next_file()
    """
    #3

    # 4
    #date_to_generate_file = datetime(2024, 7, 24)
    #Main.create_fits_file_by_time_range(date_to_generate_file)

    #5 From FITS to TXT
    #Main.create_txt_file_from_fits_file()

    #6 From FITS to JSON
    #Main.create_fits_file_by_json_directory()
    #Main.create_json_file_from_fits_file()
    #


