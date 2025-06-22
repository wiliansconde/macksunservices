import csv
import os
import json
from collections import defaultdict
from datetime import datetime
from astropy.io import fits
import csv
import os
from repositories.poemas.ClsPoemasFileRepository import ClsPoemasFileRepository


class ClsPoemasExportFileService:

    @staticmethod
    def discover_fits_structure(fits_file_path):
        with fits.open(fits_file_path) as hdul:

            for key, value in hdul[0].header.items():
                print(f"{key}: {value}")

            # Loop pelas extensões do HDU List (incluindo tabelas)
            for i, hdu in enumerate(hdul[1:], start=1):
                print("\n========================================")
                print(f"Table {i}:")

                # Header da tabela
                print("Header:")
                for key, value in hdu.header.items():
                    print(f"{key}: {value}")

                # Verifica se há colunas e as exibe
                if hasattr(hdu, 'columns') and hdu.columns:
                    print("Columns:")
                    for col in hdu.columns:
                        print(f"  {col.name}: format = {col.format}")

                    # Exibe as primeiras 5 linhas da tabela
                    print("\nTop 5 Rows:")
                    for row in hdu.data[:5]:
                        print(row)
                else:
                    print("This HDU is not a table.")
                print("========================================")



    import os
    @staticmethod
    def generate_fits_file( file_name:str, output_folder:str, records_to_generate_file: list):

        full_file_path = os.path.join(output_folder, f"{file_name}.fits")

        ClsPoemasExportFileService._create_fits_file(full_file_path, records_to_generate_file)

        print(f"[EXPORT] Arquivo FITS gerado: {full_file_path}")
        return full_file_path



    @staticmethod
    def _create_fits_file(fits_file_path: str, records_to_generate_file: list):
        """
        Cria um arquivo FITS a partir de uma lista de registros (JSONs), agrupando-os por data.
        """

        def _print_space_fits_doc_file():
            return " " * 3

        # Cabeçalho básico com os metadados principais
        header = fits.Header()
        header.add_blank()
        header.add_comment("")
        #header.add_comment("AAAAa")

        #header.add_comment("BBBBB")

        header['DATE'] = str(records_to_generate_file[0]['DATE'])
        header['YEAR'] = int(records_to_generate_file[0]['UTC_TIME_YEAR'])
        header['MONTH'] = int(records_to_generate_file[0]['UTC_TIME_MONTH'])
        header['DAY'] = int(records_to_generate_file[0]['UTC_TIME_DAY'])
        header['FREQ1'] = str(records_to_generate_file[0]['FREQ1'])
        header['FREQ2'] = str(records_to_generate_file[0]['FREQ2'])
        header['TELESCOP'] = "POlarization Emission of Millimeter Activity at the Sun (POEMAS)"
        header['INSTRUME'] = "POlarization Emission of Millimeter Activity at the Sun (POEMAS)"
        header['OBSRVTRY'] = "Complejo Astronomico El Leoncito (CASLEO)"
        header['OBS_ALTI'] = "2552 m"
        header['OBS_LONG'] = "-69.295583"
        header['OBS_LATI'] = "-31.798527"
        header['FILENAME'] = os.path.basename(fits_file_path)

        primary_hdu = fits.PrimaryHDU(header=header)

        # Geração da tabela de caminhos de arquivo distintos
        unique_filepaths = list(set(rec['FILEPATH'] for rec in records_to_generate_file))
        filepath_ids = range(len(unique_filepaths))  # Generate unique IDs
        col_filepath_id = fits.Column(name='FILEPATH_ID', format='I', array=list(filepath_ids))
        col_filepath = fits.Column(name='FILEPATHSOURCE', format='A100', array=unique_filepaths)
        filepath_hdu = fits.BinTableHDU.from_columns([col_filepath_id, col_filepath], name='FilePathTable')

        # Mapeia os caminhos de arquivo para IDs (ao invés de usar o caminho diretamente)
        filepath_id_map = {fp: idx for idx, fp in enumerate(unique_filepaths)}
        col_filepath_index = fits.Column(name='FILEPATH_IDX', format='I',
                                         array=[filepath_id_map[rec['FILEPATH']] for rec in records_to_generate_file])

        # Colunas para a tabela principal de dados
        """col_time = fits.Column(
            name='UTC_TIME',
            format='A10',  # 'A10' for a 10-character string to store 'YYYY-MM-DD'
            array=[rec['UTC_TIME'].strftime('%Y-%m-%d') for rec in records]  # Converte datetime para 'YYYY-MM-DD' format
        )"""

        col_tbmax = fits.Column(name='TBMAX', format='D', array=[float(rec['TBMAX']) for rec in records_to_generate_file])
        col_tbmin = fits.Column(name='TBMIN', format='D', array=[float(rec['TBMIN']) for rec in records_to_generate_file])
        col_nfreq = fits.Column(name='NFREQ', format='I', array=[int(rec['NFREQ']) for rec in records_to_generate_file])
        col_ele = fits.Column(name='ELE', format='D', array=[float(rec['ELE']) for rec in records_to_generate_file])
        col_azi = fits.Column(name='AZI', format='D', array=[float(rec['AZI']) for rec in records_to_generate_file])
        col_tbl45 = fits.Column(name='TBL45', format='D', array=[float(rec['TBL45']) for rec in records_to_generate_file])
        col_tbr45 = fits.Column(name='TBR45', format='D', array=[float(rec['TBR45']) for rec in records_to_generate_file])
        col_tbl90 = fits.Column(name='TBL90', format='D', array=[float(rec['TBL90']) for rec in records_to_generate_file])
        col_tbr90 = fits.Column(name='TBR90', format='D', array=[float(rec['TBR90']) for rec in records_to_generate_file])
        col_hh = fits.Column(name='UTC_TIME_HOUR', format='I', array=[int(rec['UTC_TIME_HOUR']) for rec in records_to_generate_file])
        col_mm = fits.Column(name='UTC_TIME_MINUTE', format='I', array=[int(rec['UTC_TIME_MINUTE']) for rec in records_to_generate_file])
        col_sec = fits.Column(name='UTC_TIME_SECOND', format='I', array=[int(rec['UTC_TIME_SECOND']) for rec in records_to_generate_file])
        col_ms = fits.Column(name='UTC_TIME_MILLISECOND', format='I', array=[int(rec['UTC_TIME_MILLISECOND']) for rec in records_to_generate_file])

        # Criação da tabela principal de dados com documentação adicional para cada coluna
        cols = fits.ColDefs([
            col_hh, col_mm, col_sec, col_ms, col_tbmax, col_tbmin, col_nfreq, col_ele, col_azi, col_tbl45, col_tbr45,
            col_tbl90, col_tbr90, col_filepath_index
        ])
        data_hdu = fits.BinTableHDU.from_columns(cols, name='DataTable')

        # Adiciona comentários para cada tabela e suas respectivas colunas
        primary_hdu.header.add_comment("Main Header: General metadata about the observation")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "DATE: Observation date in ISO format (YYYY-MM-DD)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "YEAR: Year of observation")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "MONTH: Month of observation")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "DAY: Day of observation")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "FREQ1: Primary frequency observed (in GHz)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "FREQ2: Secondary frequency observed (in GHz)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TELESCOP: Name of the telescope used for the observation")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "OBSRVTRY: Location of the observatory")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "OBS_LATI: Latitude of the observatory (in decimal degrees)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "OBS_LONG: Longitude of the observatory (in decimal degrees)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "OBS_ALTI: Altitude of the observatory above sea level (in meters)")

        # Documentação da Tabela 1 - Data Table
        primary_hdu.header.add_comment("")
        primary_hdu.header.add_comment("Table: Data Table - Contains detailed observation data records")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBMAX: Maximum brightness temperature (in Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBMIN: Minimum brightness temperature (in Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "NFREQ: Number of observed frequencies")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "ELE: Telescope elevation in degrees")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "AZI: Telescope azimuth in degrees")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBL45: Left polarization brightness temperature at 45 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBR45: Right polarization brightness temperature at 45 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBL90: Left polarization brightness temperature at 90 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBR90: Right polarization brightness temperature at 90 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "FILEPATH_IDX: References the file source in the FilePath Table")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "UTC_TIME_HOUR: Hour of observation in UTC")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "UTC_TIME_MINUTE: Minute of observation in UTC")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "UTC_TIME_SECOND: Second of observation in UTC")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "UTC_TIME_MILLISECOND: Millisecond of observation in UTC")

        # Documentação da Tabela 2 - FilePath Table
        primary_hdu.header.add_comment("")
        primary_hdu.header.add_comment("Table: FilePath - Contains unique file paths for each observation")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "FILEPATHSOURCE: Full file path for each observation.")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "Note: This table is indexed and referenced in the main data table.")

        # Grava o arquivo FITS com a tabela principal de dados e a tabela de caminhos de arquivo
        hdul = fits.HDUList([primary_hdu, data_hdu, filepath_hdu])
        hdul.writeto(fits_file_path, overwrite=True)

        print(f"FITS file created: {fits_file_path}")

    @staticmethod
    def generate_csv_file(file_name: str, output_folder:str, records_to_generate_file: list) -> str:
        """
        Gera um arquivo CSV a partir da lista de registros.

        :param file_name: Prefixo do nome do arquivo (sem extensão)
        :param records_to_generate_file: Lista de registros a exportar
        :return: Caminho completo do arquivo CSV gerado
        """



        full_file_path = os.path.join(output_folder, f"{file_name}.csv")
        ClsPoemasExportFileService._create_csv_file(full_file_path, records_to_generate_file)

        print(f"[EXPORT] Arquivo CSV gerado: {full_file_path}")
        return full_file_path

    @staticmethod
    def _create_csv_file(csv_file_path: str, records_to_generate_file: list):
        """
        Cria um arquivo CSV a partir de uma lista de registros (JSONs), incluindo um header descritivo como bloco de comentários no topo.

        :param csv_file_path: Caminho completo do arquivo CSV a ser gerado
        :param records_to_generate_file: Lista de registros
        """
        if not records_to_generate_file:
            print("[CSV] Nenhum registro disponível para exportação.")
            return

        fieldnames = [
            "UTC_TIME_HOUR",
            "UTC_TIME_MINUTE",
            "UTC_TIME_SECOND",
            "UTC_TIME_MILLISECOND",
            "TBMAX",
            "TBMIN",
            "NFREQ",
            "ELE",
            "AZI",
            "TBL45",
            "TBR45",
            "TBL90",
            "TBR90"
        ]

        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

        with open(csv_file_path, mode='w', newline='') as csvfile:
            # Header técnico como bloco de comentários
            csvfile.write("# Metadata Header\n")
            csvfile.write(f"# DATE: {records_to_generate_file[0].get('DATE', '')}\n")
            csvfile.write(f"# YEAR: {records_to_generate_file[0].get('UTC_TIME_YEAR', '')}\n")
            csvfile.write(f"# MONTH: {records_to_generate_file[0].get('UTC_TIME_MONTH', '')}\n")
            csvfile.write(f"# DAY: {records_to_generate_file[0].get('UTC_TIME_DAY', '')}\n")
            csvfile.write(f"# FREQ1: {records_to_generate_file[0].get('FREQ1', '')}\n")
            csvfile.write(f"# FREQ2: {records_to_generate_file[0].get('FREQ2', '')}\n")
            csvfile.write("# TELESCOP: POlarization Emission of Millimeter Activity at the Sun (POEMAS)\n")
            csvfile.write("# INSTRUME: POlarization Emission of Millimeter Activity at the Sun (POEMAS)\n")
            csvfile.write("# OBSRVTRY: Complejo Astronomico El Leoncito (CASLEO)\n")
            csvfile.write("# OBS_ALTI: 2552 m\n")
            csvfile.write("# OBS_LONG: -69.295583\n")
            csvfile.write("# OBS_LATI: -31.798527\n")
            csvfile.write(f"# FILENAME: {os.path.basename(csv_file_path)}\n")
            csvfile.write("#\n")

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for record in records_to_generate_file:
                writer.writerow({
                    "UTC_TIME_HOUR": int(record.get("UTC_TIME_HOUR", 0)),
                    "UTC_TIME_MINUTE": int(record.get("UTC_TIME_MINUTE", 0)),
                    "UTC_TIME_SECOND": int(record.get("UTC_TIME_SECOND", 0)),
                    "UTC_TIME_MILLISECOND": int(record.get("UTC_TIME_MILLISECOND", 0)),
                    "TBMAX": float(record.get("TBMAX", 0.0)),
                    "TBMIN": float(record.get("TBMIN", 0.0)),
                    "NFREQ": int(record.get("NFREQ", 0)),
                    "ELE": float(record.get("ELE", 0.0)),
                    "AZI": float(record.get("AZI", 0.0)),
                    "TBL45": float(record.get("TBL45", 0.0)),
                    "TBR45": float(record.get("TBR45", 0.0)),
                    "TBL90": float(record.get("TBL90", 0.0)),
                    "TBR90": float(record.get("TBR90", 0.0))
                })

        print(f"[CSV] CSV file created with header metadata: {csv_file_path}")

    @staticmethod
    def generate_json_file(file_name: str, output_folder:str,  records_to_generate_file: list) -> str:
        """
        Gera um arquivo JSON a partir da lista de registros.

        :param file_name: Prefixo do nome do arquivo (sem extensão)
        :param records_to_generate_file: Lista de registros a exportar
        :return: Caminho completo do arquivo JSON gerado
        """

        full_file_path = os.path.join(output_folder, f"{file_name}.json")
        ClsPoemasExportFileService._create_json_file(full_file_path, records_to_generate_file)

        print(f"[EXPORT] Arquivo JSON gerado: {full_file_path}")
        return full_file_path

    @staticmethod
    def _create_json_file(json_file_path: str, records_to_generate_file: list):
        """
        Cria um arquivo JSON contendo os metadados do header e a lista de dados.

        Estrutura final do JSON:
        {
            "HEADER": { ... },
            "DATA": [ { registro1 }, { registro2 }, ... ]
        }

        :param json_file_path: Caminho completo para o arquivo JSON de saída
        :param records_to_generate_file: Lista de registros (dicionários)
        """
        if not records_to_generate_file:
            print("[JSON] Nenhum registro disponível para exportação.")
            return

        # Monta o bloco de metadados (equivalente ao header do FITS e CSV)
        header = {
            "DATE": str(records_to_generate_file[0].get("DATE", "")),
            "YEAR": int(records_to_generate_file[0].get("UTC_TIME_YEAR", 0)),
            "MONTH": int(records_to_generate_file[0].get("UTC_TIME_MONTH", 0)),
            "DAY": int(records_to_generate_file[0].get("UTC_TIME_DAY", 0)),
            "FREQ1": str(records_to_generate_file[0].get("FREQ1", "")),
            "FREQ2": str(records_to_generate_file[0].get("FREQ2", "")),
            "TELESCOP": "POlarization Emission of Millimeter Activity at the Sun (POEMAS)",
            "INSTRUME": "POlarization Emission of Millimeter Activity at the Sun (POEMAS)",
            "OBSRVTRY": "Complejo Astronomico El Leoncito (CASLEO)",
            "OBS_ALTI": "2552 m",
            "OBS_LONG": "-69.295583",
            "OBS_LATI": "-31.798527",
            "FILENAME": os.path.basename(json_file_path)
        }

        # Define os campos a exportar (excluindo FILEPATH_IDX e FILEPATH)
        data_list = []
        for record in records_to_generate_file:
            data_list.append({
                "UTC_TIME_HOUR": int(record.get("UTC_TIME_HOUR", 0)),
                "UTC_TIME_MINUTE": int(record.get("UTC_TIME_MINUTE", 0)),
                "UTC_TIME_SECOND": int(record.get("UTC_TIME_SECOND", 0)),
                "UTC_TIME_MILLISECOND": int(record.get("UTC_TIME_MILLISECOND", 0)),
                "TBMAX": float(record.get("TBMAX", 0.0)),
                "TBMIN": float(record.get("TBMIN", 0.0)),
                "NFREQ": int(record.get("NFREQ", 0)),
                "ELE": float(record.get("ELE", 0.0)),
                "AZI": float(record.get("AZI", 0.0)),
                "TBL45": float(record.get("TBL45", 0.0)),
                "TBR45": float(record.get("TBR45", 0.0)),
                "TBL90": float(record.get("TBL90", 0.0)),
                "TBR90": float(record.get("TBR90", 0.0))
            })

        output_json = {
            "HEADER": header,
            "DATA": data_list
        }

        with open(json_file_path, mode='w', encoding='utf-8') as jsonfile:
            json.dump(output_json, jsonfile, indent=4)

        print(f"[JSON] JSON file created with metadata and data block: {json_file_path}")
