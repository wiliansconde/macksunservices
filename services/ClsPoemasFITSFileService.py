import os
import json
from collections import defaultdict
from datetime import datetime
from astropy.io import fits

from repositories.poemas.ClsPoemasFileRepository import ClsPoemasFileRepository


class ClsPoemasFITSFileService:

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

    def generate_fits_files(self, input_json_folder: str, output_fits_folder: str):
        """
        Recebe N arquivos JSON, cada um com um cabeçalho e uma linha, agrupando-os por data e gerando um arquivo FITS para cada grupo de data.
        """
        # Agrupa os arquivos JSON por data
        grouped_data = self._group_jsons_by_date()

        # Cria um arquivo FITS para cada grupo de data
        for date_str, records in grouped_data.items():
            fits_file_path = os.path.join(self.output_fits_folder, f"{date_str}.fits")
            self._create_fits_file(fits_file_path, records)

    def generate_fits_fits_file_by_time_range(self, date_to_generate_file):
        records = ClsPoemasFileRepository.get_records_by_time_range(date_to_generate_file)
        directory_path = r"C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Dados\FITS_gerados"
        formatted_date = date_to_generate_file.strftime("%Y-%m-%d")  # Formata para "ano-mes-dia"
        full_file_path = os.path.join(directory_path, f"{formatted_date}.fits")

        self._create_fits_file(full_file_path, records)

    def _group_jsons_by_date(self):
        """
        Agrupa os JSONs pela data (retirada do campo 'DATE' ou 'UTC_TIME').
        """
        grouped_data = defaultdict(list)

        for json_file in os.listdir(self.input_json_folder):
            json_path = os.path.join(self.input_json_folder, json_file)

            # Verifica se o arquivo é .json
            if json_file.endswith('.json'):
                with open(json_path, 'r') as f:
                    data = json.load(f)

                    # Pega a data (supondo que esteja no formato YYYY-MM-DD)
                    date_str = data.get('DATE', None)
                    if date_str:
                        grouped_data[date_str].append(data)

        return grouped_data

    def _create_fits_file(self, fits_file_path: str, records: list):
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

        header['DATE'] = str(records[0]['DATE'])
        header['YEAR'] = int(records[0]['UTC_TIME_YEAR'])
        header['MONTH'] = int(records[0]['UTC_TIME_MONTH'])
        header['DAY'] = int(records[0]['UTC_TIME_DAY'])
        header['FREQ1'] = str(records[0]['FREQ1'])
        header['FREQ2'] = str(records[0]['FREQ2'])
        header['TELESCOP'] = "POlarization Emission of Millimeter Activity at the Sun (POEMAS)"
        header['INSTRUME'] = "POlarization Emission of Millimeter Activity at the Sun (POEMAS)"
        header['OBSRVTRY'] = "Complejo Astronomico El Leoncito (CASLEO)"
        header['OBS_ALTI'] = "2552 m"
        header['OBS_LONG'] = "-69.295583"
        header['OBS_LATI'] = "-31.798527"
        header['FILENAME'] = os.path.basename(fits_file_path)

        primary_hdu = fits.PrimaryHDU(header=header)

        # Geração da tabela de caminhos de arquivo distintos
        unique_filepaths = list(set(rec['FILEPATH'] for rec in records))
        filepath_ids = range(len(unique_filepaths))  # Generate unique IDs
        col_filepath_id = fits.Column(name='FILEPATH_ID', format='I', array=list(filepath_ids))
        col_filepath = fits.Column(name='FILEPATHSOURCE', format='A100', array=unique_filepaths)
        filepath_hdu = fits.BinTableHDU.from_columns([col_filepath_id, col_filepath], name='FilePathTable')

        # Mapeia os caminhos de arquivo para IDs (ao invés de usar o caminho diretamente)
        filepath_id_map = {fp: idx for idx, fp in enumerate(unique_filepaths)}
        col_filepath_index = fits.Column(name='FILEPATH_IDX', format='I',
                                         array=[filepath_id_map[rec['FILEPATH']] for rec in records])

        # Colunas para a tabela principal de dados
        """col_time = fits.Column(
            name='UTC_TIME',
            format='A10',  # 'A10' for a 10-character string to store 'YYYY-MM-DD'
            array=[rec['UTC_TIME'].strftime('%Y-%m-%d') for rec in records]  # Converte datetime para 'YYYY-MM-DD' format
        )"""

        col_tbmax = fits.Column(name='TBMAX', format='D', array=[float(rec['TBMAX']) for rec in records])
        col_tbmin = fits.Column(name='TBMIN', format='D', array=[float(rec['TBMIN']) for rec in records])
        col_nfreq = fits.Column(name='NFREQ', format='I', array=[int(rec['NFREQ']) for rec in records])
        col_ele = fits.Column(name='ELE', format='D', array=[float(rec['ELE']) for rec in records])
        col_azi = fits.Column(name='AZI', format='D', array=[float(rec['AZI']) for rec in records])
        col_tbl45 = fits.Column(name='TBL45', format='D', array=[float(rec['TBL45']) for rec in records])
        col_tbr45 = fits.Column(name='TBR45', format='D', array=[float(rec['TBR45']) for rec in records])
        col_tbl90 = fits.Column(name='TBL90', format='D', array=[float(rec['TBL90']) for rec in records])
        col_tbr90 = fits.Column(name='TBR90', format='D', array=[float(rec['TBR90']) for rec in records])
        col_hh = fits.Column(name='UTC_TIME_HOUR', format='I', array=[int(rec['UTC_TIME_HOUR']) for rec in records])
        col_mm = fits.Column(name='UTC_TIME_MINUTE', format='I', array=[int(rec['UTC_TIME_MINUTE']) for rec in records])
        col_sec = fits.Column(name='UTC_TIME_SECOND', format='I', array=[int(rec['UTC_TIME_SECOND']) for rec in records])
        col_ms = fits.Column(name='UTC_TIME_MILLISECOND', format='I', array=[int(rec['UTC_TIME_MILLISECOND']) for rec in records])

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
    def convert_fits_to_json_snapshot(fits_file_path: str, output_json_folder: str):
        snapshot = {}

        with fits.open(fits_file_path) as hdul:
            # Captura o cabeçalho
            snapshot['HEADER'] = {key: hdul[0].header[key] for key in hdul[0].header.keys()}

            # Captura a tabela de dados principal
            data_hdu = hdul[1]  # Assumindo que a segunda extensão é a tabela de dados principal
            snapshot['TABLE_Data'] = []

            for record in data_hdu.data:
                # Armazena cada linha da tabela de dados como um dicionário
                data_record = {
                    "FILEPATHIDX": int(record['FILEPATH_IDX']),
                    "TBMAX": float(record['TBMAX']),
                    "TBMIN": float(record['TBMIN']),
                    "NFREQ": int(record['NFREQ']),
                    "ELE": float(record['ELE']),
                    "AZI": float(record['AZI']),
                    "TBL45": float(record['TBL45']),
                    "TBR45": float(record['TBR45']),
                    "TBL90": float(record['TBL90']),
                    "TBR90": float(record['TBR90']),
                    "UTC_TIME_YEAR": int(record['UTC_TIME_YEAR']),
                    "UTC_TIME_MONTH": int(record['UTC_TIME_MONTH']),
                    "UTC_TIME_DAY": int(record['UTC_TIME_DAY']),
                    "UTC_TIME_HOUR": int(record['UTC_TIME_HOUR']),
                    "UTC_TIME_MINUTE": int(record['UTC_TIME_MINUTE']),
                    "UTC_TIME_SECOND": int(record['UTC_TIME_SECOND']),
                    "UTC_TIME_MILLISECOND": int(record['UTC_TIME_MILLISECOND'])
                }
                snapshot['TABLE_Data'].append(data_record)

            # Captura a tabela de índices de caminhos de arquivos (assumindo que seja a terceira extensão)
            idx_hdu = hdul[2]
            snapshot['TABLE_IDX'] = []

            for idx_record in idx_hdu.data:
                snapshot['TABLE_IDX'].append({
                    "FILEPATHSOURCE": idx_record['FILEPATHSOURCE']
                })

        # Define o nome do arquivo JSON e salva a imagem do FITS
        json_filename = f"{os.path.basename(fits_file_path).replace('.fits', '')}_snapshot.json"
        json_path = os.path.join(output_json_folder, json_filename)

        with open(json_path, 'w') as json_file:
            json.dump(snapshot, json_file, indent=4)

        print(f"JSON file created with snapshot of FITS data: {json_path}")

    @staticmethod
    def export_fits_to_txt(fits_file_path, output_txt_path):
        """
        This method reads a FITS file, prints the primary header, and prints the
        FilePath and Data tables in a horizontal tabular format into a text file.
        """
        with fits.open(fits_file_path) as hdul:
            print(hdul.info())
            print()
            print(hdul[0].header)

            with open(output_txt_path, 'w') as txt_file:
                # Access and print the primary HDU header information
                primary_hdu = hdul[0]
                txt_file.write("Primary HDU Header:\n")
                for key, value in primary_hdu.header.items():
                    txt_file.write(f"{key}: {value}\n")
                txt_file.write("\n")

                # Print the FilePathTable
                filepath_hdu = hdul[1]
                txt_file.write("FilePath Table:\n")
                column_names = filepath_hdu.columns.names
                txt_file.write(" | ".join(column_names) + "\n")  # Print the header

                for row in filepath_hdu.data:
                    row_data = [str(row[col]) for col in column_names]
                    txt_file.write(" | ".join(row_data) + "\n")
                txt_file.write("\n")

                # Print the DataTable
                data_hdu = hdul[2]
                txt_file.write("Data Table:\n")
                column_names = data_hdu.columns.names
                txt_file.write(" | ".join(column_names) + "\n")  # Print the header

                for row in data_hdu.data:
                    row_data = [str(row[col]) for col in column_names]
                    txt_file.write(" | ".join(row_data) + "\n")
                txt_file.write("\n")

        print(f"TXT file generated: {output_txt_path}")
