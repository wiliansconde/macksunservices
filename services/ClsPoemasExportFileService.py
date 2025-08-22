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

        header['FILENAME'] = os.path.basename(fits_file_path)
        header['ORIGIN'] = "CRAAM/Universidade Presbiteriana Mackenzie"
        header['TELESCOP'] = "POlarization Emission of Millimeter Activity at the Sun (POEMAS)"
        header['INSTRUME'] = "POlarization Emission of Millimeter Activity at the Sun (POEMAS)"

        header['OBSERVAT'] = "CASLEO"
        header['STATION'] = "Lat = -31.79852700, Lon = -69.29558300, Height = 2.552 km"
        header['TZ'] = "GMT-3"

        first_time = records_to_generate_file[0]['UTC_TIME']
        last_time = records_to_generate_file[-1]['UTC_TIME']

        _isodate_ = first_time.strftime('%Y-%m-%d')
        _hhmmss_ = (
            first_time.strftime('%H:%M:%S'),
            last_time.strftime('%H:%M:%S')
        )
        header['DATE-OBS'] = _isodate_  # Ex: '2025-06-29'
        header['T_START'] = _isodate_ + "T" + _hhmmss_[0]  # Ex: '2025-06-29T12:00:00'
        header['T_END'] = _isodate_ + "T" + _hhmmss_[1]  # Ex: '2025-06-29T12:59:59'
        header['N_RECORD'] = len(records_to_generate_file)
        header['FREQUEN'] = "45GHz / 90GHz"

        header.add_comment("COPYRIGHT. Grant of use.")
        header.add_comment("These data are property of Universidade Presbiteriana Mackenzie.")
        header.add_comment("The Centro de Radio Astronomia e Astrofisica Mackenzie is reponsible")
        header.add_comment("for their distribution. Grant of use permission is given for Academic ")
        header.add_comment("purposes only.")

        header.add_comment("Main Header: General metadata about the SST observation export")

        header.add_comment(_print_space_fits_doc_file() + "FILENAME: Name of the generated FITS file")
        header.add_comment(_print_space_fits_doc_file() + "INSTRUME: Instrument name")
        header.add_comment(_print_space_fits_doc_file() + "TELESCOP: Telescope name")
        header.add_comment(_print_space_fits_doc_file() + "ORIGIN: Producing institution")
        header.add_comment(_print_space_fits_doc_file() + "OBSERVAT: Observatory site name")
        header.add_comment(_print_space_fits_doc_file() + "STATION: Latitude, longitude, and altitude")
        header.add_comment(_print_space_fits_doc_file() + "TZ: Time zone reference")
        header.add_comment(_print_space_fits_doc_file() + "DATE-OBS: Date of the observation (YYYY-MM-DD)")
        header.add_comment(_print_space_fits_doc_file() + "T_START / T_END: Observation start/end in ISO 8601 format")
        header.add_comment("N_RECORD: Number of data rows (time samples) in the binary table")
        header.add_comment(_print_space_fits_doc_file() + "FREQUEN: Frequency bands and channel mapping")

        primary_hdu = fits.PrimaryHDU(header=header)

        col_tbmax = fits.Column(name='TBMAX', format='D', array=[float(rec['TBMAX']) for rec in records_to_generate_file])
        col_tbmin = fits.Column(name='TBMIN', format='D', array=[float(rec['TBMIN']) for rec in records_to_generate_file])
        col_nfreq = fits.Column(name='NFREQ', format='I', array=[int(rec['NFREQ']) for rec in records_to_generate_file])
        col_ele = fits.Column(name='ELE', format='D', array=[float(rec['ELE']) for rec in records_to_generate_file])
        col_azi = fits.Column(name='AZI', format='D', array=[float(rec['AZI']) for rec in records_to_generate_file])
        col_tbl45 = fits.Column(name='TBL45', format='D', array=[float(rec['TBL45']) for rec in records_to_generate_file])
        col_tbr45 = fits.Column(name='TBR45', format='D', array=[float(rec['TBR45']) for rec in records_to_generate_file])
        col_tbl90 = fits.Column(name='TBL90', format='D', array=[float(rec['TBL90']) for rec in records_to_generate_file])
        col_tbr90 = fits.Column(name='TBR90', format='D', array=[float(rec['TBR90']) for rec in records_to_generate_file])
        #col_hh = fits.Column(name='UTC_TIME_HOUR', format='I', array=[int(rec['UTC_TIME_HOUR']) for rec in records_to_generate_file])
        #col_mm = fits.Column(name='UTC_TIME_MINUTE', format='I', array=[int(rec['UTC_TIME_MINUTE']) for rec in records_to_generate_file])
        #col_sec = fits.Column(name='UTC_TIME_SECOND', format='I', array=[int(rec['UTC_TIME_SECOND']) for rec in records_to_generate_file])
        #col_ms = fits.Column(name='UTC_TIME_MILLISECOND', format='I', array=[int(rec['UTC_TIME_MILLISECOND']) for rec in records_to_generate_file])
        col_iso_datetime = fits.Column(
            name='ISO_DATETIME',
            format='A23',
            array=[
                rec['UTC_TIME'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # Cortando para milissegundos
                for rec in records_to_generate_file
            ]
        )

        # Criação da tabela principal de dados com documentação adicional para cada coluna
        cols = fits.ColDefs([
            col_iso_datetime, col_tbmax, col_tbmin, col_nfreq, col_ele, col_azi, col_tbl45, col_tbr45,
            col_tbl90, col_tbr90
        ])
        data_hdu = fits.BinTableHDU.from_columns(cols, name='DataTable')


        # Documentação da Tabela 1 - Data Table
        primary_hdu.header.add_comment("")
        primary_hdu.header.add_comment("Table: Data Table - Contains detailed observation data records")
        primary_hdu.header.add_comment(
        _print_space_fits_doc_file() + "ISO_DATETIME: Date and time in ISO 8601 format with milliseconds (YYYY-MM-DDTHH:MM:SS.sss)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBMAX: Maximum brightness temperature (in Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBMIN: Minimum brightness temperature (in Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "NFREQ: Number of observed frequencies")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "ELE: Telescope elevation in degrees")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "AZI: Telescope azimuth in degrees")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBL45: Left polarization brightness temperature at 45 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBR45: Right polarization brightness temperature at 45 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBL90: Left polarization brightness temperature at 90 GHz (Kelvin)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "TBR90: Right polarization brightness temperature at 90 GHz (Kelvin)")


        # Grava o arquivo FITS com a tabela principal de dados e a tabela de caminhos de arquivo
        hdul = fits.HDUList([primary_hdu, data_hdu])
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

    import os
    import csv

    @staticmethod
    def _create_csv_file_from_poemas_fits(csv_file_path: str, records_to_generate_file: list):
        """
        Cria um arquivo CSV para dados do POEMAS, com metadados no topo e apenas os campos definidos no FITS.

        :param csv_file_path: Caminho completo do CSV de saída
        :param records_to_generate_file: Lista de registros (JSONs) a exportar
        """
        if not records_to_generate_file:
            print("[CSV] Nenhum registro disponível para exportação.")
            return

        fieldnames = [
            "ISO_DATETIME",
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
            # Header de metadados como comentários
            csvfile.write("# Metadata Header\n")

            first_time = records_to_generate_file[0]['UTC_TIME']
            last_time = records_to_generate_file[-1]['UTC_TIME']

            iso_date = first_time.strftime('%Y-%m-%d')
            t_start = first_time.strftime('%Y-%m-%dT%H:%M:%S')
            t_end = last_time.strftime('%Y-%m-%dT%H:%M:%S')

            csvfile.write("# ORIGIN: CRAAM/Universidade Presbiteriana Mackenzie\n")
            csvfile.write("# TELESCOP: POlarization Emission of Millimeter Activity at the Sun (POEMAS)\n")
            csvfile.write("# INSTRUME: POlarization Emission of Millimeter Activity at the Sun (POEMAS)\n")
            csvfile.write("# OBSERVAT: CASLEO\n")
            csvfile.write("# STATION: Lat = -31.79852700, Lon = -69.29558300, Height = 2.552 km\n")
            csvfile.write("# TZ: GMT-3\n")
            csvfile.write(f"# DATE-OBS: {iso_date}\n")
            csvfile.write(f"# T_START: {t_start}\n")
            csvfile.write(f"# T_END: {t_end}\n")
            csvfile.write(f"# N_RECORDS: {len(records_to_generate_file)}\n")
            csvfile.write("# FREQUEN: 45GHz / 90GHz\n")
            csvfile.write(f"# FILENAME: {os.path.basename(csv_file_path)}\n")
            csvfile.write("#\n")

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for record in records_to_generate_file:
                iso_datetime_str = (
                    record["UTC_TIME"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                    if "UTC_TIME" in record and record["UTC_TIME"] is not None
                    else ""
                )

                writer.writerow({
                    "ISO_DATETIME": iso_datetime_str,
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

        print(f"[CSV] Arquivo CSV do POEMAS criado: {csv_file_path}")

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
