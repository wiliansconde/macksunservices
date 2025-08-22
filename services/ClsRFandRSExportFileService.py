
import json
from astropy.io import fits
import csv
import os


class ClsRFandRSExportFileService:

    @staticmethod
    def generate_fits_file(file_name: str, output_folder: str, records_to_generate_file: list):

        full_file_path = os.path.join(output_folder, f"{file_name}.fits")

        ClsRFandRSExportFileService._create_fits_file(full_file_path, records_to_generate_file)

        print(f"[EXPORT] Arquivo FITS gerado: {full_file_path}")
        return full_file_path

    @staticmethod
    def _create_fits_file(fits_file_path: str, records_to_generate_file: list):
        """
        Cria um arquivo FITS a partir de uma lista de registros do SST (INTG/FAST/RF format).
        """

        def _print_space_fits_doc_file():
            return " " * 3

        # =============================
        # Cabeçalho Primário - Metadados
        # =============================
        header = fits.Header()
        header.add_blank()
        header.add_comment("")

        # ============================================
        # Extração de tempo para DATE-OBS, T_START, T_END
        # ============================================
        first_time = records_to_generate_file[0]['UTC_TIME']
        last_time = records_to_generate_file[-1]['UTC_TIME']

        _isodate_ = first_time.strftime('%Y-%m-%d')
        _hhmmss_ = (
            first_time.strftime('%H:%M:%S'),
            last_time.strftime('%H:%M:%S')
        )

        # ============================================
        # Cabeçalho FITS primário (header)
        # ============================================
        header = fits.Header()

        # Comentário e separador inicial
        header.add_blank()
        header.add_comment("")

        header['FILENAME'] = os.path.basename(fits_file_path)
        header['ORIGIN'] = "CRAAM/Universidade Presbiteriana Mackenzie"
        header['TELESCOP'] = "Solar Submillimeter Telescope"
        header['INSTRUME'] = "Solar Submillimeter Telescope"

        header['OBSERVAT'] = "CASLEO"
        header['STATION'] = "Lat = -31.79852700, Lon = -69.29558300, Height = 2.552 km"
        header['TZ'] = "GMT-3"

        header['DATE-OBS'] = _isodate_  # Ex: '2025-06-29'
        header['T_START'] = _isodate_ + "T" + _hhmmss_[0]  # Ex: '2025-06-29T12:00:00'
        header['T_END'] = _isodate_ + "T" + _hhmmss_[1]  # Ex: '2025-06-29T12:59:59'
        header['N_RECORD'] = len(records_to_generate_file)

        header['DATA_TYP'] = records_to_generate_file[0]['SSTType']  # metadata["SSTType"]                # Ex: 'FAST' ou 'INTG'
        header['ORIGFILE'] = records_to_generate_file[0]["FILEPATH"]  # metadata["RBDFileName"]            # Nome do binário original
        header['FREQUEN'] = "212 GHz ch=1,2,3,4; 405 GHz ch=5,6"

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
        header.add_comment(_print_space_fits_doc_file() + "DATA_TYP: SST data format (e.g., FAST, INTG)")
        header.add_comment(_print_space_fits_doc_file() + "ORIGFILE: Source binary file")
        header.add_comment(_print_space_fits_doc_file() + "FREQUEN: Frequency bands and channel mapping")


        primary_hdu = fits.PrimaryHDU(header=header)

        # =============================
        # Tabela de Dados Binária
        # =============================
        cols = []

        col_iso_datetime = fits.Column(
            name='ISO_DATETIME',
            format='A23',
            array=[
                rec['UTC_TIME'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # Cortando para milissegundos
                for rec in records_to_generate_file
            ]
        )
        cols.append(col_iso_datetime)

        for i in range(1, 7):
            cols.append(
                fits.Column(name=f'ADCVAL_{i}', format='I', array=[r[f'ADCVAL_{i}'] for r in records_to_generate_file]))

        cols.append(fits.Column(name='POS_TIME', format='J', array=[r['POS_TIME'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='AZIPOS', format='J', array=[r['AZIPOS'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='ELEPOS', format='J', array=[r['ELEPOS'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='PM_DAZ', format='I', array=[r['PM_DAZ'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='PM_DEL', format='I', array=[r['PM_DEL'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='AZIERR', format='J', array=[r['AZIERR'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='ELEERR', format='J', array=[r['ELEERR'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='X_OFF', format='I', array=[r['X_OFF'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='Y_OFF', format='I', array=[r['Y_OFF'] for r in records_to_generate_file]))


        for i in range(1, 7):
            cols.append(
                fits.Column(name=f'OFF_{i}', format='I', array=[r[f'OFF_{i}'] for r in records_to_generate_file]))


        cols.append(fits.Column(name='TARGET', format='B', array=[r['TARGET'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='OPMODE', format='B', array=[r['OPMODE'] for r in records_to_generate_file]))
        cols.append(
            fits.Column(name='GPS_STATUS', format='I', array=[r['GPS_STATUS'] for r in records_to_generate_file]))
        cols.append(fits.Column(name='RECNUM', format='J', array=[r['RECNUM'] for r in records_to_generate_file]))

        data_hdu = fits.BinTableHDU.from_columns(cols, name='DataTable')

        # =============================
        # Documentação das Colunas
        # =============================
        primary_hdu.header.add_comment("")
        primary_hdu.header.add_comment("Table: DataTable - Contains SST observation records")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "ISO_DATETIME: Date and time in ISO 8601 format with milliseconds (YYYY-MM-DDTHH:MM:SS.sss)")
        primary_hdu.header.add_comment(
            _print_space_fits_doc_file() + "ADCVAL_1 to ADCVAL_6: ADC channel values (uint16)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "POS_TIME: Position timestamp (int32)")
        primary_hdu.header.add_comment(
            _print_space_fits_doc_file() + "AZIPOS / ELEPOS: Positioner coordinates (.001 deg)")
        primary_hdu.header.add_comment(
            _print_space_fits_doc_file() + "PM_DAZ / PM_DEL: Pointing model corrections (int16)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "AZIERR / ELEERR: Pointing errors (.001 deg)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "X_OFF / Y_OFF: Offset to target center (int16)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "OFF_1 to OFF_6: Attenuator settings (int16)")
        primary_hdu.header.add_comment(
            _print_space_fits_doc_file() + "TARGET: Observed target / mirror position (int8)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "OPMODE: Operation mode (int8)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "GPS_STATUS: GPS status (int16)")
        primary_hdu.header.add_comment(_print_space_fits_doc_file() + "RECNUM: Record number (int32)")

        # =============================
        # Escrita Final do FITS
        # =============================
        hdul = fits.HDUList([primary_hdu, data_hdu])
        hdul.writeto(fits_file_path, overwrite=True)

        print(f"FITS file created: {fits_file_path}")

    @staticmethod
    def generate_csv_file(file_name: str, output_folder: str, records_to_generate_file: list) -> str:
        """
        Gera um arquivo CSV a partir da lista de registros SST (RF/RS).

        :param file_name: Prefixo do nome do arquivo (sem extensão)
        :param output_folder: Pasta de saída
        :param records_to_generate_file: Lista de registros
        :return: Caminho completo do CSV gerado
        """
        full_file_path = os.path.join(output_folder, f"{file_name}.csv")
        ClsRFandRSExportFileService._create_csv_file(full_file_path, records_to_generate_file)
        print(f"[EXPORT] Arquivo CSV gerado: {full_file_path}")
        return full_file_path

    @staticmethod
    def _create_csv_file(csv_file_path: str, records_to_generate_file: list):
        """
        Cria um arquivo CSV contendo os mesmos campos da tabela de dados do arquivo FITS.
        As primeiras linhas do CSV incluem metadados de cabeçalho e documentação.
        """

        def _print_space_csv_doc():
            return " " * 3

        # =============================
        # Extração de tempo e metadados
        # =============================
        first_time = records_to_generate_file[0]['UTC_TIME']
        last_time = records_to_generate_file[-1]['UTC_TIME']

        _isodate_ = first_time.strftime('%Y-%m-%d')
        _hhmmss_ = (
            first_time.strftime('%H:%M:%S'),
            last_time.strftime('%H:%M:%S')
        )

        origin = "CRAAM/Universidade Presbiteriana Mackenzie"
        telescope = "Solar Submillimeter Telescope"
        instrument = "Solar Submillimeter Telescope"
        observatory = "CASLEO"
        station = "Lat = -31.79852700, Lon = -69.29558300, Height = 2.552 km"
        timezone = "GMT-3"
        data_type = records_to_generate_file[0]["SSTType"]
        original_file = records_to_generate_file[0]["FILEPATH"]
        frequen = "212 GHz ch=1,2,3,4 / 405 GHz ch=5,6"
        n_records = len(records_to_generate_file)

        # =============================
        # Campos (iguais ao FITS)
        # =============================
        fieldnames = [
            "ISO_DATETIME",
            "ADCVAL_1", "ADCVAL_2", "ADCVAL_3", "ADCVAL_4", "ADCVAL_5", "ADCVAL_6",
            "POS_TIME",
            "AZIPOS", "ELEPOS",
            "PM_DAZ", "PM_DEL",
            "AZIERR", "ELEERR",
            "X_OFF", "Y_OFF",
            "OFF_1", "OFF_2", "OFF_3", "OFF_4", "OFF_5", "OFF_6",
            "TARGET", "OPMODE", "GPS_STATUS", "RECNUM"
        ]

        with open(csv_file_path, mode="w", newline="") as csv_file:
            writer = csv.writer(csv_file)

            # =============================
            # Header técnico e documentação
            # =============================
            writer.writerow(["# CRAAM/Universidade Presbiteriana Mackenzie - SST Data Export"])
            writer.writerow(["# Main Header: General metadata about the SST observation export"])
            writer.writerow(["# ORIGIN:", origin])
            writer.writerow(["# TELESCOP:", telescope])
            writer.writerow(["# INSTRUME:", instrument])
            writer.writerow(["# OBSERVAT:", observatory])
            writer.writerow(["# STATION:", station])
            writer.writerow(["# TZ:", timezone])
            writer.writerow(["# DATE-OBS:", _isodate_])
            writer.writerow(["# T_START:", f"{_isodate_}T{_hhmmss_[0]}"])
            writer.writerow(["# T_END:", f"{_isodate_}T{_hhmmss_[1]}"])
            writer.writerow(["# N_RECORD:", n_records])
            writer.writerow(["# DATA_TYP:", data_type])
            writer.writerow(["# ORIGFILE:", original_file])
            writer.writerow(["# FREQUEN:", frequen])
            writer.writerow(["#"])
            writer.writerow(["# COPYRIGHT. Grant of use."])
            writer.writerow(["# These data are property of Universidade Presbiteriana Mackenzie."])
            writer.writerow(["# The Centro de Radio Astronomia e Astrofisica Mackenzie is reponsible"])
            writer.writerow(["# for their distribution. Grant of use permission is given for Academic purposes only."])
            writer.writerow(["#"])
            writer.writerow(["# Columns:"])
            writer.writerow(
                ["# " + _print_space_csv_doc() + "ISO_DATETIME: Date and time in ISO 8601 format with milliseconds"])
            writer.writerow(["# " + _print_space_csv_doc() + "ADCVAL_1 to ADCVAL_6: ADC channel values (uint16)"])
            writer.writerow(["# " + _print_space_csv_doc() + "POS_TIME: Position timestamp (int32)"])
            writer.writerow(["# " + _print_space_csv_doc() + "AZIPOS / ELEPOS: Positioner coordinates (.001 deg)"])
            writer.writerow(["# " + _print_space_csv_doc() + "PM_DAZ / PM_DEL: Pointing model corrections (int16)"])
            writer.writerow(["# " + _print_space_csv_doc() + "AZIERR / ELEERR: Pointing errors (.001 deg)"])
            writer.writerow(["# " + _print_space_csv_doc() + "X_OFF / Y_OFF: Offset to target center (int16)"])
            writer.writerow(["# " + _print_space_csv_doc() + "OFF_1 to OFF_6: Attenuator settings (int16)"])
            writer.writerow(["# " + _print_space_csv_doc() + "TARGET: Observed target / mirror position (int8)"])
            writer.writerow(["# " + _print_space_csv_doc() + "OPMODE: Operation mode (int8)"])
            writer.writerow(["# " + _print_space_csv_doc() + "GPS_STATUS: GPS status (int16)"])
            writer.writerow(["# " + _print_space_csv_doc() + "RECNUM: Record number (int32)"])
            writer.writerow([])  # Linha em branco

            # =============================
            # Escrita dos dados
            # =============================
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for rec in records_to_generate_file:
                row = {
                    "ISO_DATETIME": rec["UTC_TIME"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
                    "ADCVAL_1": rec["ADCVAL_1"],
                    "ADCVAL_2": rec["ADCVAL_2"],
                    "ADCVAL_3": rec["ADCVAL_3"],
                    "ADCVAL_4": rec["ADCVAL_4"],
                    "ADCVAL_5": rec["ADCVAL_5"],
                    "ADCVAL_6": rec["ADCVAL_6"],
                    "POS_TIME": rec["POS_TIME"],
                    "AZIPOS": rec["AZIPOS"],
                    "ELEPOS": rec["ELEPOS"],
                    "PM_DAZ": rec["PM_DAZ"],
                    "PM_DEL": rec["PM_DEL"],
                    "AZIERR": rec["AZIERR"],
                    "ELEERR": rec["ELEERR"],
                    "X_OFF": rec["X_OFF"],
                    "Y_OFF": rec["Y_OFF"],
                    "OFF_1": rec["OFF_1"],
                    "OFF_2": rec["OFF_2"],
                    "OFF_3": rec["OFF_3"],
                    "OFF_4": rec["OFF_4"],
                    "OFF_5": rec["OFF_5"],
                    "OFF_6": rec["OFF_6"],
                    "TARGET": rec["TARGET"],
                    "OPMODE": rec["OPMODE"],
                    "GPS_STATUS": rec["GPS_STATUS"],
                    "RECNUM": rec["RECNUM"]
                }
                writer.writerow(row)

        print(f"CSV file created: {csv_file_path}")


