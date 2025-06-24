
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

        header['FILENAME'] = os.path.basename(fits_file_path)
        header['INSTRUME'] = "Solar Submillimeter Telescope (SST)"
        header['TELESCOP'] = "Solar Submillimeter Telescope (SST)"
        header['OBSRVTRY'] = "Complejo Astronomico El Leoncito (CASLEO)"
        #header['FORMAT'] = "INTG/FAST/RF"
        header['OBS_ALTI'] = "2552 m"
        header['OBS_LONG'] = "-69.295583"
        header['OBS_LATI'] = "-31.798527"

        # Documentação do Header
        header.add_comment("Main Header: General metadata about the SST observation export")
        header.add_comment(_print_space_fits_doc_file() + "FILENAME: Name of the generated FITS file")
        header.add_comment(_print_space_fits_doc_file() + "INSTRUME: Instrument name (SST)")
        header.add_comment(_print_space_fits_doc_file() + "TELESCOP: Telescope name")
        header.add_comment(_print_space_fits_doc_file() + "OBSRVTRY: Observatory institution")
        #header.add_comment(_print_space_fits_doc_file() + "FORMAT: Internal data format (INTG/FAST/RF)")
        header.add_comment(_print_space_fits_doc_file() + "OBS_ALTI: Observatory altitude in meters")
        header.add_comment(_print_space_fits_doc_file() + "OBS_LONG: Observatory longitude in degrees")
        header.add_comment(_print_space_fits_doc_file() + "OBS_LATI: Observatory latitude in degrees")

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
        Cria um arquivo CSV para o SST (INTG/FAST/RF) incluindo documentação de campos como comentários no topo.
        """

        if not records_to_generate_file:
            print("[CSV] Nenhum registro disponível para exportação.")
            return

        fieldnames = [
            "ISO_DATETIME",
            "TIME",
            "ADCVAL_1",
            "ADCVAL_2",
            "ADCVAL_3",
            "ADCVAL_4",
            "ADCVAL_5",
            "ADCVAL_6",
            "POS_TIME",
            "AZIPOS",
            "ELEPOS",
            "PM_DAZ",
            "PM_DEL",
            "AZIERR",
            "ELEERR",
            "X_OFF",
            "Y_OFF",
            "OFF_1",
            "OFF_2",
            "OFF_3",
            "OFF_4",
            "OFF_5",
            "OFF_6",
            "TARGET",
            "OPMODE",
            "GPS_STATUS",
            "RECNUM"
        ]

        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

        with open(csv_file_path, mode='w', newline='') as csvfile:
            # ================================
            # Documentação dos campos (bloco de comentários)
            # ================================
            csvfile.write(
                "# ISO_DATETIME: Date and time in ISO 8601 format with milliseconds (YYYY-MM-DDTHH:MM:SS.sss)\n")
            csvfile.write("# TIME: Time in 0.1 ms since 0 UT\n")
            csvfile.write("# ADCVAL_1 to ADCVAL_6: ADC channel values\n")
            csvfile.write("# POS_TIME: Time of position sampling (0.1 ms since 0 UT)\n")
            csvfile.write("# AZIPOS: Positioner azimuth in millidegrees\n")
            csvfile.write("# ELEPOS: Positioner elevation in millidegrees\n")
            csvfile.write("# PM_DAZ: Pointing model correction in azimuth (mdeg)\n")
            csvfile.write("# PM_DEL: Pointing model correction in elevation (mdeg)\n")
            csvfile.write("# AZIERR: Azimuth error in millidegrees\n")
            csvfile.write("# ELEERR: Elevation error in millidegrees\n")
            csvfile.write("# X_OFF: Offset in azimuth or RA\n")
            csvfile.write("# Y_OFF: Offset in elevation or DEC\n")
            csvfile.write("# OFF_1 to OFF_6: Radiometric attenuator settings\n")
            csvfile.write("# TARGET: Observed target (including calibration mirror position)\n")
            csvfile.write("# OPMODE: Operation mode\n")
            csvfile.write("# GPS_STATUS: GPS status\n")
            csvfile.write("# RECNUM: Record number\n")
            csvfile.write("#\n")

            # ================================
            # Escrita dos dados
            # ================================
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
                    "TIME": record.get("TIME", ""),
                    "ADCVAL_1": record.get("ADCVAL_1", ""),
                    "ADCVAL_2": record.get("ADCVAL_2", ""),
                    "ADCVAL_3": record.get("ADCVAL_3", ""),
                    "ADCVAL_4": record.get("ADCVAL_4", ""),
                    "ADCVAL_5": record.get("ADCVAL_5", ""),
                    "ADCVAL_6": record.get("ADCVAL_6", ""),
                    "POS_TIME": record.get("POS_TIME", ""),
                    "AZIPOS": record.get("AZIPOS", ""),
                    "ELEPOS": record.get("ELEPOS", ""),
                    "PM_DAZ": record.get("PM_DAZ", ""),
                    "PM_DEL": record.get("PM_DEL", ""),
                    "AZIERR": record.get("AZIERR", ""),
                    "ELEERR": record.get("ELEERR", ""),
                    "X_OFF": record.get("X_OFF", ""),
                    "Y_OFF": record.get("Y_OFF", ""),
                    "OFF_1": record.get("OFF_1", ""),
                    "OFF_2": record.get("OFF_2", ""),
                    "OFF_3": record.get("OFF_3", ""),
                    "OFF_4": record.get("OFF_4", ""),
                    "OFF_5": record.get("OFF_5", ""),
                    "OFF_6": record.get("OFF_6", ""),
                    "TARGET": record.get("TARGET", ""),
                    "OPMODE": record.get("OPMODE", ""),
                    "GPS_STATUS": record.get("GPS_STATUS", ""),
                    "RECNUM": record.get("RECNUM", "")
                })

        print(f"[CSV] CSV file created with documentation header: {csv_file_path}")
