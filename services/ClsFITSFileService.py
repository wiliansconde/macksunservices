import os
import json
from collections import defaultdict
from datetime import datetime
from astropy.io import fits

class ClsFITSFileService:
    def __init__(self, input_json_folder: str, output_fits_folder: str):
        self.input_json_folder = input_json_folder
        self.output_fits_folder = output_fits_folder

    def generate_fits_files(self):
        """
        Recebe N arquivos JSON, cada um com um cabeçalho e uma linha, agrupando-os por data e gerando um arquivo FITS para cada grupo de data.
        """
        # Agrupa os arquivos JSON por data
        grouped_data = self._group_jsons_by_date()

        # Cria um arquivo FITS para cada grupo de data
        for date_str, records in grouped_data.items():
            fits_file_path = os.path.join(self.output_fits_folder, f"{date_str}.fits")
            self._create_fits_file(fits_file_path, records)

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
        # Cabeçalho básico com os metadados principais
        header = fits.Header()
        header['COMMENT'] = "FITS file generated from JSON data"
        header['DATE'] = records[0]['DATE']  # Usa a data do primeiro registro para o cabeçalho

        # Adiciona informações adicionais ao cabeçalho
        primary_hdu = fits.PrimaryHDU(header=header)

        # Definindo as colunas (campos principais dos registros JSON)
        col_time = fits.Column(
            name='UTC_TIME',
            format='D',  # 'D' for double precision to preserve exact precision
            array=[datetime.fromisoformat(rec['UTC_TIME']['$date'][:-1]).timestamp() for rec in records]
        )
        col_filepath = fits.Column(name='FILEPATHSOURCE', format='A100', array=[rec['FILEPATH'] for rec in records])
        col_tbmax = fits.Column(name='TBMAX', format='D', array=[float(rec['TBMAX']) for rec in records])
        col_tbmin = fits.Column(name='TBMIN', format='D', array=[float(rec['TBMIN']) for rec in records])
        col_freq1 = fits.Column(name='FREQ1', format='D', array=[float(rec['FREQ1']) for rec in records])
        col_freq2 = fits.Column(name='FREQ2', format='D', array=[float(rec['FREQ2']) for rec in records])
        col_nfreq = fits.Column(name='NFREQ', format='I', array=[int(rec['NFREQ']) for rec in records])

        col_ele = fits.Column(name='ELE', format='D', array=[float(rec['ELE']) for rec in records])
        col_azi = fits.Column(name='AZI', format='D', array=[float(rec['AZI']) for rec in records])
        col_tbl45 = fits.Column(name='TBL45', format='D', array=[float(rec['TBL45']) for rec in records])
        col_tbr45 = fits.Column(name='TBR45', format='D', array=[float(rec['TBR45']) for rec in records])
        col_tbl90 = fits.Column(name='TBL90', format='D', array=[float(rec['TBL90']) for rec in records])
        col_tbr90 = fits.Column(name='TBR90', format='D', array=[float(rec['TBR90']) for rec in records])

        col_year = fits.Column(name='UTC_TIME_YEAR', format='I', array=[int(rec['UTC_TIME_YEAR']) for rec in records])
        col_month = fits.Column(name='UTC_TIME_MONTH', format='I', array=[int(rec['UTC_TIME_MONTH']) for rec in records])
        col_day = fits.Column(name='UTC_TIME_DAY', format='I', array=[int(rec['UTC_TIME_DAY']) for rec in records])
        col_hh = fits.Column(name='UTC_TIME_HOUR', format='I', array=[int(rec['UTC_TIME_HOUR']) for rec in records])
        col_mm = fits.Column(name='UTC_TIME_MINUTE', format='I', array=[int(rec['UTC_TIME_MINUTE']) for rec in records])
        col_sec = fits.Column(name='UTC_TIME_SECOND', format='I', array=[int(rec['UTC_TIME_SECOND']) for rec in records])
        col_ms = fits.Column(name='UTC_TIME_MILLISECOND', format='I', array=[int(rec['UTC_TIME_MILLISECOND']) for rec in records])

        # Cria a tabela com os dados
        cols = fits.ColDefs([col_time, col_ele, col_azi,
                             col_tbl45, col_tbr45, col_tbl90,
                             col_tbr90, col_filepath, col_tbmax,
                             col_tbmin, col_freq1, col_freq2, col_nfreq,
                             col_year,col_month, col_day, col_hh,
                             col_mm, col_sec, col_ms

                             ])
        hdu = fits.BinTableHDU.from_columns(cols)

        # Grava o arquivo FITS
        hdul = fits.HDUList([primary_hdu, hdu])
        hdul.writeto(fits_file_path, overwrite=True)

        print(f"FITS file created: {fits_file_path}")

    def convert_fits_to_json(self, fits_file_path: str, output_json_folder: str):
        """
        Lê um arquivo FITS e gera um único arquivo JSON com todos os registros.
        """
        with fits.open(fits_file_path) as hdul:
            # Obtém o cabeçalho
            header = hdul[0].header

            # Obtém a tabela de dados do arquivo FITS
            data = hdul[1].data
            columns = data.columns.names  # Obtém os nomes das colunas do FITS

            records = []
            # Lê as colunas e cria JSON para cada linha
            for idx, record in enumerate(data):
                # Converte UTC_TIME de volta para o formato ISO8601
                utc_time_iso = datetime.utcfromtimestamp(float(record['UTC_TIME'])).isoformat() + 'Z'

                record_dict = {
                    'DATE': header.get('DATE', 'N/A'),
                    'FILEPATHSOURCE': record['FILEPATHSOURCE'] if 'FILEPATHSOURCE' in columns else None,
                    'TBMAX': float(record['TBMAX']) if 'TBMAX' in columns else None,
                    'TBMIN': float(record['TBMIN']) if 'TBMIN' in columns else None,
                    'FREQ1': float(record['FREQ1']) if 'FREQ1' in columns else None,
                    'FREQ2': float(record['FREQ2']) if 'FREQ2' in columns else None,
                    'NFREQ': float(record['NFREQ']) if 'NFREQ' in columns else None,
                    'ELE': float(record['ELE']) if 'ELE' in columns else None,
                    'AZI': float(record['AZI']) if 'AZI' in columns else None,
                    'TBL45': float(record['TBL45']) if 'TBL45' in columns else None,
                    'TBR45': float(record['TBR45']) if 'TBR45' in columns else None,
                    'TBL90': float(record['TBL90']) if 'TBL90' in columns else None,
                    'TBR90': float(record['TBR90']) if 'TBR90' in columns else None,
                    'UTC_TIME': {'$date': utc_time_iso},  # Mantém o UTC_TIME no formato ISO8601
                    'UTC_TIME_YEAR': float(record['UTC_TIME_YEAR']) if 'UTC_TIME_YEAR' in columns else None,
                    'UTC_TIME_MONTH': float(record['UTC_TIME_MONTH']) if 'UTC_TIME_MONTH' in columns else None,
                    'UTC_TIME_DAY': float(record['UTC_TIME_DAY']) if 'UTC_TIME_DAY' in columns else None,
                    'UTC_TIME_HOUR': float(record['UTC_TIME_HOUR']) if 'UTC_TIME_HOUR' in columns else None,
                    'UTC_TIME_MINUTE': float(record['UTC_TIME_MINUTE']) if 'UTC_TIME_MINUTE' in columns else None,
                    'UTC_TIME_SECOND': float(record['UTC_TIME_SECOND']) if 'UTC_TIME_SECOND' in columns else None,
                    'UTC_TIME_MILLISECOND': float(record['UTC_TIME_MILLISECOND']) if 'UTC_TIME_MILLISECOND' in columns else None,

                }

                records.append(record_dict)

            # Cria o nome do arquivo JSON
            json_filename = f"{os.path.basename(fits_file_path).replace('.fits', '')}.json"
            json_path = os.path.join(output_json_folder, json_filename)

            # Salva o JSON com todos os registros
            with open(json_path, 'w') as json_file:
                json.dump(records, json_file, indent=4)

            print(f"JSON file created: {json_path}")
