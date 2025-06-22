import gzip
import os
import shutil
import zipfile


class ZipHelper:
    @staticmethod
    def decompress_gz(file_path: str) -> str:
        """
        Descompacta um arquivo .gz no mesmo diretório onde o arquivo foi lido.

        :param file_path: Caminho do arquivo .gz a ser descompactado.
        :return: Caminho do arquivo descompactado.
        """
        if not file_path.endswith('.gz'):
            raise ValueError("O arquivo não possui a extensão .gz")

        output_file_path = file_path[:-3]  # Remove a extensão .gz

        with gzip.open(file_path, 'rb') as f_in:
            with open(output_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        return output_file_path

    @staticmethod
    def compress_files_by_stamp(output_folder: str, file_stamp: str) -> str:
        """
        Compacta todos os arquivos de um diretório que contenham o carimbo especificado no nome.

        Exemplo:
        Se o file_stamp for "poemas_10ms_2025-06-27", ele irá zipar todos os arquivos no output_folder
        que contenham essa string no nome.

        :param output_folder: Caminho da pasta onde estão os arquivos.
        :param file_stamp: Identificador único (carimbo) para selecionar os arquivos.
        :return: Caminho completo do arquivo .zip gerado.
        """
        import os
        import zipfile

        output_zip_path = os.path.join(output_folder, f"{file_stamp}.zip")
        matching_files = [
            os.path.join(output_folder, f) for f in os.listdir(output_folder)
            if file_stamp in f and os.path.isfile(os.path.join(output_folder, f))
        ]

        if not matching_files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado com o carimbo '{file_stamp}' em {output_folder}.")

        with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            for file_path in matching_files:
                arcname = os.path.basename(file_path)
                zipf.write(file_path, arcname=arcname)

        print(f"[ZIP] {len(matching_files)} arquivo(s) compactado(s) em: {output_zip_path}")
        return output_zip_path

    @staticmethod
    def compress_single_file(input_file_path: str) -> str:
        """
        Compacta um único arquivo gerando um ZIP com o nome: original.ext.zip

        Exemplo:
        Entrada: C:\...\poemas_10ms_20111203_abc123.csv
        Saída:   C:\...\poemas_10ms_20111203_abc123.csv.zip

        :param input_file_path: Caminho completo do arquivo de entrada
        :return: Caminho completo do arquivo .zip gerado
        """
        if not os.path.isfile(input_file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {input_file_path}")

        output_zip_path = input_file_path + ".zip"

        with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            arcname = os.path.basename(input_file_path)
            zipf.write(input_file_path, arcname=arcname)

        print(f"[ZIP] Arquivo compactado: {output_zip_path}")
        return output_zip_path
