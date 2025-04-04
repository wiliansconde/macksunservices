import gzip
import shutil


class GzipHandler:
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
