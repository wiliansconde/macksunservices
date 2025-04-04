import os

class FileManager:
    @staticmethod
    def exists(file_path):
        """Verifica se o arquivo existe."""
        return os.path.exists(file_path)

    @staticmethod
    def delete(file_path):
        """Exclui o arquivo se existir."""
        if FileManager.exists(file_path):
            try:
                os.remove(file_path)
                print("Arquivo excluído com sucesso.")
                return True
            except Exception as e:
                print(f"Falha ao excluir o arquivo: {e}")
                return False
        else:
            print("O arquivo não existe.")
            return False

    @staticmethod
    def read(file_path):
        """Lê o conteúdo do arquivo se existir."""
        if FileManager.exists(file_path):
            with open(file_path, 'r') as file:
                return file.read()
        else:
            print("O arquivo não existe.")
            return None

    @staticmethod
    def write(file_path, content):
        """Escreve conteúdo no arquivo, criando-o se não existir."""
        with open(file_path, 'w') as file:
            file.write(content)
            print("Conteúdo escrito no arquivo com sucesso.")

    @staticmethod
    def delete_all_in_directory(directory_path):
        """Exclui todos os arquivos no diretório especificado."""
        if os.path.isdir(directory_path):
            try:
                for filename in os.listdir(directory_path):
                    file_to_delete = os.path.join(directory_path, filename)
                    if os.path.isfile(file_to_delete):
                        os.remove(file_to_delete)
                        print(f"Arquivo '{filename}' excluído com sucesso.")
                print("Todos os arquivos foram excluídos do diretório.")
            except Exception as e:
                print(f"Falha ao excluir arquivos no diretório: {e}")
        else:
            print("O caminho fornecido não é um diretório válido.")