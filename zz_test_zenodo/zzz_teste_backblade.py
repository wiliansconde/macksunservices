#!/usr/bin/env python3
import sys
import os
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError


class ClsBackblazeB2Repository:
    """
    Uma classe para gerenciar o upload de arquivos para o Backblaze B2,
    organizando-os em uma estrutura de pastas baseada em data.
    """

    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, bucket_name: str):
        """
        Inicializa o repositório B2.

        :param endpoint_url: A URL do S3 Endpoint da sua App Key B2.
        :param access_key: O seu keyID da App Key.
        :param secret_key: A sua applicationKey da App Key.
        :param bucket_name: O nome do bucket B2 de destino.
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name

        # A URL base para arquivos públicos. Ex: 'https://f005.backblazeb2.com/file'
        # Você pode encontrar seu 'friendly URL' na página de 'Buckets' do B2.
        self.public_base_url = f"https://{bucket_name}.s3.{endpoint_url.split('.')[1]}.backblazeb2.com"

        self.b2_client = self._get_b2_client()

    # --- MÉTODO PÚBLICO PRINCIPAL ---
    def upload_file(self, instrument: str, resolution: str, local_file_path: str, overwrite: bool = False) -> Optional[
        str]:
        """
        Faz upload de um arquivo para o B2, organizando-o em pastas por ano/mês.

        :param instrument: Nome do instrumento (ex: 'POEMAS', 'SST').
        :param resolution: Resolução temporal (ex: '10ms').
        :param local_file_path: Caminho completo do arquivo local.
        :param overwrite: Se True, sobrescreve o arquivo no B2 se ele já existir.
        :return: URL pública de download do arquivo, ou None em caso de falha.
        """
        filename = Path(local_file_path).name
        print(f"\n--- Processando Arquivo: {filename} ---")

        parsed_info = self._parse_from_filename(local_file_path, instrument, resolution)
        if not parsed_info:
            return None

        inst, _, file_date, _ = parsed_info
        object_key = self._construct_object_key(inst, file_date, filename)

        try:
            if not overwrite and self._file_exists(object_key):
                print(f"   > Arquivo já existe em '{object_key}'. Pulando.")
                return f"{self.public_base_url}/{object_key}"

            print(f"   > Fazendo upload para '{object_key}'...")
            self.b2_client.upload_file(local_file_path, self.bucket_name, object_key)

            public_url = f"{self.public_base_url}/{object_key}"
            print(f"   ✅ Sucesso! URL pública: {public_url}")
            return public_url

        except FileNotFoundError:
            print(f"ERRO: O arquivo local '{local_file_path}' não foi encontrado.", file=sys.stderr)
            return None
        except ClientError as e:
            print(f"ERRO de API ao fazer o upload: {e}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"ERRO inesperado ao processar {filename}: {e}", file=sys.stderr)
            return None

    # --- MÉTODOS INTERNOS (PRIVADOS) ---
    def _get_b2_client(self):
        """Cria e retorna o cliente boto3 para se conectar ao B2."""
        try:
            return boto3.client(
                service_name='s3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
            )
        except Exception as e:
            print(f"ERRO: Falha ao inicializar o cliente B2: {e}", file=sys.stderr)
            sys.exit(1)  # Sai do script se não conseguir conectar

    def _construct_object_key(self, instrument: str, file_date: datetime, filename: str) -> str:
        """
        Constrói o caminho completo (chave de objeto) para o arquivo no B2.
        Exemplo: POEMAS/2024/08/poemas_10ms_2024-08-15.fits
        """
        year = file_date.strftime("%Y")
        month = file_date.strftime("%m")
        return f"{instrument.upper()}/{year}/{month}/{filename}"

    def _file_exists(self, object_key: str) -> bool:
        """Verifica se um objeto já existe no bucket."""
        try:
            self.b2_client.head_object(Bucket=self.bucket_name, Key=object_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                # Outro erro de API (ex: sem permissão)
                print(f"AVISO: Erro inesperado ao checar o arquivo '{object_key}': {e}", file=sys.stderr)
                raise

    @staticmethod
    def _parse_from_filename(file_path: str, inst_arg: str, res_arg: str) -> Optional[Tuple[str, str, datetime, str]]:
        """
        Tenta extrair informações de um nome de arquivo.
        Este método foi copiado do script original para Zenodo.
        """
        # Adicionei a dependência do `re` que faltava no exemplo original
        import re

        DATE_ANY_REGEX = re.compile(r"(?P<y>\d{4})[-_]?(\s)?(?P<m>\d{2})[-_]?(\s)?(?P<d>\d{2})")
        FILENAME_REGEX = re.compile(
            r"^(?P<inst>[A-Za-z]+)_(?P<res>\d+ms|[0-9]+ms|1s)_(?P<date>\d{4}-\d{2}-\d{2})(?:_fits)?\.(?P<ext>trk|fits|zip|gz|bz2|csv)$",
            re.IGNORECASE
        )
        fname = Path(file_path).name
        m = FILENAME_REGEX.match(fname)
        if m:
            inst, res, ext = m.group("inst").upper(), m.group("res").lower(), m.group("ext").lower()
            date_obj = datetime.strptime(m.group("date"), "%Y-%m-%d")
            return inst, res, date_obj, ext

        m_date = DATE_ANY_REGEX.search(fname)
        date_obj = datetime(int(m_date.group("y")), int(m_date.group("m")), int(m_date.group("d"))) if m_date else None
        ext = Path(fname).suffix.lstrip(".").lower()
        if date_obj and ext:
            return inst_arg.upper(), res_arg.lower(), date_obj, ext

        print(f"AVISO: Não foi possível extrair informações do nome '{fname}', pulando.", file=sys.stderr)
        return None


# --- Exemplo de como usar a classe ---
if __name__ == "__main__":

    # 1. Defina suas credenciais e configurações do B2
    # É uma boa prática carregar isso de variáveis de ambiente ou de um arquivo de configuração
    B2_ENDPOINT = "https://s3.us-west-004.backblazeb2.com"
    B2_KEY_ID = "K005KUS6QGbye/Vt8Jxjjg+gG9T/uOI"
    B2_APP_KEY = "0054a2c6eda249f0000000002"
    B2_BUCKET = "craamrep"

    # 2. Instancia a classe do repositório
    b2_uploader = ClsBackblazeB2Repository(
        endpoint_url=B2_ENDPOINT,
        access_key=B2_KEY_ID,
        secret_key=B2_APP_KEY,
        bucket_name=B2_BUCKET
    )

    # 3. Define os parâmetros para o upload
    instrumento = "POEMAS"  # Ajuste se necessário
    resolucao = "10ms"  # Ajuste se necessário

    # Aponta para a pasta específica no seu computador
    # O 'r' antes da string é importante para tratar as barras invertidas do Windows
    pasta_alvo = Path(r"C:\Y\WConde\zDemoPortal")

    # 4. Procura por todos os arquivos .zip dentro da pasta_alvo
    print(f"Procurando por arquivos .zip em '{pasta_alvo}'...")
    arquivos_zip = list(pasta_alvo.glob("*.zip"))

    if not arquivos_zip:
        print("Nenhum arquivo .zip encontrado na pasta. Encerrando.")
        sys.exit()

    print(f"Encontrados {len(arquivos_zip)} arquivos para upload.")

    # 5. Itera e chama o método principal para cada arquivo .zip encontrado
    for arquivo_path in arquivos_zip:
        b2_uploader.upload_file(
            instrument=instrumento,
            resolution=resolucao,
            local_file_path=str(arquivo_path)
        )
        print("-" * 50)

    print("\nExecução finalizada.")