import os
import shutil
import subprocess
import re
import argparse
import sys
from pathlib import Path

"""
**********************************************************************************
******************* JOB DE UPLOAD NATIVO PARA O SRVCRAAM ************************
**********************************************************************************

Job: run_job_upload_instrument_native_raw_to_linea.py

Descrição:
    Realiza o upload de arquivos de instrumentos para o SRVCRAAM através de um salto (jump)
    no servidor login.linea.org.br. O script mantém a estrutura de diretórios 
    Ano/Mês/Dia no destino final.

Lógica de Funcionamento:
    1. Valida conectividade (Windows -> login.linea -> srvcraam).
    2. Varre o diretório local em busca do padrão AAAA/Mnn/Dnn.
    3. Envia arquivos para a ponte (~/ no login.linea).
    4. Move da ponte para o diretório final no NFS do srvcraam e remove da ponte.
    5. Move o arquivo local para a pasta '00_ENVIADOS_LINEA' para controle.

Uso Manual:
    No command DOS:
    1. Navegue até a raiz do projeto:
       cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

    2. Execute definindo o caminho e o instrumento:
       POEMAS (Raiz completa)
       python -m jobs.run_job_upload_instrument_native_raw_to_linea --path "X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS" --instrument "poemas"

       POEMAS (Mês específico)
       python -m jobs.run_job_upload_instrument_native_raw_to_linea --path "X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS\2012\M01" --instrument "poemas" --force

       POEMAS (Dia específico)
       python -m jobs.run_job_upload_instrument_native_raw_to_linea --path "X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS\2012\M01\D01" --instrument "poemas"

    3. Para reprocessar (mover arquivos de volta para a raiz local):
       python -m jobs.run_job_upload_instrument_native_raw_to_linea --path "..." --instrument "poemas" --force

Parâmetros:
    --path: Diretório local de origem (pode ser a raiz do instrumento, ano, mês ou dia).
    --instrument: Nome do instrumento (poemas, sst, fast, etc) para compor o path remoto.
    --force: (Opcional) Restaura arquivos da pasta 00_ENVIADOS_LINEA para reenvio.

Requisitos:
    - Chave privada SSH sem passphrase configurada em: C:\Y\WConde\Estudo\DoutoradoMack\linea_chave\id_rsa
    - Acesso SSH autorizado no login.linea.org.br e salto permitido para srvcraam.
"""

# ==============================================================================
#  CONFIGURACOES
# ==============================================================================
KEY_PATH = r"C:\Y\WConde\Estudo\DoutoradoMack\linea_chave\id_rsa"
JUMP_USER = "wilians.souza"
JUMP_HOST = "login.linea.org.br"
TARGET_HOST = "srvcraam"
REMOTE_ROOT_PATH = "/mnt/cl/prj/craam/instrument_native_raw"
SYNC_FOLDER_NAME = "00_enviado_linea"


# ==============================================================================
#  LOGICA DE IDENTIFICACAO DE DIRETORIO (O CORAÇÃO DO SCRIPT)
# ==============================================================================

def get_structure_suffix(full_path):
    """
    Analisa o caminho completo e extrai o sufixo AAAA/Mnn/Dnn.
    Funciona nao importa se voce começou a varredura na raiz ou no dia.
    """
    # Converte para string com barras para frente para a regex
    path_str = str(full_path).replace("\\", "/")

    # Regex: Procura por 4 digitos / M + numeros / D + numeros no final do caminho
    match = re.search(r'(\d{4}/M\d+/D\d+)$', path_str)

    if match:
        return match.group(1)
    return None


# ==============================================================================
#  TESTE DE CONEXAO
# ==============================================================================

def check_connections():
    key_posix = Path(KEY_PATH).as_posix()
    print("\n[CHECK] Testando conectividade...")
    try:
        # Teste 1: Jump
        c1 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-i", key_posix, f"{JUMP_USER}@{JUMP_HOST}",
              "echo OK"]
        if subprocess.run(c1, capture_output=True, text=True).returncode != 0:
            print("[ERRO] Nao foi possivel acessar o Login.Linea.")
            return False

        # Teste 2: Salto para srvcraam
        c2 = ["ssh", "-i", key_posix, f"{JUMP_USER}@{JUMP_HOST}", f"ssh -o ConnectTimeout=10 {TARGET_HOST} 'echo OK'"]
        if subprocess.run(c2, capture_output=True, text=True).returncode != 0:
            print("[ERRO] Nao foi possivel saltar para o srvcraam.")
            return False

        print("[OK] Conexões validadas.")
        return True
    except Exception as e:
        print(f"[ERRO] Erro inesperado no teste: {e}")
        return False


# ==============================================================================
#  PROCESSO DE UPLOAD
# ==============================================================================

def process_day_folder(local_folder_path, remote_suffix, instrument):
    sync_dir = local_folder_path / SYNC_FOLDER_NAME
    sync_dir.mkdir(exist_ok=True)

    # Lista arquivos reais (ignora a pasta 00_ENVIADOS e outras subpastas)
    files = [f for f in local_folder_path.iterdir() if f.is_file()]
    if not files:
        return

    key_posix = Path(KEY_PATH).as_posix()
    final_dest = f"{REMOTE_ROOT_PATH}/{instrument}/inbox/{remote_suffix}"

    print(f"\n>>> PROCESSANDO DIA: {remote_suffix}")
    print(f"    Origem: {local_folder_path}")

    # PASSO 1: Criar pasta remota
    mkdir_cmd = ["ssh", "-i", key_posix, f"{JUMP_USER}@{JUMP_HOST}", f"ssh {TARGET_HOST} 'mkdir -p {final_dest}'"]
    subprocess.run(mkdir_cmd, capture_output=True)

    for file_path in files:
        filename = file_path.name
        print(f"    [->] Copiando: {filename}")

        # PASSO 2: Windows -> Jump
        scp_cmd = ["scp", "-i", key_posix, str(file_path), f"{JUMP_USER}@{JUMP_HOST}:~/{filename}"]
        if subprocess.run(scp_cmd, capture_output=True).returncode == 0:

            # PASSO 3: Jump -> Target (e apaga da ponte)

            mv_cmd = ["ssh", "-i", key_posix, f"{JUMP_USER}@{JUMP_HOST}",
                      f"scp -p ~/{filename} {TARGET_HOST}:{final_dest}/ && rm ~/{filename}"]
            if subprocess.run(mv_cmd, capture_output=True).returncode == 0:
                # PASSO 4: Move localmente para a pasta de controle
                shutil.move(str(file_path), str(sync_dir / filename))
                print(f"         [OK]")
            else:
                print(f"         [ERRO] Falha no salto final para {TARGET_HOST}")
        else:
            print(f"         [ERRO] Falha no envio para ponte {JUMP_HOST}")


# ==============================================================================
#  MAIN
# ==============================================================================

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Caminho raiz ou específico")
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    root_input = Path(args.path)

    if not check_connections():
        return

    # Se --force for usado, limpa as pastas de controle para reprocessar
    if args.force:
        print(f"\n[FORCE] Restaurando arquivos de {SYNC_FOLDER_NAME}...")
        for r, d, f in os.walk(root_input):
            if SYNC_FOLDER_NAME in d:
                p = Path(r) / SYNC_FOLDER_NAME
                for x in p.iterdir():
                    if x.is_file():
                        shutil.move(str(x), str(Path(r) / x.name))
                try:
                    p.rmdir()
                except:
                    pass

    print(f"\n[INFO] Iniciando varredura a partir de: {root_input}")

    folders_found = 0
    # O walk percorre recursivamente. Se voce passar o caminho ate o DIA,
    # ele processa apenas aquela pasta. Se passar ate o MES, processa todos os dias dentro.
    for root, dirs, files in os.walk(root_input):
        # Nao entra na pasta de controle
        if SYNC_FOLDER_NAME in dirs:
            dirs.remove(SYNC_FOLDER_NAME)

        suffix = get_structure_suffix(root)

        # Só processa se o caminho terminar em AAAA/Mnn/Dnn
        if suffix:
            # Verifica se existem arquivos de fato para processar
            if any(f.is_file() for f in Path(root).iterdir()):
                folders_found += 1
                process_day_folder(Path(root), suffix, args.instrument)

    if folders_found == 0:
        print("\n[AVISO] Nenhuma pasta valida (padrao Ano/Mes/Dia) com arquivos foi encontrada.")
    else:
        print(f"\n[FIM] Processamento finalizado. {folders_found} pastas enviadas.")


if __name__ == "__main__":
    # DEBUG: Altere o --path para testar os cenarios 1 ou 2
    debug_params = [
        "--path", r"X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS\2012\M01",
        "--instrument", "poemas",
        "--force"
    ]

    main(sys.argv[1:] if len(sys.argv) > 1 else debug_params)