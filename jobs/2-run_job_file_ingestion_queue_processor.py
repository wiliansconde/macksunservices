"""
Job: run_job_upload_instrument_native_raw_to_linea.py

Uso:
    python run_job_upload.py <PATH_LOCAL> --instrument <NOME> --password <SENHA> [--force]
"""

import os
import sys
import shutil
import subprocess
import re
import time
import argparse
from pathlib import Path

# ==============================================================================
#  CONFIGURACOES FIXAS
# ==============================================================================

KEY_PATH = r"C:\Y\WConde\Estudo\DoutoradoMack\linea_chave\id_rsa"
JUMP_USER = "wilians.souza"
JUMP_HOST = "login.linea.org.br"
TARGET_USER = "wilians.souza"
TARGET_HOST = "srvcraam"
REMOTE_ROOT_PATH = "/mnt/cl/prj/craam/instrument_native_raw"

SYNC_FOLDER_NAME = "00_ENVIADOS_LINEA"
IGNORE_EXTENSIONS = ['.tmp', '.log', '.ini', '.DS_Store', 'Thumbs.db']

# ==============================================================================
#  FUNCOES
# ==============================================================================

def build_base_cmd(cmd_type, password=None):
    """Constrói a base do comando com ou sem sshpass."""
    key_posix = Path(KEY_PATH).as_posix()
    base = []

    if password:
        base.extend(["sshpass", "-p", password])

    base.append(cmd_type)

    # Flags de estabilidade
    base.extend(["-o", "StrictHostKeyChecking=no"])
    base.extend(["-i", key_posix])

    return base

def check_ssh_connection(password=None):
    """Testa conectividade SSH."""
    print("[INFO] Testando conexao SSH...")

    cmd = build_base_cmd("ssh", password)
    cmd.extend([
        "-o", f"ProxyJump={JUMP_USER}@{JUMP_HOST}",
        f"{TARGET_USER}@{TARGET_HOST}",
        "echo 'OK'"
    ])

    try:
        subprocess.run(cmd, check=True, timeout=15, capture_output=True)
        print("[OK] Conexao SSH estabelecida.\n")
        return True
    except Exception as e:
        print(f"[ERRO] FALHA DE CONEXAO: {e}")
        return False

def ensure_remote_dir(base_path, remote_suffix, password=None):
    """Garante pasta no servidor."""
    full_remote_path = f"{base_path}/{remote_suffix}"

    cmd = build_base_cmd("ssh", password)
    cmd.extend([
        "-o", f"ProxyJump={JUMP_USER}@{JUMP_HOST}",
        f"{TARGET_USER}@{TARGET_HOST}",
        f"mkdir -p {full_remote_path}"
    ])

    subprocess.run(cmd, check=True, capture_output=True)
    return full_remote_path

def process_day_folder(local_folder_path, remote_suffix, remote_base_path, password=None):
    """Processa lote de arquivos."""
    sync_dir = local_folder_path / SYNC_FOLDER_NAME
    sync_dir.mkdir(exist_ok=True)

    files = [f for f in local_folder_path.iterdir() if f.is_file()]
    if not files: return 0, 0

    print(f"   [DIR] Lote: {remote_suffix} ({len(files)} arquivos)")

    try:
        remote_full_dest = ensure_remote_dir(remote_base_path, remote_suffix, password)
    except Exception as e:
        print(f"      [ERRO] Falha ao criar pasta remota: {e}")
        return 0, len(files)

    ok_count = 0
    fail_count = 0

    for file_path in files:
        if file_path.suffix in IGNORE_EXTENSIONS: continue

        cmd = build_base_cmd("scp", password)
        cmd.extend([
            "-p",
            "-o", f"ProxyJump={JUMP_USER}@{JUMP_HOST}",
            str(file_path),
            f"{TARGET_USER}@{TARGET_HOST}:{remote_full_dest}/"
        ])

        res = subprocess.run(cmd, capture_output=True, text=True)

        if res.returncode == 0:
            try:
                shutil.move(str(file_path), str(sync_dir / file_path.name))
                ok_count += 1
                print(f"      [OK] {file_path.name}")
            except Exception as e:
                print(f"      [AVISO] Erro ao mover local: {e}")
        else:
            fail_count += 1
            print(f"      [FALHA] {file_path.name}")

    return ok_count, fail_count

def get_structure_suffix(full_path, root_input_path):
    try:
        path_obj = Path(full_path)
        root_obj = Path(root_input_path)
        relative = path_obj.relative_to(root_obj)
        relative_str = str(relative).replace("\\", "/")
        if re.search(r'\d{4}/M\d+/D\d+$', relative_str):
            return relative_str
        return None
    except ValueError:
        return None

def reset_processed_files(root_path):
    print(f"[INFO] Modo Force: Restaurando arquivos...")
    count = 0
    for root, dirs, files in os.walk(root_path, topdown=False):
        if SYNC_FOLDER_NAME in dirs:
            sync_path = Path(root) / SYNC_FOLDER_NAME
            for f in sync_path.iterdir():
                if f.is_file():
                    shutil.move(str(f), str(Path(root) / f.name))
                    count += 1
            try: sync_path.rmdir()
            except: pass
    print(f"[OK] {count} arquivos restaurados.\n")

# ==============================================================================
#  MAIN
# ==============================================================================

def main(manual_args=None):
    parser = argparse.ArgumentParser(description="Upload de Dados Brutos CRAAM")
    parser.add_argument("path", help=r"Caminho local raiz")
    parser.add_argument("--instrument", required=True, help="Nome do instrumento")
    parser.add_argument("--password", help="Senha do usuario (opcional se usar chave sem senha)")
    parser.add_argument("--force", action="store_true", help="Forcar reenvio")

    args = parser.parse_args(manual_args) if manual_args else parser.parse_args()

    root_input = Path(args.path)
    instrument = args.instrument.lower().strip()
    remote_instrument_path = f"{REMOTE_ROOT_PATH}/{instrument}/inbox"

    print("========================================================")
    print(f"   JOB UPLOAD: {instrument.upper()} -> LINEA")
    print(f"   DESTINO: {remote_instrument_path}")
    print("========================================================")

    if not root_input.exists():
        print(f"[ERRO] Caminho invalido.")
        sys.exit(1)

    if args.force:
        reset_processed_files(root_input)

    if not check_ssh_connection(args.password):
        sys.exit(1)

    tasks = []
    for root, dirs, files in os.walk(root_input):
        if SYNC_FOLDER_NAME in dirs: dirs.remove(SYNC_FOLDER_NAME)
        current = Path(root)
        suffix = get_structure_suffix(current, root_input)
        if suffix and any(current.iterdir()):
            tasks.append((current, suffix))

    if not tasks:
        print("[INFO] Nada para processar.")
        return

    total_ok, total_fail = 0, 0
    for i, (folder, suffix) in enumerate(tasks, 1):
        print(f"--- [{i}/{len(tasks)}] ---")
        ok, fail = process_day_folder(folder, suffix, remote_instrument_path, args.password)
        total_ok += ok
        total_fail += fail

    print(f"\nFINALIZADO. Sucesso: {total_ok} | Falha: {total_fail}")

if __name__ == "__main__":
    # --- MODO DEBUG ---
    caminho_teste = r"X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS"

    debug_params = [
        caminho_teste,
        "--instrument", "poemas",
        "--password", "teste.100" # Coloque sua senha aqui para testar
    ]

    main(debug_params)