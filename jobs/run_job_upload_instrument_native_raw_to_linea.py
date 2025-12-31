import os
import shutil
import subprocess
import re
import argparse
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple, Dict

"""

**********************************************************************************
******************* JOB DE UPLOAD NATIVO PARA O SRVCRAAM **************************
**********************************************************************************

Job: run_job_upload_instrument_native_raw_to_linea.py

Descrição geral
----------------
Este job realiza o upload de arquivos nativos de instrumentos para o SRVCRAAM
utilizando um salto via login.linea.org.br. O processo preserva a estrutura
temporal Ano–Mês–Dia no destino remoto e implementa controle local de arquivos
já enviados.

O job foi projetado para lidar com diferentes estruturas de diretório entre
instrumentos por meio de perfis explícitos. Cada instrumento possui regras
claramente definidas para descoberta de arquivos, evitando heurísticas
implícitas e garantindo comportamento determinístico, auditável e seguro.

-------------------------------------------------------------------------------
Lógica de funcionamento
-------------------------------------------------------------------------------

1) Validação de conectividade SSH  
   - Windows → login.linea  
   - login.linea → srvcraam  

2) Varredura do diretório local informado procurando caminhos que terminem em  
   AAAA/Mnn/Dnn, independentemente do ponto inicial da varredura.

3) Para cada pasta de dia válida:
   - Seleção do perfil do instrumento com base no parâmetro --instrument  
   - Identificação dos arquivos a serem enviados conforme as regras do perfil  
   - Criação do diretório de destino remoto correspondente  
   - Envio de cada arquivo em dois saltos:
       • Windows → login.linea  
       • login.linea → srvcraam  
   - Remoção do arquivo da ponte após confirmação do envio  
   - Movimentação do arquivo local para a pasta de controle 00_enviado_linea  

4) A pasta 00_enviado_linea funciona como checkpoint local, impedindo reenvio
   acidental de arquivos já transferidos.

5) O modo --force restaura os arquivos da pasta de controle para permitir
   reprocessamento completo.

-------------------------------------------------------------------------------
Uso manual
-------------------------------------------------------------------------------

No Command Prompt do Windows:

1) Navegar até a raiz do projeto  
   cd C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\Craam_Loader

2) Executar definindo o caminho e o instrumento

POEMAS – raiz completa  
python -m jobs.run_job_upload_instrument_native_raw_to_linea ^
  --path "X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS" ^
  --instrument "poemas" ^
  --force

POEMAS – mês específico  
python -m jobs.run_job_upload_instrument_native_raw_to_linea ^
  --path "X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS\2012\M01" ^
  --instrument "poemas" ^
  --force

POEMAS – dia específico  
python -m jobs.run_job_upload_instrument_native_raw_to_linea ^
  --path "X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS\2012\M01\D01" ^
  --instrument "poemas"

SST – raiz completa  
python -m jobs.run_job_upload_instrument_native_raw_to_linea ^
  --path "C:\Y\WConde\Estudo\DoutoradoMack\Disciplinas\_PesquisaFinal\DadosSST" ^
  --instrument "sst"

-------------------------------------------------------------------------------
Parâmetros
-------------------------------------------------------------------------------

--path  
Diretório local de origem.  
Pode apontar para a raiz do instrumento, para um ano, para um mês ou para um dia.

--instrument  
Nome lógico do instrumento.  
Seleciona o perfil de regras utilizado para localizar arquivos e também
compõe o caminho remoto de destino.

--force  
Opcional.  
Restaura arquivos da pasta 00_enviado_linea para o diretório original,
permitindo reenvio completo.

-------------------------------------------------------------------------------
Como adicionar um novo instrumento
-------------------------------------------------------------------------------

O comportamento específico de cada instrumento é definido por um perfil
explícito no dicionário INSTRUMENT_PROFILES.

Cada perfil é uma instância de InstrumentProfile e define, de forma clara,
como os arquivos devem ser descobertos, enviados e organizados no destino.

Estrutura básica do perfil:

InstrumentProfile(
    name: str,
    recursive_from_day: bool,
    allowed_day_subfolders: Optional[List[str]],
    preserve_subfolders_on_remote: bool
)

-------------------------------------------------------------------------------
Descrição detalhada dos parâmetros do perfil
-------------------------------------------------------------------------------

name  
Nome lógico do instrumento.  
Deve coincidir com o valor passado em --instrument.  
Também define o caminho remoto base:

    <REMOTE_ROOT_PATH>/<instrument>/inbox/AAAA/Mnn/Dnn

recursive_from_day  
Define como os arquivos são coletados a partir da pasta do dia Dnn.

- False  
  Apenas arquivos diretamente dentro de Dnn são considerados.  
  Subpastas são ignoradas.  
  Adequado para instrumentos com arquivos planos por dia.

- True  
  Os arquivos são coletados de forma recursiva a partir de Dnn ou de subpastas
  explicitamente autorizadas.  
  Adequado para instrumentos que organizam dados por tipo.

allowed_day_subfolders  
Lista opcional de subpastas válidas dentro de Dnn.

- None  
  A varredura recursiva começa diretamente em Dnn.

- Lista de nomes  
  A varredura recursiva ocorre apenas dentro dessas subpastas.  
  Exemplo típico:
      fast  
      instr  
      intg  
      log  

Esse parâmetro restringe o escopo da varredura, evita envio de dados indevidos
e torna o comportamento explícito e auditável.

preserve_subfolders_on_remote  
Define se a estrutura de subpastas deve ser preservada no destino remoto.

- False  
  Todos os arquivos são enviados diretamente para:
      inbox/AAAA/Mnn/Dnn  
  Exige que não haja colisão de nomes entre arquivos.

- True  
  A estrutura relativa a partir de Dnn é preservada no destino remoto.  
  Exemplo:
      D20/fast/a.bin → inbox/AAAA/Mnn/Dnn/fast/a.bin

-------------------------------------------------------------------------------
Exemplo completo: Instrumento SST
-------------------------------------------------------------------------------

Estrutura local típica do SST a partir da pasta do dia Dnn:

Dnn/
 ├── fast/
 ├── instr/
 ├── intg/
 └── log/

Os arquivos do SST não existem diretamente no nível de Dnn.
Cada subpasta representa um tipo distinto de dado produzido pelo instrumento.

Perfil correspondente:

INSTRUMENT_PROFILES["sst"] = InstrumentProfile(
    name="sst",
    recursive_from_day=True,
    allowed_day_subfolders=["fast", "instr", "intg", "log"],
    preserve_subfolders_on_remote=True
)

Explicação do comportamento:

- O dia Dnn funciona apenas como contêiner lógico.  
- A coleta de arquivos é obrigatoriamente recursiva.  
- Apenas subpastas conhecidas são varridas.  
- A organização por tipo é preservada no destino remoto.

Mapeamento resultante:

    Dnn/fast/a.bin   → inbox/AAAA/Mnn/Dnn/fast/a.bin
    Dnn/instr/b.dat  → inbox/AAAA/Mnn/Dnn/instr/b.dat
    Dnn/intg/c.fits  → inbox/AAAA/Mnn/Dnn/intg/c.fits
    Dnn/log/run.log  → inbox/AAAA/Mnn/Dnn/log/run.log

Esse modelo evita colisão de nomes, mantém significado semântico dos dados
e garante consistência entre origem e destino.

-------------------------------------------------------------------------------
Comportamento padrão
-------------------------------------------------------------------------------

Caso um instrumento não esteja definido explicitamente em INSTRUMENT_PROFILES,
o job assume o comportamento padrão:

- recursive_from_day = False  
- allowed_day_subfolders = None  
- preserve_subfolders_on_remote = False  

Nesse modo, apenas arquivos diretamente dentro de Dnn serão enviados.

-------------------------------------------------------------------------------
Requisitos
-------------------------------------------------------------------------------

- Chave privada SSH sem passphrase configurada em  
  C:\Y\WConde\Estudo\DoutoradoMack\linea_chave\id_rsa

- Acesso SSH autorizado no login.linea.org.br com salto permitido para srvcraam

**********************************************************************************


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
#  PERFIS POR INSTRUMENTO
# ==============================================================================

@dataclass(frozen=True)
class InstrumentProfile:
    name: str
    recursive_from_day: bool
    allowed_day_subfolders: Optional[List[str]] = None
    preserve_subfolders_on_remote: bool = True

    def list_files(self, day_folder: Path) -> List[Tuple[Path, Path]]:
        """
        Retorna lista de pares:
            (arquivo_absoluto, caminho_relativo_a_partir_do_dia)
        """
        if not day_folder.exists():
            return []

        if not self.recursive_from_day:
            files: List[Tuple[Path, Path]] = []
            for p in day_folder.iterdir():
                if p.is_file():
                    files.append((p, Path(p.name)))
            return files

        base_paths: List[Path] = []
        if self.allowed_day_subfolders:
            for sub in self.allowed_day_subfolders:
                candidate = day_folder / sub
                if candidate.exists() and candidate.is_dir():
                    base_paths.append(candidate)
        else:
            base_paths = [day_folder]

        files_rec: List[Tuple[Path, Path]] = []
        for base in base_paths:
            for f in base.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(day_folder)
                    files_rec.append((f, rel))
        return files_rec

    def has_files(self, day_folder: Path) -> bool:
        return len(self.list_files(day_folder)) > 0


INSTRUMENT_PROFILES: Dict[str, InstrumentProfile] = {
    "poemas": InstrumentProfile(
        name="poemas",
        recursive_from_day=False,
        allowed_day_subfolders=None,
        preserve_subfolders_on_remote=False
    ),
    "sst": InstrumentProfile(
        name="sst",
        recursive_from_day=True,
        allowed_day_subfolders=["fast", "instr", "intg", "log"],
        preserve_subfolders_on_remote=True
    ),
}


def get_profile(instrument: str) -> InstrumentProfile:
    key = instrument.strip().lower()
    if key in INSTRUMENT_PROFILES:
        return INSTRUMENT_PROFILES[key]
    return InstrumentProfile(
        name=key,
        recursive_from_day=False,
        allowed_day_subfolders=None,
        preserve_subfolders_on_remote=False
    )


# ==============================================================================
#  LOGICA DE IDENTIFICACAO DE DIRETORIO
# ==============================================================================

def get_structure_suffix(full_path: Path) -> Optional[str]:
    path_str = str(full_path).replace("\\", "/")
    match = re.search(r"(\d{4}/M\d+/D\d+)$", path_str)
    if match:
        return match.group(1)
    return None


# ==============================================================================
#  TESTE DE CONEXAO
# ==============================================================================

def check_connections() -> bool:
    key_posix = Path(KEY_PATH).as_posix()
    print("\n[CHECK] Testando conectividade...")
    try:
        c1 = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=10",
            "-i", key_posix,
            f"{JUMP_USER}@{JUMP_HOST}",
            "echo OK"
        ]
        if subprocess.run(c1, capture_output=True, text=True).returncode != 0:
            print("[ERRO] Nao foi possivel acessar o Login.Linea.")
            return False

        c2 = [
            "ssh",
            "-i", key_posix,
            f"{JUMP_USER}@{JUMP_HOST}",
            f"ssh -o ConnectTimeout=10 {TARGET_HOST} 'echo OK'"
        ]
        if subprocess.run(c2, capture_output=True, text=True).returncode != 0:
            print("[ERRO] Nao foi possivel saltar para o srvcraam.")
            return False

        print("[OK] Conexoes validadas.")
        return True
    except Exception as e:
        print(f"[ERRO] Erro inesperado no teste: {e}")
        return False


# ==============================================================================
#  UPLOAD
# ==============================================================================

def run_remote_mkdir(remote_dir: str) -> None:
    key_posix = Path(KEY_PATH).as_posix()
    mkdir_cmd = [
        "ssh",
        "-i", key_posix,
        f"{JUMP_USER}@{JUMP_HOST}",
        f"ssh {TARGET_HOST} 'mkdir -p {remote_dir}'"
    ]
    subprocess.run(mkdir_cmd, capture_output=True)


def upload_one_file(local_file: Path, remote_dir: str) -> bool:
    """
    Faz upload em 2 saltos.
    Retorna True se o arquivo chegou no destino e foi removido da ponte.
    """
    key_posix = Path(KEY_PATH).as_posix()
    filename = local_file.name

    scp_cmd = [
        "scp",
        "-i", key_posix,
        str(local_file),
        f"{JUMP_USER}@{JUMP_HOST}:~/{filename}"
    ]
    r1 = subprocess.run(scp_cmd, capture_output=True)
    if r1.returncode != 0:
        return False

    mv_cmd = [
        "ssh",
        "-i", key_posix,
        f"{JUMP_USER}@{JUMP_HOST}",
        f"scp -p ~/{filename} {TARGET_HOST}:{remote_dir}/ && rm ~/{filename}"
    ]
    r2 = subprocess.run(mv_cmd, capture_output=True)
    if r2.returncode != 0:
        return False

    return True


def move_to_sync(day_folder: Path, sync_dir: Path, local_file: Path, rel_from_day: Path) -> None:
    """
    Move preservando estrutura dentro de 00_enviado_linea.
    Exemplo SST:
        D20/fast/a.bin  -> D20/00_enviado_linea/fast/a.bin
    """
    dst = sync_dir / rel_from_day
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(local_file), str(dst))


def process_day_folder(day_folder: Path, remote_suffix: str, instrument: str, profile: InstrumentProfile) -> None:
    sync_dir = day_folder / SYNC_FOLDER_NAME
    sync_dir.mkdir(exist_ok=True)

    items = profile.list_files(day_folder)
    if not items:
        return

    base_dest = f"{REMOTE_ROOT_PATH}/{instrument}/inbox/{remote_suffix}"

    print(f"\n>>> PROCESSANDO DIA: {remote_suffix}")
    print(f"    Origem: {day_folder}")
    print(f"    Arquivos: {len(items)}")

    if not profile.preserve_subfolders_on_remote:
        run_remote_mkdir(base_dest)

    for local_file, rel_from_day in items:
        if not local_file.exists():
            continue

        remote_dir = base_dest
        if profile.preserve_subfolders_on_remote:
            remote_dir = f"{base_dest}/{rel_from_day.parent.as_posix()}"
            run_remote_mkdir(remote_dir)

        print(f"    [->] Copiando: {rel_from_day.as_posix()}")

        ok = upload_one_file(local_file, remote_dir)
        if ok:
            move_to_sync(day_folder, sync_dir, local_file, rel_from_day)
            print("         [OK]")
        else:
            print(f"         [ERRO] Falha no envio: {rel_from_day.as_posix()}")


# ==============================================================================
#  FORCE RESTORE
# ==============================================================================

def restore_sync_folders(root_input: Path) -> None:
    print(f"\n[FORCE] Restaurando arquivos de {SYNC_FOLDER_NAME}...")
    for r, d, f in os.walk(root_input):
        if SYNC_FOLDER_NAME in d:
            sync_path = Path(r) / SYNC_FOLDER_NAME
            day_folder = Path(r)

            for item in sync_path.rglob("*"):
                if item.is_file():
                    rel = item.relative_to(sync_path)
                    dst = day_folder / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(item), str(dst))

            try:
                shutil.rmtree(sync_path)
            except Exception:
                pass


# ==============================================================================
#  MAIN
# ==============================================================================

def main(argv: List[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Caminho raiz ou especifico")
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    root_input = Path(args.path)
    instrument = args.instrument.strip().lower()
    profile = get_profile(instrument)

    if not check_connections():
        return

    if args.force:
        restore_sync_folders(root_input)

    print(f"\n[INFO] Iniciando varredura a partir de: {root_input}")
    print(f"[INFO] Instrumento: {instrument}")

    folders_found = 0

    for root, dirs, files in os.walk(root_input):
        if SYNC_FOLDER_NAME in dirs:
            dirs.remove(SYNC_FOLDER_NAME)

        suffix = get_structure_suffix(Path(root))
        if not suffix:
            continue

        day_folder = Path(root)
        if profile.has_files(day_folder):
            folders_found += 1
            process_day_folder(day_folder, suffix, instrument, profile)

    if folders_found == 0:
        print("\n[AVISO] Nenhuma pasta valida com arquivos foi encontrada.")
    else:
        print(f"\n[FIM] Processamento finalizado. {folders_found} pastas enviadas.")


if __name__ == "__main__":
    debug_params = [
        "--path", r"X:\DADOS_POEMAS_SST\Dados\_FINAL\POEMAS\2012\M01",
        "--instrument", "poemas",
        "--force"
    ]
    main(sys.argv[1:] if len(sys.argv) > 1 else debug_params)
