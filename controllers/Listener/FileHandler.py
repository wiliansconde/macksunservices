import os
import time
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Lista de diretórios raiz a monitorar
WATCH_ROOTS = [
    r"C:\teste",
    r"C:\teste2"
]

# Sufixo usado para pastas de saída
PROCESSED_SUFFIX = "_processados"

# Garante que todas as raízes existam
for root in WATCH_ROOTS:
    os.makedirs(root, exist_ok=True)

class MultiRootHandler(FileSystemEventHandler):
    def process_file(self, src_path):
        file_name = os.path.basename(src_path)
        parent_folder = os.path.dirname(src_path)

        # Ignora arquivos que já estejam em pastas _processados
        if PROCESSED_SUFFIX in parent_folder:
            return

        time.sleep(1)  # Aguarda escrita, no caso de eventos reais

        processed_folder = parent_folder + PROCESSED_SUFFIX
        os.makedirs(processed_folder, exist_ok=True)
        dest_path = os.path.join(processed_folder, file_name)

        print(f"[NOVO ARQUIVO] {file_name}")
        print(f"Origem: {src_path}")
        print(f"Destino: {dest_path}")

        try:
            shutil.move(src_path, dest_path)
            print(f"[OK] Movido com sucesso.\n")
        except Exception as e:
            print(f"[ERRO] {e}\n")

    def on_created(self, event):
        if not event.is_directory:
            self.process_file(event.src_path)

def process_existing_files(handler):
    print("[SCAN] Processando arquivos já existentes...")
    for root in WATCH_ROOTS:
        for dirpath, dirnames, filenames in os.walk(root):
            if PROCESSED_SUFFIX in dirpath:
                continue
            dirnames[:] = [d for d in dirnames if PROCESSED_SUFFIX not in d]

            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                handler.process_file(full_path)

if __name__ == "__main__":
    print("[START] Monitorando múltiplas raízes:")
    for path in WATCH_ROOTS:
        print(f"✔ {path}")
    print(f"[REGRA] Ignorando pastas com sufixo '{PROCESSED_SUFFIX}'")

    event_handler = MultiRootHandler()
    observer = Observer()

    # Registra todas as pastas (exceto _processados)
    for root in WATCH_ROOTS:
        for dirpath, dirnames, _ in os.walk(root):
            if PROCESSED_SUFFIX in dirpath:
                continue
            dirnames[:] = [d for d in dirnames if PROCESSED_SUFFIX not in d]
            observer.schedule(event_handler, dirpath, recursive=False)

    # Processa arquivos já existentes ao iniciar
    process_existing_files(event_handler)

    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
