#!/usr/-bin/env python3
import sys
import time
import requests
import locale
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

from config.ClsSettings import ClsSettings


class ClsZenodoBlobRepository:
    """
    Uma classe para gerenciar o upload e versionamento de arquivos no Zenodo.
    """

    def __init__(self, token: str, community: str, license_str: str, base_url: str):
        self.base_url = base_url
        self.community = community
        self.license = license_str
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

        try:
            locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
        except locale.Error:
            print("WARNING: 'en_US.UTF-8' locale not available.", file=sys.stderr)

    # --- MÉTODO PÚBLICO PRINCIPAL ---
    def upload_file(self, instrument: str, resolution: str, local_file_path: str) -> Optional[str]:
        """
        Faz upload de um arquivo para o Zenodo, criando ou versionando um depósito mensal.

        :param instrument: Nome do instrumento (ex: 'POEMAS', 'SST')
        :param resolution: Resolução temporal (ex: '10ms')
        :param local_file_path: Caminho completo do arquivo local
        :return: URL pública de download do arquivo, ou None em caso de falha.
        """
        filename_to_add = Path(local_file_path).name
        print(f"\n--- Processing File: {filename_to_add} ---")

        parsed_info = self._parse_from_filename(local_file_path, instrument, resolution)
        if not parsed_info:
            return None

        _, _, file_date, file_ext = parsed_info
        title = f"{instrument.upper()} {file_date.strftime('%B %Y')} {resolution.lower()}"

        try:
            deposit = self._find_deposit_by_title(title)
            draft_dep = None

            if deposit:
                existing_files = {f['filename'] for f in deposit.get('files', [])}
                if filename_to_add in existing_files and deposit['submitted']:
                    print(f"   > File already in latest published version. Skipping.")
                    record_id = deposit.get('id')
                    return f"https://zenodo.org/records/{record_id}/files/{filename_to_add}?download=1"

                draft_dep = self._get_draft_for_update(deposit)
            else:
                print(f"   > No deposit found for this month. Creating a new one...")
                draft_dep = self._create_new_deposit(instrument, resolution, title, file_date)

            if not draft_dep:
                print(f"ERROR: Could not get a draft for '{title}'.", file=sys.stderr)
                return None

            print(f"   > Verifying and updating metadata for draft ID {draft_dep['id']}...")
            self._ensure_metadata(draft_dep, instrument, resolution, title, file_date)

            draft_id = draft_dep['id']
            file_was_uploaded, file_existed = self._upload_single_file_to_draft(draft_id, local_file_path)

            if file_was_uploaded or (file_existed and not deposit.get('submitted', False)):
                return self._publish_draft(draft_id, filename_to_add)
            else:
                print("   > No new file uploaded. Not publishing.")
                return None

        except Exception as e:
            print(f"An unexpected error occurred while processing {filename_to_add}: {e}", file=sys.stderr)
            return None

    # --- MÉTODOS INTERNOS (PRIVADOS) ---
    def _get_draft_for_update(self, deposit: Dict[str, Any]) -> Dict[str, Any]:
        if deposit['submitted']:
            print(f"   > Found published deposit (ID: {deposit['id']}). Creating new version...")
            return self._create_new_version(deposit['id'])
        else:
            print(f"   > Found existing draft (ID: {deposit['id']}). Using it.")
            return deposit

    def _ensure_metadata(self, draft_dep, instrument, resolution, title, date_obj):
        required_metadata = self._get_required_metadata(instrument, resolution, title, date_obj)
        current_metadata = draft_dep.get('metadata', {})
        current_metadata.update(required_metadata)
        return self._update_deposit_metadata(draft_dep['id'], current_metadata)

    def _api_request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", 240)
        r = self.session.request(method, f"{self.base_url}{url}", **kwargs)
        r.raise_for_status()
        return r

    def _find_deposit_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        params = {"q": f'title:"{title}"', "all_versions": "true", "sort": "mostrecent"}
        r = self._api_request("get", "/deposit/depositions", params=params)
        results = r.json()
        return results[0] if results else None

    def _get_required_metadata(self, instrument, resolution, title: str, date_obj: datetime) -> Dict[str, Any]:
        return {
            "publication_date": datetime.now().strftime("%Y-%m-%d"),
            "title": title,
            "upload_type": "dataset",
            "description": (f"Data from the {instrument.upper()} instrument for {date_obj.strftime('%B %Y')}, "
                            f"with a temporal resolution of {resolution.lower()}. "
                            "This record contains daily data files in one or more formats."),
            "creators": [{'name': 'CRAAM, Universidade Presbiteriana Mackenzie',
                          'affiliation': 'Universidade Presbiteriana Mackenzie'}],
            "keywords": ["timeseries", instrument.upper(), "telescope", "Data Files", resolution.lower(), "CRAAM",
                         "Mackenzie"],
            "license": self.license,
            "communities": [{"identifier": self.community}] if self.community else []
        }

    def _create_new_deposit(self, instrument, resolution, title: str, date_obj: datetime) -> Dict[str, Any]:
        metadata = self._get_required_metadata(instrument, resolution, title, date_obj)
        r = self._api_request("post", "/deposit/depositions", json={"metadata": metadata})
        return r.json()

    def _update_deposit_metadata(self, dep_id: int, metadata: Dict[str, Any]) -> Dict[str, Any]:
        r = self._api_request("put", f"/deposit/depositions/{dep_id}", json={"metadata": metadata})
        return r.json()

    def _create_new_version(self, dep_id: int) -> Dict[str, Any]:
        r = self._api_request("post", f"/deposit/depositions/{dep_id}/actions/newversion")
        new_version_data = r.json()
        new_draft_url = new_version_data.get("links", {}).get("latest_draft")
        if not new_draft_url:
            raise Exception("Failed to get new draft URL after creating version.")
        # O new_draft_url é absoluto, então não usamos o base_url
        r_draft = self.session.get(new_draft_url, timeout=240)
        r_draft.raise_for_status()
        return r_draft.json()

    def _upload_single_file_to_draft(self, dep_id: int, file_to_upload: str) -> Tuple[bool, bool]:
        r_files = self._api_request("get", f"/deposit/depositions/{dep_id}/files")
        existing_filenames = {f['filename'] for f in r_files.json()}
        fname = Path(file_to_upload).name

        if fname in existing_filenames:
            print(f"   > File '{fname}' already exists in this draft. Skipping upload.")
            return False, True

        print(f"   > Uploading '{fname}'...")
        max_retries = 5
        retry_delay_seconds = 30
        for attempt in range(max_retries):
            try:
                with open(file_to_upload, "rb") as fh:
                    self._api_request("post", f"/deposit/depositions/{dep_id}/files", files={"file": (fname, fh)})
                print(f"    > Success on attempt {attempt + 1}.")
                return True, False
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 and attempt < max_retries - 1:
                    print(f"    > WARNING: Retrying in {retry_delay_seconds}s... (Attempt {attempt + 2}/{max_retries})")
                    time.sleep(retry_delay_seconds)
                else:
                    raise
        return False, False

    def _publish_draft(self, dep_id: int, filename_just_uploaded: str) -> Optional[str]:
        print(f"   > Publishing deposit ID {dep_id}...")
        r = self._api_request("post", f"/deposit/depositions/{dep_id}/actions/publish")
        pub_data = r.json()

        record_url = pub_data.get('links', {}).get('html')
        print(f"   ✅ Successfully published! Record URL: {record_url}")

        files = pub_data.get('files', [])
        record_id = pub_data.get('id')

        if files and record_id:
            for file_info in files:
                if file_info.get('filename') == filename_just_uploaded:
                    return f"https://zenodo.org/records/{record_id}/files/{filename_just_uploaded}?download=1"
        return record_url  # Fallback para a URL do registro

    @staticmethod
    def _parse_from_filename(file_path: str, inst_arg: str, res_arg: str) -> Optional[Tuple[str, str, datetime, str]]:
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

        print(f"WARNING: Could not parse filename '{fname}', skipping.", file=sys.stderr)
        return None


# --- Exemplo de como usar a classe ---
if __name__ == "__main__":

    # 1. Instancia a classe ClsZenodo com as configurações
    zenodo_uploader = ClsZenodo(
        token=ClsSettings.ZENODO_TOKEN,
        base_url=ClsSettings.ZENODO_BASE_URL,
        community=ClsSettings.ZENODO_COMMUNITY,
        license_str=ClsSettings.ZENODO_LICENSE
    )

    # 2. Define os parâmetros para o upload
    instrument = "POEMAS"
    resolution = "10ms"
    root_folder = r"C:\Y\WConde\Estudo\DoutoradoMack\z_test_zenodo"

    # 3. Escaneia a pasta e encontra os arquivos
    exts = ["*.zip", "*.fits", "*.csv"]
    all_files_paths = [p for pat in exts for p in Path(root_folder).rglob(pat)]

    if not all_files_paths:
        sys.exit("No new files found to process.")

    # 4. Itera e chama o método principal para cada arquivo
    for file_path in all_files_paths:
        print("-" * 80)
        final_url = zenodo_uploader.upload_file(
            instrument=instrument,
            resolution=resolution,
            local_file_path=str(file_path)
        )

        if final_url:
            print(f"\n>>> Final URL for {Path(file_path).name}: {final_url}")

        print(f"\n> Waiting 35 seconds before next operation...")
        time.sleep(35)

    print("\nExecution finished.")