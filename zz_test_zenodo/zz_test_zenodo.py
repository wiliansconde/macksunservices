#!/usr/bin/env python3
import sys
import time
import requests
import locale
import json
import shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Optional, Tuple, Dict, Any
from types import SimpleNamespace
import re

# Set locale to English for month names.
try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except locale.Error:
    print("WARNING: 'en_US.UTF-8' locale not available. Month names may appear in system language.", file=sys.stderr)

# Regular Expressions
DATE_ANY_REGEX = re.compile(r"(?P<y>\d{4})[-_]?(\s)?(?P<m>\d{2})[-_]?(\s)?(?P<d>\d{2})")
FILENAME_REGEX = re.compile(
    r"^(?P<inst>[A-Za-z]+)_(?P<res>\d+ms|[0-9]+ms|1s)_(?P<date>\d{4}-\d{2}-\d{2})(?:_fits)?\.(?P<ext>trk|fits|zip|gz|bz2|csv)$",
    re.IGNORECASE
)


def extract_date_from_string(name: str) -> Optional[datetime]:
    m = DATE_ANY_REGEX.search(name)
    if not m: return None
    try:
        return datetime(int(m.group("y")), int(m.group("m")), int(m.group("d")))
    except ValueError:
        return None


def parse_from_filename(file_path: str, inst_arg: str, res_arg: str) -> Optional[
    Tuple[str, str, datetime, str]]:
    fname = Path(file_path).name
    m = FILENAME_REGEX.match(fname)
    if m:
        inst, res, ext = m.group("inst").upper(), m.group("res").lower(), m.group("ext").lower()
        date_obj = datetime.strptime(m.group("date"), "%Y-%m-%d")
        return inst, res, date_obj, ext

    date_in_name = extract_date_from_string(fname)
    ext = Path(fname).suffix.lstrip(".").lower()
    if date_in_name and ext:
        return inst_arg.upper(), res_arg.lower(), date_in_name, ext

    print(f"WARNING: Could not extract date or extension from filename '{fname}', skipping.", file=sys.stderr)
    return None


def api_request(method: str, session: requests.Session, url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", 240)
    try:
        r = session.request(method, url, **kwargs)
        r.raise_for_status()
        return r
    except requests.exceptions.HTTPError as e:
        raise
    except requests.exceptions.RequestException as e:
        print(f"Connection Error: {e}", file=sys.stderr)
        raise


def find_deposit_by_title(session: requests.Session, base_url: str, title: str) -> Optional[Dict[str, Any]]:
    params = {"q": f'title:"{title}"', "all_versions": "true", "sort": "mostrecent"}
    r = api_request("get", session, f"{base_url}/deposit/depositions", params=params)
    results = r.json()
    return results[0] if results else None


def get_required_metadata(args: SimpleNamespace, title: str, date_obj: datetime) -> Dict[str, Any]:
    return {
        "publication_date": datetime.now().strftime("%Y-%m-%d"),
        "title": title,
        "upload_type": "dataset",
        "description": (f"Data from the {args.instrument.upper()} instrument for {date_obj.strftime('%B %Y')}, "
                        f"with a temporal resolution of {args.resolution.lower()}. "
                        "This record contains daily data files in one or more formats (e.g., ZIP, FITS, CSV)."),
        "creators": [{'name': 'CRAAM, Universidade Presbiteriana Mackenzie',
                      'affiliation': 'Universidade Presbiteriana Mackenzie'}],
        "keywords": ["timeseries", args.instrument.upper(), "telescope", "Data Files", args.resolution.lower(),
                     "CRAAM", "Mackenzie"],
        "license": args.license,
        "communities": [{"identifier": args.community}] if args.community else []
    }


def create_new_deposit(session: requests.Session, args: SimpleNamespace, title: str, date_obj: datetime) -> Dict[
    str, Any]:
    metadata = get_required_metadata(args, title, date_obj)
    r = api_request("post", session, f"{args.base_url}/deposit/depositions", json={"metadata": metadata})
    return r.json()


def update_deposit_metadata(session: requests.Session, base_url: str, dep_id: int, metadata: Dict[str, Any]) -> Dict[
    str, Any]:
    r = api_request("put", session, f"{base_url}/deposit/depositions/{dep_id}", json={"metadata": metadata})
    return r.json()


def create_new_version(session: requests.Session, base_url: str, dep_id: int) -> Dict[str, Any]:
    r = api_request("post", session, f"{base_url}/deposit/depositions/{dep_id}/actions/newversion")
    new_version_data = r.json()
    new_draft_url = new_version_data.get("links", {}).get("latest_draft")
    if not new_draft_url:
        raise Exception("Failed to get new draft URL after creating version.")
    r_draft = api_request("get", session, new_draft_url)
    return r_draft.json()


def upload_files_to_draft(session: requests.Session, base_url: str, dep_id: int, file_to_upload: str) -> Tuple[
    bool, bool]:
    r_files = api_request("get", session, f"{base_url}/deposit/depositions/{dep_id}/files")
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
                api_request("post", session, f"{base_url}/deposit/depositions/{dep_id}/files",
                            files={"file": (fname, fh)})
            print(f"    > Success on attempt {attempt + 1}.")
            return True, False
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and attempt < max_retries - 1:
                print(
                    f"    > WARNING: Received 403 Forbidden. Retrying in {retry_delay_seconds}s... (Attempt {attempt + 2}/{max_retries})")
                time.sleep(retry_delay_seconds)
            else:
                print(
                    f"    > ERROR: Failed on last attempt or received a non-retriable error: {e.response.status_code} - {e.response.text}",
                    file=sys.stderr)
                raise
    return False, False


def publish_draft(session: requests.Session, base_url: str, dep_id: int, filename_just_uploaded: str):
    print(f"   > Publishing deposit ID {dep_id}...")
    r = api_request("post", session, f"{base_url}/deposit/depositions/{dep_id}/actions/publish")
    pub_data = r.json()
    record_url = pub_data.get('links', {}).get('html')
    print(f"   ✅ Successfully published! Record URL: {record_url}")

    files = pub_data.get('files', [])
    record_id = pub_data.get('id')
    if files and record_id:
        print("   --- Published File URL ---")
        for file_info in files:
            filename = file_info.get('filename')
            if filename == filename_just_uploaded:
                download_url = f"https://zenodo.org/records/{record_id}/files/{filename}?download=1"
                print(f"   - {filename}: {download_url}")
                break
        print("   --------------------------")


def process_single_file_publication(session: requests.Session, args: SimpleNamespace, file_info: dict,
                                    processed_dir: Path):
    file_path = file_info['path']
    file_date = file_info['date']

    title = f"{args.instrument.upper()} {file_date.strftime('%B %Y')} {args.resolution.lower()}"
    filename_to_add = Path(file_path).name
    print(f"\n--- Processing File: {filename_to_add} for Deposit: '{title}' ---")

    source_path = Path(file_path)
    dest_path = processed_dir / source_path.name

    deposit = find_deposit_by_title(session, args.base_url, title)
    draft_dep = None

    if deposit:
        existing_filenames_published = {f['filename'] for f in deposit.get('files', [])}
        if filename_to_add in existing_filenames_published and deposit['submitted']:
            print(f"   > File '{filename_to_add}' already in the LATEST PUBLISHED version. Moving to processed folder.")
            shutil.move(source_path, dest_path)
            return

        if deposit['submitted']:
            print(f"   > Found published deposit (ID: {deposit['id']}). Creating new version...")
            draft_dep = create_new_version(session, args.base_url, deposit['id'])
        else:
            print(f"   > Found existing draft (ID: {deposit['id']}). Using it.")
            draft_dep = deposit
    else:
        print(f"   > No deposit found for this month. Creating a new one...")
        draft_dep = create_new_deposit(session, args, title, file_date)

    if not draft_dep:
        print(f"ERROR: Could not get a draft for '{title}'. Skipping this file.", file=sys.stderr)
        return

    print(f"   > Verifying and updating metadata for draft ID {draft_dep['id']}...")
    required_metadata = get_required_metadata(args, title, file_date)
    current_metadata = draft_dep.get('metadata', {})
    current_metadata.update(required_metadata)
    draft_dep = update_deposit_metadata(session, args.base_url, draft_dep['id'], current_metadata)

    draft_id = draft_dep['id']
    file_was_uploaded, file_existed_in_draft = upload_files_to_draft(session, args.base_url, draft_id, file_path)

    if file_was_uploaded or (file_existed_in_draft and not deposit['submitted']):
        publish_draft(session, args.base_url, draft_id, filename_to_add)
        print(f"   > Moving '{source_path.name}' to processed folder.")
        shutil.move(source_path, dest_path)
    else:
        print("   > No new file uploaded. Not publishing.")


def main():
    args = SimpleNamespace(
        token="vN0jaQTdNwmfhs4lg6abmdUwssTLnl6eSZpuKSVkYMSfyyrcfEJGmzEyqCuJ",
        base_url="https://zenodo.org/api",
        community="conde_set",
        license="cc-by-4.0",
        instrument="POEMAS",
        root_folder=r"C:\Y\WConde\Estudo\DoutoradoMack\z_test_zenodo",
        resolution="10ms",
    )

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {args.token}"})

    # Cria a pasta de arquivos processados, se não existir
    processed_dir = Path(args.root_folder) / "processed_ok"
    processed_dir.mkdir(parents=True, exist_ok=True)
    print(f"Processed files will be moved to: {processed_dir}")

    print(f"Starting scan for instrument '{args.instrument}' in '{args.root_folder}'...")

    exts = ["*.trk", "*.TRK", "*.zip", "*.fits", "*.csv", "*.bz2"]
    all_files_paths = [p for pat in exts for p in Path(args.root_folder).rglob(pat)]

    sortable_files = []
    for file_path in all_files_paths:
        parsed_info = parse_from_filename(str(file_path), args.instrument, args.resolution)
        if parsed_info:
            sortable_files.append({
                "date": parsed_info[2],
                "path": str(file_path),
            })

    if not sortable_files:
        sys.exit("No new files found to process.")

    sorted_files = sorted(sortable_files, key=lambda x: x['date'])

    for file_info in sorted_files:
        process_single_file_publication(session, args, file_info, processed_dir)
        print("\n   > Waiting 35 seconds before processing next file...")
        time.sleep(35)

    print("\nExecution finished.")


if __name__ == "__main__":
    main()