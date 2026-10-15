
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from zipfile import ZipFile
from datetime import datetime, timezone
import hashlib
import json
import re
import shutil


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
STAGING_DIR = ROOT / "data" / "staging"

INDEX_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024


def fetch_bytes(url, max_bytes):
    request = Request(url, headers={"User-Agent": "EventPulse/1.0"})

    with urlopen(request, timeout=60) as response:
        data = response.read(max_bytes + 1)

    if len(data) > max_bytes:
        raise ValueError("Download exceeds the configured size limit")

    return data


def get_latest_events():
    index = fetch_bytes(INDEX_URL, 10000).decode("utf-8")

    for line in index.splitlines():
        size, checksum, url = line.split()

        filename = Path(urlparse(url).path).name

        if re.fullmatch(r"\d{14}\.export\.CSV\.zip", filename):
            return {
                "filename": filename,
                "expected_size": int(size),
                "md5": checksum.lower(),
                "url": url.replace("http://", "https://", 1),
            }

    raise ValueError("No GDELT Events file found in latest index")


def download_events(metadata):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    zip_path = RAW_DIR / metadata["filename"]
    csv_name = metadata["filename"].removesuffix(".zip")
    csv_path = STAGING_DIR / csv_name

    if zip_path.exists():
        content = zip_path.read_bytes()
    else:
        content = fetch_bytes(metadata["url"], MAX_DOWNLOAD_BYTES)

    actual_md5 = hashlib.md5(content).hexdigest()

    if len(content) != metadata["expected_size"]:
        raise ValueError("Downloaded file size does not match GDELT metadata")

    if actual_md5 != metadata["md5"]:
        raise ValueError("Downloaded file checksum does not match GDELT metadata")

    if not zip_path.exists():
        zip_path.write_bytes(content)

    with ZipFile(zip_path) as archive:
        if archive.testzip() is not None:
            raise ValueError("ZIP integrity validation failed")

        files = [item for item in archive.infolist() if not item.is_dir()]

        if len(files) != 1 or Path(files[0].filename).name != csv_name:
            raise ValueError("Unexpected GDELT archive structure")

        if not csv_path.exists():
            with archive.open(files[0]) as source:
                with csv_path.open("wb") as target:
                    shutil.copyfileobj(source, target)

    manifest = {
        **metadata,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "local_csv": str(csv_path.relative_to(ROOT)).replace("\\", "/"),
        "csv_size_bytes": csv_path.stat().st_size,
        "status": "validated",
    }

    manifest_path = RAW_DIR / f"{csv_name}.manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return zip_path, csv_path


def main():
    metadata = get_latest_events()
    zip_path, csv_path = download_events(metadata)

    print("GDELT EVENTS DOWNLOAD")
    print("=" * 60)
    print(f"Archive: {zip_path.name}")
    print(f"CSV: {csv_path.name}")
    print(f"Compressed size: {zip_path.stat().st_size:,} bytes")
    print(f"Extracted size: {csv_path.stat().st_size:,} bytes")
    print("Checksum: PASSED")
    print("ZIP integrity: PASSED")


if __name__ == "__main__":
    main()
