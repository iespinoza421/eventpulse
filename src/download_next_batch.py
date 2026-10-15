
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from zipfile import ZipFile
import shutil


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
STAGING_DIR = ROOT / "data" / "staging"

PREVIOUS_BATCH = "20261013210000"
BASE_URL = "https://data.gdeltproject.org/gdeltv2/"


def main():
    previous_time = datetime.strptime(
        PREVIOUS_BATCH, "%Y%m%d%H%M%S"
    ).replace(tzinfo=timezone.utc)

    next_time = previous_time + timedelta(minutes=15)
    batch_name = next_time.strftime("%Y%m%d%H%M%S")

    filename = f"{batch_name}.export.CSV.zip"
    url = BASE_URL + filename

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    zip_path = RAW_DIR / filename
    csv_path = STAGING_DIR / filename.removesuffix(".zip")

    if not zip_path.exists():
        request = Request(
            url,
            headers={"User-Agent": "EventPulse/1.0"}
        )

        with urlopen(request, timeout=60) as response:
            content = response.read(20 * 1024 * 1024 + 1)

        if len(content) > 20 * 1024 * 1024:
            raise ValueError("Download exceeds size limit")

        zip_path.write_bytes(content)

    with ZipFile(zip_path) as archive:
        if archive.testzip() is not None:
            raise ValueError("ZIP integrity validation failed")

        members = [
            item for item in archive.infolist()
            if not item.is_dir()
        ]

        if len(members) != 1:
            raise ValueError("Unexpected archive structure")

        if Path(members[0].filename).name != csv_path.name:
            raise ValueError("Unexpected CSV filename")

        if not csv_path.exists():
            with archive.open(members[0]) as source:
                with csv_path.open("wb") as target:
                    shutil.copyfileobj(source, target)

    print("SECOND GDELT BATCH")
    print("=" * 60)
    print(f"Filename: {csv_path.name}")
    print(f"Compressed size: {zip_path.stat().st_size:,} bytes")
    print(f"Extracted size: {csv_path.stat().st_size:,} bytes")
    print("ZIP integrity: PASSED")


if __name__ == "__main__":
    main()
