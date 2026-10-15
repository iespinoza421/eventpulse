# Databricks notebook source

from datetime import datetime, timedelta, timezone

VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"
MAX_FILES_PER_RUN = 16
SAFETY_DELAY_MINUTES = 30

existing_files = sorted(
    file.name
    for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.endswith(".export.CSV")
    and len(file.name) == 25
    and file.name[:14].isdigit()
)

if not existing_files:
    raise ValueError("No existing GDELT files found")

existing_names = set(existing_files)

first_timestamp = datetime.strptime(
    existing_files[0][:14],
    "%Y%m%d%H%M%S"
).replace(tzinfo=timezone.utc)

latest_timestamp = datetime.strptime(
    existing_files[-1][:14],
    "%Y%m%d%H%M%S"
).replace(tzinfo=timezone.utc)

cutoff_timestamp = datetime.now(timezone.utc) - timedelta(
    minutes=SAFETY_DELAY_MINUTES
)

candidate_files = []
current_timestamp = first_timestamp

while current_timestamp <= cutoff_timestamp and len(candidate_files) < MAX_FILES_PER_RUN:
    file_name = current_timestamp.strftime("%Y%m%d%H%M%S") + ".export.CSV"

    if file_name not in existing_names:
        candidate_files.append(file_name)

    current_timestamp += timedelta(minutes=15)

print("EVENTPULSE — GAP-AWARE INCREMENTAL DISCOVERY")
print("=" * 60)
print(f"Existing files: {len(existing_files)}")
print(f"Latest stored file: {existing_files[-1]}")
print(f"Maximum downloads per run: {MAX_FILES_PER_RUN}")
print(f"Pending candidates: {len(candidate_files)}")

for file_name in candidate_files:
    print(f"PENDING: {file_name}")

print("Gap-aware discovery: PASSED")


# COMMAND ----------


import requests
from datetime import datetime, timezone

BASE_URL = "https://data.gdeltproject.org/gdeltv2"
MAX_MISSING_AGE_HOURS = 3

available_candidates = []
unavailable_candidates = []

print("EVENTPULSE — ORDERED SOURCE AVAILABILITY")
print("=" * 60)

for file_name in candidate_files:
    url = f"{BASE_URL}/{file_name}.zip"

    try:
        response = requests.head(
            url,
            allow_redirects=True,
            timeout=30
        )

        if response.status_code == 200:
            available_candidates.append(file_name)
            print(f"{file_name}: AVAILABLE")

        elif response.status_code == 404:
            unavailable_candidates.append(file_name)

            file_timestamp = datetime.strptime(
                file_name[:14],
                "%Y%m%d%H%M%S"
            ).replace(tzinfo=timezone.utc)

            missing_age_hours = (
                datetime.now(timezone.utc) - file_timestamp
            ).total_seconds() / 3600

            print(f"{file_name}: NOT FOUND")
            print(f"Missing interval age: {missing_age_hours:.2f} hours")

            if missing_age_hours >= MAX_MISSING_AGE_HOURS:
                raise ValueError(
                    f"Historical GDELT interval unavailable: {file_name}"
                )

            print("Recent interval not yet available. Retry on next run.")
            break

        else:
            raise ValueError(
                f"Unexpected HTTP {response.status_code}: {file_name}"
            )

    except requests.RequestException as error:
        raise RuntimeError(
            f"Source availability check failed: {file_name}: {error}"
        ) from error

print("=" * 60)
print(f"Available files: {len(available_candidates)}")
print(f"Unavailable files: {len(unavailable_candidates)}")
print("Ordered availability: PASSED")


# COMMAND ----------


import io
import zipfile
import requests
import hashlib

BASE_URL = "https://data.gdeltproject.org/gdeltv2"

validated_files = {}

print("EVENTPULSE — MULTI-FILE DOWNLOAD VALIDATION")
print("=" * 60)

for file_name in available_candidates:
    url = f"{BASE_URL}/{file_name}.zip"

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        members = archive.namelist()

        if len(members) != 1 or members[0] != file_name:
            raise ValueError(f"Unexpected ZIP contents: {file_name}")

        file_data = archive.read(file_name)

    lines = file_data.splitlines()

    if not lines:
        raise ValueError(f"Empty source file: {file_name}")

    invalid_rows = sum(
        len(line.split(b"\t")) != 61
        for line in lines
    )

    if invalid_rows > 0:
        raise ValueError(
            f"Invalid GDELT structure in {file_name}: {invalid_rows} rows"
        )

    validated_files[file_name] = {
        "data": file_data,
        "records": len(lines),
        "sha256": hashlib.sha256(file_data).hexdigest()
    }

    print(f"{file_name}: {len(lines):,} records — VALIDATED")

total_records = sum(
    item["records"]
    for item in validated_files.values()
)

print("=" * 60)
print(f"Validated files: {len(validated_files)}")
print(f"Total records: {total_records:,}")
print(f"Download validation: {len(validated_files) == len(available_candidates)}")

if len(validated_files) != len(available_candidates):
    raise ValueError("Not all GDELT files passed validation")


# COMMAND ----------


import os
import hashlib
import uuid

VOLUME_PATH = "/Volumes/workspace/eventpulse_bronze/raw_files"

stored_files = []
verified_existing_files = []

print("EVENTPULSE — SAFE MULTI-FILE STORAGE")
print("=" * 60)

for file_name, file_info in validated_files.items():
    destination = f"{VOLUME_PATH}/{file_name}"
    temporary_path = f"{VOLUME_PATH}/.{file_name}.{uuid.uuid4().hex}.tmp"

    if os.path.exists(destination):
        with open(destination, "rb") as existing_file:
            existing_hash = hashlib.sha256(
                existing_file.read()
            ).hexdigest()

        if existing_hash != file_info["sha256"]:
            raise ValueError(
                f"Existing file checksum mismatch: {file_name}"
            )

        verified_existing_files.append(file_name)
        print(f"{file_name}: ALREADY VERIFIED")
        continue

    try:
        with open(temporary_path, "xb") as output_file:
            output_file.write(file_info["data"])

        with open(temporary_path, "rb") as temporary_file:
            temporary_hash = hashlib.sha256(
                temporary_file.read()
            ).hexdigest()

        if temporary_hash != file_info["sha256"]:
            raise ValueError(
                f"Temporary file checksum mismatch: {file_name}"
            )

        if os.path.exists(destination):
            raise FileExistsError(
                f"Destination already exists: {file_name}"
            )

        os.rename(temporary_path, destination)

        with open(destination, "rb") as saved_file:
            saved_hash = hashlib.sha256(
                saved_file.read()
            ).hexdigest()

        if saved_hash != file_info["sha256"]:
            raise ValueError(
                f"Published file checksum mismatch: {file_name}"
            )

        stored_files.append(file_name)
        print(f"{file_name}: STORED AND VERIFIED")

    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)

print("=" * 60)
print(f"New files stored: {len(stored_files)}")
print(f"Previously verified files: {len(verified_existing_files)}")
print(f"Validated files: {len(validated_files)}")

storage_validation = (
    len(stored_files) + len(verified_existing_files)
    == len(validated_files)
)

print(f"Safe storage: {'PASSED' if storage_validation else 'FAILED'}")

if not storage_validation:
    raise ValueError("GDELT storage validation failed")
