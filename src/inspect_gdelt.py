
from pathlib import Path
from collections import Counter
import csv


ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = ROOT / "data" / "staging"
EXPECTED_COLUMNS = 61


def main():
    files = sorted(STAGING_DIR.glob("*.export.CSV"))

    if not files:
        raise FileNotFoundError("No GDELT Events CSV files found")

    for path in files:
        total_rows = 0
        column_counts = Counter()
        samples = []

        with path.open("r", encoding="utf-8", errors="replace", newline="") as file:
            reader = csv.reader(file, delimiter="\t")

            for row in reader:
                total_rows += 1
                column_counts[len(row)] += 1

                if len(samples) < 3:
                    samples.append(row)

        print("\nGDELT DATA PROFILE")
        print("=" * 65)
        print(f"File: {path.name}")
        print(f"Total records: {total_rows:,}")
        print(f"Column distribution: {dict(column_counts)}")

        invalid_rows = sum(
            count
            for columns, count in column_counts.items()
            if columns != EXPECTED_COLUMNS
        )

        print(f"Invalid column-count rows: {invalid_rows:,}")

        print("\nSAMPLE RECORDS")
        for index, row in enumerate(samples, start=1):
            print(f"\nRecord {index}")
            print(f"GLOBALEVENTID: {row[0]}")
            print(f"SQLDATE: {row[1]}")
            print(f"EventCode: {row[26]}")
            print(f"EventRootCode: {row[28]}")
            print(f"ActionGeo_CountryCode: {row[51]}")
            print(f"DATEADDED: {row[59]}")

        if invalid_rows:
            raise ValueError("Found records with unexpected column counts")

        print("\nSchema validation: PASSED")


if __name__ == "__main__":
    main()
