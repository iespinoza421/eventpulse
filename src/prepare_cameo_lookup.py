
from pathlib import Path
import csv
import re


ROOT = Path(__file__).resolve().parents[1]

INPUT_FILE = ROOT / "data" / "raw" / "CAMEO.eventcodes.txt"
OUTPUT_FILE = ROOT / "data" / "staging" / "cameo_root_lookup.csv"


def main():
    categories = {}

    with INPUT_FILE.open("r", encoding="utf-8-sig") as file:
        next(file)  # Skip header

        for line_number, line in enumerate(file, start=2):
            parts = line.strip().split(maxsplit=1)

            if not parts:
                continue

            if len(parts) != 2:
                raise ValueError(
                    f"Invalid CAMEO record at line {line_number}"
                )

            code, description = parts

            # Keep only two-digit root categories
            if not re.fullmatch(r"\d{2}", code):
                continue

            if code in categories:
                raise ValueError(
                    f"Duplicate CAMEO root code: {code}"
                )

            categories[code] = description

    expected_codes = {
        f"{code:02d}"
        for code in range(1, 21)
    }

    if set(categories) != expected_codes:
        raise ValueError(
            "CAMEO root categories do not match 01-20. "
            f"Missing: {sorted(expected_codes - set(categories))}. "
            f"Unexpected: {sorted(set(categories) - expected_codes)}"
        )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(["event_root_code", "category_name"])

        for code, description in sorted(categories.items()):
            writer.writerow([code, description])

    print("EVENTPULSE — CAMEO ROOT LOOKUP")
    print("=" * 60)
    print(f"Root categories: {len(categories)}")
    print(f"Output: {OUTPUT_FILE}")
    print("CAMEO preparation: PASSED")


if __name__ == "__main__":
    main()
