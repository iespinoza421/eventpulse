
from pathlib import Path
import csv


ROOT = Path(__file__).resolve().parents[1]

INPUT_FILE = ROOT / "data" / "raw" / "FIPS.country.txt"
OUTPUT_FILE = ROOT / "data" / "staging" / "fips_country_lookup.csv"


def main():
    countries = {}

    with INPUT_FILE.open("r", encoding="utf-8-sig") as file:
        for line_number, line in enumerate(file, start=1):
            parts = line.strip().split(maxsplit=1)

            if not parts:
                continue

            if len(parts) != 2:
                raise ValueError(
                    f"Invalid country mapping at line {line_number}"
                )

            code, name = parts

            if code in countries:
                raise ValueError(f"Duplicate FIPS code: {code}")

            countries[code] = name

    if not countries:
        raise ValueError("Country lookup is empty")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as file:
        writer = csv.writer(file)

        writer.writerow(["country_code", "country_name"])

        for code, name in sorted(countries.items()):
            writer.writerow([code, name])

    print("EVENTPULSE COUNTRY LOOKUP")
    print("=" * 60)
    print(f"Reference entries: {len(countries)}")
    print(f"Output: {OUTPUT_FILE}")
    print("Preparation: PASSED")


if __name__ == "__main__":
    main()
