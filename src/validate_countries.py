
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOOKUP_FILE = ROOT / "data" / "raw" / "FIPS.country.txt"

COUNTRY_CODES = [
    "US", "NI", "CA", "UK", "AS",
    "IS", "EG", "IN", "RS", "TU",
    "CH", "GR", "GH", "UP"
]


def load_countries(path):
    countries = {}

    with path.open("r", encoding="utf-8-sig") as file:
        for line in file:
            parts = line.strip().split(maxsplit=1)

            if len(parts) != 2:
                continue

            code, name = parts
            countries[code] = name

    return countries


def main():
    countries = load_countries(LOOKUP_FILE)

    print("GDELT FIPS COUNTRY VALIDATION")
    print("=" * 60)
    print(f"Reference entries: {len(countries)}")

    missing = []

    for code in COUNTRY_CODES:
        name = countries.get(code)

        if name is None:
            missing.append(code)
            name = "NOT FOUND"

        print(f"{code:5} {name}")

    print(f"\nMissing reference codes: {len(missing)}")

    if missing:
        raise ValueError(f"Unknown FIPS codes: {missing}")

    print("Country lookup validation: PASSED")


if __name__ == "__main__":
    main()
