import json
from pathlib import Path

import pandas as pd
from lake import upload_to_lake

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def pending_raw_files() -> list[Path]:
    files = sorted(RAW_DIR.glob("weather_*.json"))
    pending = []
    for f in files:
        silver_file = SILVER_DIR / f.with_suffix(".parquet").name
        if not silver_file.exists():
            pending.append(f)
    return pending
    

def transform(raw: dict) -> pd.DataFrame:
    df = pd.DataFrame(raw["hourly"])
    df = df.rename(columns={
        "temperature_2m": "temperature_c",
        "precipitation": "precipitation_mm",
    })
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df["latitude"] = raw["latitude"]
    df["longitude"] = raw["longitude"]
    return df.drop_duplicates(subset="time")


def validate(df: pd.DataFrame) -> None:
    assert not df.empty, "No rows"
    assert df["time"].notna().all(), "Null timestamps"
    assert df["time"].is_unique, "Duplicate timestamps"
    assert df["temperature_c"].dropna().between(-60, 60).all(), "Temperature out of range"
    assert (df["precipitation_mm"].dropna() >= 0).all(), "Negative precipitation"
def main():
    raw_files = pending_raw_files()
    if not raw_files:
        print("No new raw files to process.")
        return

    for raw_path in raw_files:
        with open(raw_path) as f:
            raw = json.load(f)

        df = transform(raw)
        validate(df)

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        out_path = SILVER_DIR / raw_path.with_suffix(".parquet").name
        df.to_parquet(out_path, index=False)
        print(f"Wrote {len(df)} rows to {out_path}")
        lake_path = upload_to_lake(out_path, "silver", "weather")
        print(f"Uploaded to {lake_path}")


if __name__ == "__main__":
    main()