import json
import os
from pathlib import Path

import pandas as pd
from lake import upload_to_lake

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"

class ValidationError(Exception):
     pass


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
    if df.empty:
        raise ValidationError("No rows")

    problems = []
    if df["time"].isna().any():
        problems.append("Null timestamps")
    if not df["time"].is_unique:
        problems.append("Duplicate timestamps")
    if not df["temperature_c"].dropna().between(-60, 60).all():
        problems.append("Temperature out of range")
    if (df["precipitation_mm"].dropna() < 0).any():
        problems.append("Negative precipitation")

    if problems:
        raise ValidationError("; ".join(problems))
    
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

        # Stage the file and only move it into SILVER_DIR after a successful
        # upload, so a failed upload leaves the raw file pending for retry.
        staging_dir = SILVER_DIR / ".staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_path = staging_dir / raw_path.with_suffix(".parquet").name
        out_path = SILVER_DIR / staged_path.name

        df.to_parquet(staged_path, index=False)
        print(f"Wrote {len(df)} rows to {staged_path}")
        lake_path = upload_to_lake(staged_path, "silver", "weather")
        print(f"Uploaded to {lake_path}")
        staged_path.replace(out_path)


if __name__ == "__main__":
    main()