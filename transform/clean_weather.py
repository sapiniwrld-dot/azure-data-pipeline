import json
import logging
import os
from pathlib import Path

import pandas as pd

from lake import upload_to_lake

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"
logger = logging.getLogger(__name__)


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


def city_from_path(raw_path: Path) -> str:
    # weather_new_york_2026-10-10_1400.json -> new_york
    return raw_path.stem.removeprefix("weather_").rsplit("_", 2)[0]


def transform(raw: dict, city: str) -> pd.DataFrame:
    df = pd.DataFrame(raw["hourly"])
    df = df.rename(
        columns={
            "temperature_2m": "temperature_c",
            "precipitation": "precipitation_mm",
        }
    )
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df["latitude"] = raw["latitude"]
    df["city"] = city
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
        logger.info("No new raw files to process.")
        return

    for raw_path in raw_files:
        with open(raw_path) as f:
            raw = json.load(f)

        df = transform(raw, city_from_path(raw_path))
        validate(df)

        # Stage the file and only move it into SILVER_DIR after a successful
        # upload, so a failed upload leaves the raw file pending for retry.
        staging_dir = SILVER_DIR / ".staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_path = staging_dir / raw_path.with_suffix(".parquet").name
        out_path = SILVER_DIR / staged_path.name

        df.to_parquet(staged_path, index=False)
        logger.info("Wrote %d rows to %s", len(df), staged_path)
        lake_path = upload_to_lake(staged_path, "silver", "weather")
        logger.info("Uploaded to %s", lake_path)
        staged_path.replace(out_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
