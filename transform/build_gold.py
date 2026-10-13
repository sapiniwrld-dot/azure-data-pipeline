import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from config import load_cities
from lake import read_parquet_files, upload_to_lake

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
GOLD_DIR = DATA_DIR / "gold"
logger = logging.getLogger(__name__)


def load_silver() -> pd.DataFrame:
    frames = []
    for name, df in read_parquet_files("silver", "weather"):
        # weather_new_york_2026-10-10_1400 -> 2026-10-10_1400
        timestamp = "_".join(Path(name).stem.rsplit("_", 2)[1:])
        fetched_at = datetime.strptime(timestamp, "%Y-%m-%d_%H%M").replace(tzinfo=UTC)
        df["fetched_at"] = fetched_at
        # Silver files from before multi-city support are all New York.
        if "city" not in df.columns:
            df["city"] = "new_york"
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No silver files in the lake. Run silver first.")
    return pd.concat(frames, ignore_index=True)


def latest_hourly(silver: pd.DataFrame) -> pd.DataFrame:
    return (
        silver.sort_values("fetched_at")
        .drop_duplicates(subset=["city", "time"], keep="last")
        .sort_values(["city", "time"])
        .reset_index(drop=True)
    )


def daily(hourly: pd.DataFrame) -> pd.DataFrame:
    timezones = {c.name: c.timezone for c in load_cities()}
    frames = []
    for city, group in hourly.groupby("city"):
        # Skip cities that were removed from config.toml.
        if city not in timezones:
            continue
        local_time = group["time"].dt.tz_convert(timezones[city])
        frames.append(group.assign(date=local_time.dt.date))

    hourly = pd.concat(frames)
    result = hourly.groupby(["city", "date"]).agg(
        temp_min_c=("temperature_c", "min"),
        temp_max_c=("temperature_c", "max"),
        temp_mean_c=("temperature_c", "mean"),
        precipitation_mm=("precipitation_mm", "sum"),
        hours=("time", "count"),
    )
    return result.round(1).reset_index()


def main():
    hourly = latest_hourly(load_silver())
    result = daily(hourly)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = GOLD_DIR / "weather_daily.parquet"
    result.to_parquet(out_path, index=False)
    logger.info("Wrote %d daily rows to %s", len(result), out_path)
    lake_path = upload_to_lake(out_path, "gold", "weather_daily", partitioned=False)
    logger.info("Uploaded to %s", lake_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
