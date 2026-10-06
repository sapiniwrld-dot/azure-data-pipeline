import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from lake import read_parquet_files, upload_to_lake

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
GOLD_DIR = DATA_DIR / "gold"
TIMEZONE = "America/New_York"

def load_silver() -> pd.DataFrame:
    frames = []
    for name, df in read_parquet_files("silver", "weather"):
        timestamp = Path(name).stem.removeprefix("weather_")
        fetched_at = datetime.strptime(timestamp, "%Y-%m-%d_%H%M")
        df["fetched_at"] = fetched_at.replace(tzinfo=timezone.utc)
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No silver files in the lake. Run silver first.")
    return pd.concat(frames, ignore_index=True)


def latest_hourly(silver: pd.DataFrame) -> pd.DataFrame:
    return (
        silver.sort_values("fetched_at")
        .drop_duplicates(subset="time", keep="last")
        .sort_values("time")
        .reset_index(drop=True)
    )


def daily(hourly: pd.DataFrame) -> pd.DataFrame:
    local_time = hourly["time"].dt.tz_convert(TIMEZONE)
    hourly = hourly.assign(date=local_time.dt.date)
    result = hourly.groupby("date").agg(
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
    print(f"Wrote {len(result)} daily rows to {out_path}")
    lake_path = upload_to_lake(out_path, "gold", "weather_daily")
    print(f"Uploaded to {lake_path}")


if __name__ == "__main__":
    main()