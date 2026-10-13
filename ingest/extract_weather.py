import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import requests

from config import City, load_cities
from lake import upload_to_lake

API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = "temperature_2m,precipitation"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
logger = logging.getLogger(__name__)


def fetch_weather(city: City) -> dict:
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": HOURLY_VARIABLES,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(data: dict, city_name: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d_%H%M")
    path = RAW_DIR / f"weather_{city_name}_{timestamp}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path


def main():
    for city in load_cities():
        data = fetch_weather(city)
        path = save_raw(data, city.name)
        logger.info(
            "Saved %d hourly records for %s to %s",
            len(data["hourly"]["time"]),
            city.name,
            path,
        )
        lake_path = upload_to_lake(path, "bronze", "weather")
        logger.info("Uploaded to %s", lake_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
