import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from lake import upload_to_lake

API_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": 40.71,
    "longitude": -74.01,
    "hourly": "temperature_2m,precipitation",
}

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def fetch_weather() -> dict:
    response = requests.get(API_URL, params=PARAMS, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(data: dict) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    path = RAW_DIR / f"weather_{timestamp}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path


def main():
    data = fetch_weather()
    path = save_raw(data)
    print(f"Saved {len(data['hourly']['time'])} hourly records to {path}")
    lake_path = upload_to_lake(path, "bronze", "weather")
    print(f"Uploaded to {lake_path}")

if __name__ == "__main__":
    main()
