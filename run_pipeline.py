import sys
import time

from ingest import extract_weather
from transform import build_gold, clean_weather

STAGES = [
    ("bronze", extract_weather.main),
    ("silver", clean_weather.main),
    ("gold", build_gold.main),
]


def main() -> int:
    start = time.perf_counter()
    for name, stage in STAGES:
        print(f"--- {name} ---")
        try:
            stage()
        except Exception as e:
            print(f"Pipeline failed in {name} stage: {e}")
            return 1
    print(f"Pipeline finished in {time.perf_counter() - start:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())