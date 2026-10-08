import logging
import sys
import time

from ingest import extract_weather
from transform import build_gold, clean_weather

logger = logging.getLogger(__name__)

STAGES = [
    ("bronze", extract_weather.main),
    ("silver", clean_weather.main),
    ("gold", build_gold.main),
]


def main() -> int:
    start = time.perf_counter()
    for name, stage in STAGES:
        logger.info("--- %s ---", name)
        try:
            stage()
        except Exception:
            logger.exception("Stage %s failed", name)
            return 1

    logger.info("Pipeline completed in %.2f seconds", time.perf_counter() - start)
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    sys.exit(main())
