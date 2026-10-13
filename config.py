import tomllib
from dataclasses import dataclass
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "config.toml"


@dataclass(frozen=True)
class City:
    name: str
    latitude: float
    longitude: float
    timezone: str


def load_cities(path: Path = CONFIG_PATH) -> list[City]:
    with open(path, "rb") as f:
        config = tomllib.load(f)
    return [City(**c) for c in config["cities"]]
