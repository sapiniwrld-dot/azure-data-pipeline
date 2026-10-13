# Azure Weather Data Pipeline

A small data engineering project that pulls hourly weather forecasts for several cities from the [Open-Meteo API](https://open-meteo.com/), cleans them, and builds a daily summary table in Azure Data Lake Storage. It runs every 6 hours as an Azure Function.

## Architecture

The pipeline follows the **medallion** pattern (bronze → silver → gold):

```
Open-Meteo API
     │
     ▼
 bronze   raw JSON, one file per city per run         ingest/extract_weather.py
     │
     ▼
 silver   cleaned, typed, validated Parquet           transform/clean_weather.py
     │
     ▼
 gold     one row per city per local calendar day     transform/build_gold.py
```

`run_pipeline.py` runs the three stages in order. `function_app.py` wraps it in an Azure Functions timer trigger (`0 0 */6 * * *`).

### Data lake layout

| Container | Path | Format |
|-----------|------|--------|
| `bronze` | `weather/year=YYYY/month=MM/day=DD/weather_<city>_<timestamp>.json` | Raw API response |
| `silver` | `weather/year=YYYY/month=MM/day=DD/weather_<city>_<timestamp>.parquet` | Columns: `time` (UTC), `temperature_c`, `precipitation_mm`, `latitude`, `longitude`, `city` |
| `gold` | `weather_daily/weather_daily.parquet` | Columns: `city`, `date`, `temp_min_c`, `temp_max_c`, `temp_mean_c`, `precipitation_mm`, `hours` |

Gold is rebuilt from all silver files each run. When forecasts for the same hour overlap, the most recently fetched value wins. Hours are grouped into days using each city's own timezone.

## Configuration

Cities live in `config.toml`. Add or remove a `[[cities]]` block to change them:

```toml
[[cities]]
name = "london"
latitude = 51.51
longitude = -0.13
timezone = "Europe/London"
```

## Running locally

Requires Python 3.13+ and an Azure Storage account with hierarchical namespace enabled and `bronze`, `silver` and `gold` containers.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# .env (not committed)
echo 'AZURE_STORAGE_CONNECTION_STRING=<your connection string>' > .env

python run_pipeline.py
```

Local working files go to `./data` (override with `PIPELINE_DATA_DIR`).

## Deploying to Azure Functions

The app runs on a Flex Consumption plan (Python 3.13). Set the connection string as an app setting, then publish:

```bash
az functionapp config appsettings set -g <resource-group> -n <function-app> \
  --settings AZURE_STORAGE_CONNECTION_STRING="<your connection string>"

func azure functionapp publish <function-app> --python
```

Stage messages use Python `logging`, so they show up in Application Insights under `traces`.

## Development

Linting and formatting use [ruff](https://docs.astral.sh/ruff/):

```bash
ruff check .
ruff format .
```

## Tech stack

Python · pandas · PyArrow · Azure Data Lake Storage Gen2 · Azure Functions · Application Insights · ruff
