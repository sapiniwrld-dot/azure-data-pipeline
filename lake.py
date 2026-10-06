import io
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()


def upload_to_lake(local_path: Path, container: str, dataset: str) -> str:
    now = datetime.now(timezone.utc)
    lake_path = (
        f"{dataset}/year={now:%Y}/month={now:%m}/day={now:%d}/{local_path.name}"
    )
    service = DataLakeServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )
    file_client = service.get_file_system_client(container).get_file_client(lake_path)
    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)
    return f"{container}/{lake_path}"


def read_parquet_files(container: str, directory: str) -> list[tuple[str, pd.DataFrame]]:
    service = DataLakeServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )
    file_system = service.get_file_system_client(container)
    results = []
    for item in file_system.get_paths(path=directory, recursive=True):
        if item.name.endswith(".parquet"):
            data = file_system.get_file_client(item.name).download_file().readall()
            results.append((item.name, pd.read_parquet(io.BytesIO(data))))
    return results
