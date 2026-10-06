import os
from datetime import datetime, timezone
from pathlib import Path

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