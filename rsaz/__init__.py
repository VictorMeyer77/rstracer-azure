import logging
import os
import socket
from datetime import datetime, timezone
from time import sleep

from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def list_directory(path, extension):
    return [entry.path for entry in os.scandir(path) if entry.is_file() and entry.name.endswith(extension)]


def destination_file_path(file_path, destination_directory):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    hostname = socket.gethostname()
    now = datetime.now(timezone.utc)
    year = f"year={now.year}"
    month = f"month={now.month}"
    day = f"day={now.day}"
    file_name = hostname + f"_{now.strftime('%Y%m%d%H%M%S')}.parquet"
    return os.path.join(destination_directory, table_name.replace("gold_", ""), year, month, day, file_name)


def azure_blob_service(config):
    connection_string = (
        f"DefaultEndpointsProtocol=https;AccountName={config['credentials']['account']}"
        f";AccountKey={config['credentials']['access_key']};EndpointSuffix=core.windows.net"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client


def copy_to_azure(blob, container, file_source, destination_path):
    blob_client = blob.get_blob_client(container=container, blob=destination_path)
    with open(file=file_source, mode="rb") as data:
        blob_client.upload_blob(data)
    logger.info(f"Copy {file_source} to {container}@{destination_path}")


def launch(config):

    logger.info(f"Start transfer {config['source']} to azure {config['storage']} every {config['frequency']} seconds")

    last_send = datetime.now().timestamp()
    blob_service_client = azure_blob_service(config)
    container = config["storage"]["container"]

    while True:
        try:
            if last_send + config["frequency"] < datetime.now().timestamp():
                source_files = list_directory(config["source"]["directory"], config["source"]["format"])
                for source_file in source_files:
                    destination_file = destination_file_path(source_file, config["storage"]["directory"])
                    copy_to_azure(blob_service_client, container, source_file, destination_file)
                    last_send = datetime.now().timestamp()
            sleep(1)
        except KeyboardInterrupt:
            logger.info("CTRL+C: Stop gracefully")
            break
        except Exception as e:
            logger.error(e)
            break
