import json
import logging
import os
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
import azure.functions as func

CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "uploads_logging_system_blueprint"


def get_blob_service():
    connect_str = os.getenv("AzureWebJobsStorage")
    return BlobServiceClient.from_connection_string(connect_str)


def get_blob_client(blob_path):
    blob_service = get_blob_service()
    container = blob_service.get_container_client(CONTAINER_NAME)
    return container.get_blob_client(blob_path), container


def read_json():
    blob_path = f"{FOLDER_NAME}/reports/reports.json"
    blob, _ = get_blob_client(blob_path)
    try:
        content = blob.download_blob().readall()
        return json.loads(content)
    except:
        return []


def write_json(data):
    logging.error("Writing JSON data to blob")
    blob_path = f"{FOLDER_NAME}/reports/reports.json"
    blob, _ = get_blob_client(blob_path)
    blob.upload_blob(json.dumps(data, indent=2), overwrite=True)


def upload_file_to_blob(file, report_id):
    filename = file.filename
    blob_path = f"{FOLDER_NAME}/reports/files/{report_id}/{filename}"
    blob, _ = get_blob_client(blob_path)

    blob.upload_blob(file.stream.read(), overwrite=True, content_settings=ContentSettings(content_type=file.content_type))
    return blob.url


def generate_report(data, user_email):
    report_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"

    report = {
        "id": report_id,
        "reporter": data.get("reporter"),
        "email": user_email,
        "company": data.get("company"),
        "flow": data.get("flow"),
        "issue": data.get("issue"),  # markdown content
        "files": data.get("files", []),  # list of URLs
        "status": "open",
        "comment": "",
        "created_at": now
    }
    return report