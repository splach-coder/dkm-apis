import json
import logging
from azure.storage.blob import BlobServiceClient
import os

CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "uploads_logging_system_blueprint"

def get_blob_client(company):
    logging.info(f"Getting blob client for company: {company}")
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        logging.error("Azure storage connection string not found")
        raise ValueError("Missing Azure storage connection string")
    
    try:
        blob_service = BlobServiceClient.from_connection_string(connect_str)
        container = blob_service.get_container_client(CONTAINER_NAME)
        blob_path = f"{FOLDER_NAME}/{company}.json"
        logging.info(f"Accessing blob at path: {blob_path}")
        blob = container.get_blob_client(blob_path)
        return blob, container
    except Exception as e:
        logging.error(f"Error getting blob client: {str(e)}")
        raise

def load_logs(company):
    logging.info(f"Loading logs for company: {company}")
    blob, _ = get_blob_client(company)

    try:
        download = blob.download_blob().readall()
        logs = json.loads(download)
        logging.info(f"Successfully loaded {len(logs)} log entries")
        return logs
    except Exception as e:
        logging.warning(f"Error loading logs for {company}: {str(e)}")
        return []

def save_logs(company, logs):
    logging.info(f"Saving {len(logs)} logs for company: {company}")
    blob, _ = get_blob_client(company)
    
    try:
        blob.upload_blob(json.dumps(logs, indent=2), overwrite=True)
        logging.info(f"Successfully saved logs for {company}")
    except Exception as e:
        logging.error(f"Error saving logs for {company}: {str(e)}")
        raise
