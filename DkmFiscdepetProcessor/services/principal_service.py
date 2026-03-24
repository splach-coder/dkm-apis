import json
import logging
import os
from azure.storage.blob import BlobServiceClient

CONTAINER_NAME = "document-intelligence"
PRINCIPALS_BLOB_NAME = "FiscalRepresentationWebApp/principals.json"

_principals_cache = None

def get_principals_list() -> list:
    """Fetch the list of principals from Blob Storage and cache it."""
    global _principals_cache
    if _principals_cache is not None:
        return _principals_cache
    
    try:
        connect_str = os.getenv("AzureWebJobsStorage")
        if not connect_str:
            logging.error("Missing Azure storage connection string")
            return []
        
        blob_service = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(PRINCIPALS_BLOB_NAME)
        
        data = blob_client.download_blob().readall().decode("utf-8")
        parsed_data = json.loads(data)
        
        principals = [str(p).upper() for p in parsed_data.get("principals", [])]
        _principals_cache = principals
        return principals
    except Exception as e:
        logging.error(f"Failed to fetch principals.json from blob storage: {str(e)}")
        return []
