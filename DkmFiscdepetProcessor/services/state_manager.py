import json
import logging
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from typing import List

CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "Fiscal-Representation"
STATE_BLOB_NAME = "fiscdebet_state.json"

def get_blob_client():
    """Get blob storage client"""
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing Azure storage connection string")
    
    blob_service = BlobServiceClient.from_connection_string(connect_str)
    container = blob_service.get_container_client(CONTAINER_NAME)
    return container


def get_max_id(rows: List[dict]) -> int:
    """
    Find maximum INTERNFACTUURNUMMER in batch
    
    Args:
        rows: List of SQL row dictionaries
        
    Returns:
        Maximum INTERNFACTUURNUMMER
    """
    if not rows:
        return 0
    
    return max([row.get("INTERNFACTUURNUMMER", 0) for row in rows])


def update_state(max_id: int, count: int):
    """
    Update fiscdebet_state.json in blob storage
    
    Args:
        max_id: Maximum INTERNFACTUURNUMMER processed
        count: Number of records processed
    """
    try:
        container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{STATE_BLOB_NAME}"
        blob_client = container.get_blob_client(blob_path)
        
        # Load existing state or create new
        try:
            download = blob_client.download_blob().readall()
            state = json.loads(download)
        except:
            state = {"lastProcessedId": 0}
        
        # Update state
        state["lastProcessedId"] = max(max_id, state.get("lastProcessedId", 0))
        state["lastRun"] = datetime.utcnow().isoformat() + "Z"
        state["recordsProcessed"] = count
        
        # Save state
        blob_client.upload_blob(json.dumps(state, indent=2), overwrite=True)
        logging.info(f"✅ Updated state: lastProcessedId = {state['lastProcessedId']}")
        
    except Exception as e:
        logging.error(f"❌ Failed to update state: {str(e)}")
        # Don't raise - state update is not critical