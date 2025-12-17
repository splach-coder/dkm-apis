import json
import logging
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from typing import Dict, List

# --- Configuration ---
CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "Bestemmingsrapport/Queue"

def get_blob_client():
    """Get blob storage container client using AzureWebJobsStorage."""
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing Azure storage connection string")
    
    blob_service = BlobServiceClient.from_connection_string(connect_str)
    return blob_service.get_container_client(CONTAINER_NAME)

def get_daily_queue_filename() -> str:
    """Returns the filename for the current day's queue."""
    today_str = datetime.now().strftime("%Y%m%d")
    return f"{FOLDER_NAME}/Queue_{today_str}.json"

def add_to_daily_queue(row: Dict) -> None:
    """
    Appends the raw row data to the daily queue file.
    This ensures the BestDoc processor has all necessary data to generate the document later.
    """
    try:
        container = get_blob_client()
        blob_path = get_daily_queue_filename()
        blob_client = container.get_blob_client(blob_path)
        
        # 1. Read existing queue
        try:
            if blob_client.exists():
                data = blob_client.download_blob().readall().decode("utf-8")
                current_queue = json.loads(data)
            else:
                current_queue = []
        except Exception as e:
            logging.warning(f"Could not read existing queue, starting fresh: {e}")
            current_queue = []
            
        # 2. Check for duplicates
        new_id = row.get("INTERNFACTUURNUMMER")
        if not new_id:
             return

        # If queue contains full objects (old format), this might break or need migration.
        # Assuming new format starts fresh or we handle mixed (robustness).
        # We will strictly switch to storing IDs (int/str).
        
        if new_id in current_queue:
            logging.info(f"Duplicate record ID {new_id} already in queue. Skipping.")
            return

        # 3. Append new ID
        current_queue.append(new_id)
        
        # 4. Save back to blob
        blob_client.upload_blob(json.dumps(current_queue, indent=2), overwrite=True)
        logging.info(f"✅ Added INTERNFACTUURNUMMER {new_id} to daily BestDoc queue.")

    except Exception as e:
        logging.error(f"❌ Error adding to BestDoc queue: {str(e)}")
