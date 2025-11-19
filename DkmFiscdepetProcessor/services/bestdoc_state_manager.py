import json
import logging
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from typing import Dict
from ..models.response_model import PDFResponse # Import PDFResponse model

# --- New State Configuration ---
BESTDOC_CONTAINER_NAME = "document-intelligence"
BESTDOC_FOLDER_NAME = "Bestemmingsrapport"
BESTDOC_STATE_BLOB_NAME = "Bestdoc_state.json"

def get_blob_client():
    """Get blob storage container client using AzureWebJobsStorage."""
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        # In a real environment, this should raise an error. 
        # For local testing, you might use a dummy client or mock the env var.
        raise ValueError("Missing Azure storage connection string")
    
    blob_service = BlobServiceClient.from_connection_string(connect_str)
    return blob_service.get_container_client(BESTDOC_CONTAINER_NAME)


def get_bestdoc_state() -> Dict:
    """Reads and returns current Bestdoc blob state as a dict."""
    try:
        container = get_blob_client()
        blob_path = f"{BESTDOC_FOLDER_NAME}/{BESTDOC_STATE_BLOB_NAME}"
        blob_client = container.get_blob_client(blob_path)
        data = blob_client.download_blob().readall().decode("utf-8")
        return json.loads(data)
    except Exception as e:
        # Handle case where the blob does not exist yet
        logging.warning(f"Bestdoc state blob not found or failed to read: {str(e)}. Initializing default state.")
        now_iso = datetime.utcnow().isoformat() + "Z"
        return {
             "metadata": {
                "version": "1.0",
                "created": now_iso,
                "last_modified": now_iso,
                "description": "Tracks debet notes and their bestemmingsdocument generation status"
            },
            "statistics": {
                "total_records": 0,
                "pending_bestdocs": 0,
                "generated_bestdocs": 0,
                "last_5pm_run": None,
                "last_5pm_processed_count": 0
            },
            "records": [],
            "pending_by_client_month": {},
            "daily_runs": {}
        }


def save_bestdoc_state(state: Dict) -> None:
    """Writes the given state dictionary atomically to the Bestdoc blob."""
    container = get_blob_client()
    blob_path = f"{BESTDOC_FOLDER_NAME}/{BESTDOC_STATE_BLOB_NAME}"
    blob_client = container.get_blob_client(blob_path)
    
    # Update metadata before saving
    state["metadata"]["last_modified"] = datetime.utcnow().isoformat() + "Z"

    blob_client.upload_blob(json.dumps(state, indent=2), overwrite=True)
    logging.info("✅ Bestdoc state saved.")


def update_bestdoc_state(pdf_response: PDFResponse) -> None:
    """
    Adds a new record for a successfully processed Debenote to the Bestdoc state.
    """
    try:
        state = get_bestdoc_state()
        
        # Extract necessary data from PDFResponse metadata
        metadata = pdf_response.metadata
        internfactuurnummer = pdf_response.internfactuurnummer
        
        new_record = {
            "internfactuurnummer": internfactuurnummer,
            "klant": metadata.get("klant", ""),
            # The row date is YYYYMMDD, we need to store it as such for compatibility
            "datum": metadata.get("datum", "").replace("/", ""), 
            "added_at": datetime.utcnow().isoformat() + "Z",
            "bestdoc": False,
            "bestdoc_generated_at": None,
            "bestdoc_filename": None
        }

        # Check for existing record to prevent duplicates (optional, but robust)
        if any(r["internfactuurnummer"] == internfactuurnummer for r in state["records"]):
            logging.warning(f"Record with ID {internfactuurnummer} already exists in Bestdoc state. Skipping addition.")
            return

        # 1. Add new record
        state["records"].append(new_record)

        # 2. Update statistics
        state["statistics"]["total_records"] = len(state["records"])
        
        # 3. Save the state
        save_bestdoc_state(state)
        logging.info(f"✅ Added {internfactuurnummer} to Bestdoc state records.")

    except Exception as e:
        logging.error(f"❌ Error updating Bestdoc state for ID {pdf_response.internfactuurnummer}: {str(e)}")