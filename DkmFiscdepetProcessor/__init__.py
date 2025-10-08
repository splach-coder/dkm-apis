import logging
import azure.functions as func
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os

# Blob Storage Configuration
CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "Fiscal-Representation"
STATE_BLOB_NAME = "fiscdebet_state.json"

def get_blob_client():
    logging.info("Getting blob client")
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing Azure storage connection string")
    
    blob_service = BlobServiceClient.from_connection_string(connect_str)
    container = blob_service.get_container_client(CONTAINER_NAME)
    return blob_service, container

def load_json_from_blob(blob_name):
    try:
        _, container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{blob_name}"
        blob_client = container.get_blob_client(blob_path)
        download = blob_client.download_blob().readall()
        return json.loads(download)
    except Exception as e:
        logging.warning(f"Could not load {blob_name}: {str(e)}")
        return None

def save_json_to_blob(blob_name, data):
    try:
        _, container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{blob_name}"
        blob_client = container.get_blob_client(blob_path)
        blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)
        logging.info(f"Successfully saved {blob_name}")
    except Exception as e:
        logging.error(f"Error saving {blob_name}: {str(e)}")
        raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method != "POST":
        return func.HttpResponse("Method not allowed", status_code=405)
    
    try:
        # 1. Get data from Logic Apps
        body = req.get_json()
        rows = body.get("Table1", [])
        
        if not rows:
            return func.HttpResponse(
                json.dumps({"success": True, "message": "No new data received"}),
                status_code=200,
                mimetype="application/json"
            )
        
        logging.info(f"Received {len(rows)} rows from Logic Apps")
        
        # 2. Find max INTERNFACTUURNUMMER
        max_id = max([row.get("INTERNFACTUURNUMMER", 0) for row in rows])
        logging.info(f"Max INTERNFACTUURNUMMER in this batch: {max_id}")
        
        # 3. Load previous state
        state = load_json_from_blob(STATE_BLOB_NAME) or {"lastProcessedId": 0}
        previous_id = state.get("lastProcessedId", 0)
        
        # 4. Update state
        state["lastProcessedId"] = max(max_id, previous_id)
        state["lastRun"] = datetime.utcnow().isoformat() + "Z"
        state["recordsProcessed"] = len(rows)
        
        # 5. Save updated state
        save_json_to_blob(STATE_BLOB_NAME, state)
        
        # 6. Return response
        return func.HttpResponse(
            json.dumps({
                "success": True,
                "maxIdProcessed": max_id,
                "previousId": previous_id,
                "recordsProcessed": len(rows)
            }),
            status_code=200,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
