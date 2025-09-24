import json
import logging
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
import os

import requests

CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "uploads_logging_system_blueprint"

# Cache for recent data (in-memory)
_cache = {}
_cache_time = None
CACHE_TTL_SECONDS = 30

def get_blob_client():
    logging.info(f"Getting blob client ")
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        logging.error("Azure storage connection string not found")
        raise ValueError("Missing Azure storage connection string")
    
    try:
        blob_service = BlobServiceClient.from_connection_string(connect_str)
        container = blob_service.get_container_client(CONTAINER_NAME)
        return blob_service, container
    except Exception as e:
        logging.error(f"Error getting blob client: {str(e)}")
        raise

def get_blob_path(data_type, company=None, date_str=None):
    """Generate optimized blob paths"""
    if data_type == "recent":
        return f"{FOLDER_NAME}/recent/recent_flows.json"
    elif data_type == "pending":
        return f"{FOLDER_NAME}/pending/pending_flows.json"
    elif data_type == "all":
        return f"{FOLDER_NAME}/Uploads.json"  # Your original file
    elif data_type == "company":
        today = datetime.utcnow().strftime("%Y-%m-%d")
        return f"{FOLDER_NAME}/daily/{company}/{today}.json"

def load_json_from_blob(blob_path, default_value=None):
    """Load JSON from specific blob path"""
    try:
        _, container = get_blob_client()
        blob = container.get_blob_client(blob_path)
        download = blob.download_blob().readall()
        return json.loads(download)
    except Exception as e:
        logging.warning(f"Could not load {blob_path}: {str(e)}")
        return default_value or []

def save_json_to_blob(blob_path, data):
    """Save JSON to specific blob path"""
    try:
        _, container = get_blob_client()
        blob = container.get_blob_client(blob_path)
        blob.upload_blob(json.dumps(data, indent=2), overwrite=True)
        logging.info(f"Successfully saved to {blob_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving to {blob_path}: {str(e)}")
        return False

def load_logs():
    """Load logs - force fallback to main storage for now"""
    global _cache, _cache_time
    
    # For debugging - skip recent and go straight to main storage
    logging.info("Loading from main storage (debug mode)")
    
    try:
        main_path = get_blob_path("all")
        logging.info(f"Loading from path: {main_path}")
        
        all_logs = load_json_from_blob(main_path, [])
        logging.info(f"Loaded {len(all_logs)} logs from main storage")
        
        if not all_logs:
            logging.warning("Main storage returned empty array")
            return []
        
        # Return recent items
        recent_logs = sorted(all_logs, key=lambda x: x.get("createdAt", ""), reverse=True)[:100]
        logging.info(f"Returning {len(recent_logs)} recent logs")
        
        return recent_logs
        
    except Exception as e:
        logging.error(f"Error loading from main storage: {str(e)}")
        return []

def save_logs(logs):
    """Save logs to multiple locations for performance"""
    global _cache, _cache_time
    
    # Clear cache
    _cache = {}
    _cache_time = None
    
    success = True
    
    # 1. Save to main file (backward compatibility)
    if not save_json_to_blob(get_blob_path("all"), logs):
        success = False
    
    # 2. Save recent logs only (performance)
    recent_logs = sorted(logs, key=lambda x: x.get("createdAt", ""), reverse=True)[:100]
    if not save_json_to_blob(get_blob_path("recent"), recent_logs):
        success = False
    
    # 3. Save pending workflows only
    pending_logs = [log for log in logs if log.get("finalResult", {}).get("workflowStatus") == "pending"]
    if not save_json_to_blob(get_blob_path("pending"), pending_logs):
        success = False
    
    return success

def load_recent_logs(limit=50):
    """Load only recent logs (fastest)"""
    try:
        recent_logs = load_json_from_blob(get_blob_path("recent"), [])
        return recent_logs[:limit]
    except Exception as e:
        logging.error(f"Error loading recent logs: {str(e)}")
        return []

def load_pending_logs():
    """Load only pending logs (for timeout checking)"""
    try:
        return load_json_from_blob(get_blob_path("pending"), [])
    except Exception as e:
        logging.error(f"Error loading pending logs: {str(e)}")
        return []

def update_workflow_status(file_ref, new_status, update_data=None):
    """Update workflow status efficiently"""
    global _cache, _cache_time
    
    # Clear cache
    _cache = {}
    _cache_time = None
    
    updated = False
    
    # 1. Update in recent logs
    recent_logs = load_json_from_blob(get_blob_path("recent"), [])
    for i, log in enumerate(recent_logs):
        if log.get("fileRef") == file_ref:
            recent_logs[i]["finalResult"]["workflowStatus"] = new_status
            if new_status == "success":
                recent_logs[i]["finalResult"]["allStepsSucceeded"] = True
            if update_data:
                recent_logs[i].update(update_data)
            updated = True
            break
    
    if updated:
        save_json_to_blob(get_blob_path("recent"), recent_logs)
    
    # 2. Update in main logs
    all_logs = load_json_from_blob(get_blob_path("all"), [])
    for i, log in enumerate(all_logs):
        if log.get("fileRef") == file_ref:
            all_logs[i]["finalResult"]["workflowStatus"] = new_status
            if new_status == "success":
                all_logs[i]["finalResult"]["allStepsSucceeded"] = True
            if update_data:
                all_logs[i].update(update_data)
            break
    
    save_json_to_blob(get_blob_path("all"), all_logs)
    
    # 3. Update pending logs
    if new_status != "pending":
        pending_logs = load_json_from_blob(get_blob_path("pending"), [])
        pending_logs = [log for log in pending_logs if log.get("fileRef") != file_ref]
        save_json_to_blob(get_blob_path("pending"), pending_logs)
    
    return updated

def check_and_timeout_pending():
    """Check pending workflows and timeout if needed"""
    timeout_minutes = 5
    current_time = datetime.utcnow()
    timed_out = []
    
    pending_logs = load_pending_logs()
    active_pending = []
    
    for log in pending_logs:
        created_at = log.get("createdAt")
        if created_at:
            try:
                created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00')).replace(tzinfo=None)
                time_diff = current_time - created_time
                
                if time_diff.total_seconds() / 60 > timeout_minutes:
                    # Timeout this workflow
                    file_ref = log.get("fileRef")
                    timeout_data = {
                        "finalResult": {
                            **log.get("finalResult", {}),
                            "workflowStatus": "failed",
                            "allStepsSucceeded": False,
                            "timeoutAt": current_time.isoformat() + "Z",
                            "failureReason": f"Workflow timed out after {timeout_minutes} minutes"
                        }
                    }
                    
                    # Update final step
                    if "Steps" in log:
                        for step in log["Steps"]:
                            if "finalStep" in step:
                                step["finalStep"]["status"] = "failed"
                                step["finalStep"]["description"] = f"Timed out after {timeout_minutes} minutes"
                                step["finalStep"]["failedAt"] = current_time.isoformat() + "Z"
                                break
                    
                    update_workflow_status(file_ref, "failed", timeout_data)
                    timed_out.append({
                        "fileRef": file_ref,
                        "companyName": log.get("companyName"),
                        "timeoutDuration": str(time_diff)
                    })
                else:
                    active_pending.append(log)
            except Exception as e:
                logging.error(f"Error processing pending workflow: {str(e)}")
                active_pending.append(log)  # Keep it if we can't process
    
    # Update pending logs with only active ones
    if len(active_pending) != len(pending_logs):
        save_json_to_blob(get_blob_path("pending"), active_pending)
    
    return timed_out

    
def call_declaration_lookup_logic_app(commercial_ref):
    """Call Logic App to get declaration ID using clean commercial reference"""
    logic_app_url = "https://prod-190.westeurope.logic.azure.com:443/workflows/0905963c88a84e97937bc4dff939d065/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=1AiDHenDRlJscA9fckctAZG5kQMiU0IIcz0jC-gXoI8"
    
    payload = {"fileRef": commercial_ref}
    
    try:
        response = requests.post(logic_app_url, json=payload, timeout=30)
        response.raise_for_status()
        raw_result = response.json()
        
        # Extract declaration ID from Oracle response structure
        declaration_id = None
        try:
            table_data = raw_result.get("declarationId", {}).get("ResultSets", {}).get("Table1", [])
            if table_data and len(table_data) > 0:
                declaration_id = table_data[0].get("DECLARATIONID")
        except (KeyError, IndexError, TypeError):
            declaration_id = None
        
        # Return standardized format
        return {
            "found": declaration_id is not None,
            "declarationId": str(declaration_id) if declaration_id else None,
            "commercialReference": commercial_ref
        }
        
    except Exception as e:
        logging.error(f"Logic App call failed: {str(e)}")
        raise    
