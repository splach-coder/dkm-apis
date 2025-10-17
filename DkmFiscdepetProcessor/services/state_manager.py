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
    """Get blob storage container client."""
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing Azure storage connection string")
    
    blob_service = BlobServiceClient.from_connection_string(connect_str)
    return blob_service.get_container_client(CONTAINER_NAME)


def get_max_id(rows: List[dict]) -> int:
    """Find maximum INTERNFACTUURNUMMER in a batch."""
    if not rows:
        return 0
    return max([row.get("INTERNFACTUURNUMMER", 0) for row in rows])


def get_state() -> dict:
    """Reads and returns current blob state as a dict."""
    try:
        container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{STATE_BLOB_NAME}"
        blob_client = container.get_blob_client(blob_path)
        data = blob_client.download_blob().readall().decode("utf-8")
        return json.loads(data)
    except Exception:
        return {"lastProcessedId": 0, "pendingIds": [], "pendingCreated": {}}


def save_state(state: dict) -> None:
    """Writes the given state dictionary atomically to the blob."""
    container = get_blob_client()
    blob_path = f"{FOLDER_NAME}/{STATE_BLOB_NAME}"
    blob_client = container.get_blob_client(blob_path)
    blob_client.upload_blob(json.dumps(state, indent=2), overwrite=True)
    logging.info(f"✅ Blob state saved: lastProcessedId={state.get('lastProcessedId')}, pending={state.get('pendingIds')}")


def update_state(processed_ids: list[int], new_max_id: int) -> None:
    """
    Updates blob state after a successful processing run.
    - Adds missing IDs between lastProcessedId and new_max_id to pendingIds.
    - Removes IDs that were just processed from pendingIds.
    - Updates lastProcessedId and lastRun atomically.
    """
    import datetime

    state = get_state()
    old_last = state.get("lastProcessedId", 0)
    old_pending = set(state.get("pendingIds", []))
    processed = set(processed_ids or [])

    # Compute new missing IDs (gaps between old_last and new_max)
    missing_between = set(range(old_last + 1, new_max_id + 1)) - processed

    # Merge pending lists
    new_pending = (old_pending | missing_between) - processed

    # Optional: add timestamps for new pending entries
    pending_created = state.get("pendingCreated", {})
    now_iso = datetime.datetime.utcnow().isoformat() + "Z"
    for mid in missing_between:
        pending_created[str(mid)] = now_iso
    for pid in processed:
        pending_created.pop(str(pid), None)  # remove if processed

    # Update state
    state["lastProcessedId"] = max(old_last, new_max_id)
    state["pendingIds"] = sorted(list(new_pending))
    state["pendingCreated"] = pending_created
    state["lastRun"] = now_iso
    state["recordsProcessed"] = len(processed)

    save_state(state)
