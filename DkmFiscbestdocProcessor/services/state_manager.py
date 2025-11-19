import json
import logging
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from typing import List, Dict

CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "Bestemmingsrapport"
STATE_BLOB_NAME = "Bestdoc_state.json"


def get_blob_client():
    """Get blob storage container client."""
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing Azure storage connection string")
    
    blob_service = BlobServiceClient.from_connection_string(connect_str)
    return blob_service.get_container_client(CONTAINER_NAME)


def get_state() -> dict:
    """Reads and returns current blob state as a dict."""
    try:
        container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{STATE_BLOB_NAME}"
        blob_client = container.get_blob_client(blob_path)
        data = blob_client.download_blob().readall().decode("utf-8")
        return json.loads(data)
    except Exception:
        return {
            "metadata": {
                "version": "1.0",
                "created": datetime.utcnow().isoformat() + "Z",
                "last_modified": datetime.utcnow().isoformat() + "Z",
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


def save_state(state: dict) -> None:
    """Writes the given state dictionary atomically to the blob."""
    state["metadata"]["last_modified"] = datetime.utcnow().isoformat() + "Z"
    container = get_blob_client()
    blob_path = f"{FOLDER_NAME}/{STATE_BLOB_NAME}"
    blob_client = container.get_blob_client(blob_path)
    blob_client.upload_blob(json.dumps(state, indent=2), overwrite=True)
    logging.info(f"✅ State saved: {len(state.get('records', []))} records, {len(state.get('pending_by_client_month', {}))} pending groups")


def generate_client_month_key(klant: str, datum: str) -> str:
    """Generate client-month key: KLANT_YYYYMM from YYYYMMDD format"""
    if len(datum) == 8 and datum.isdigit():
        # Format: YYYYMMDD -> extract YYYYMM
        year_month = datum[:6]
    else:
        logging.warning(f"Unexpected date format: {datum}")
        year_month = datum[:6] if len(datum) >= 6 else datum
    
    clean_klant = klant.replace(" ", "").replace("-", "").replace("'", "").upper()
    return f"{clean_klant}_{year_month}"


def add_records_to_state(incoming_data: List[Dict]) -> None:
    """Add new records to state and pending_by_client_month with FULL TABLE DATA"""
    if not incoming_data:
        return
        
    state = get_state()
    now_iso = datetime.utcnow().isoformat() + "Z"
    
    existing_ids = {record["internfactuurnummer"] for record in state.get("records", [])}
    new_records = []
    pending_by_client_month = state.get("pending_by_client_month", {})
    
    for record in incoming_data:
        intern_id = record.get("INTERNFACTUURNUMMER")
        if intern_id and intern_id not in existing_ids:
            # Add to records array
            new_records.append({
                "internfactuurnummer": intern_id,
                "klant": record.get("KLANT", ""),
                "datum": record.get("DATUM", ""),
                "added_at": now_iso,
                "bestdoc": False,
                "bestdoc_generated_at": None,
                "bestdoc_filename": None
            })
            
            # Generate client_month_key from YYYYMMDD format
            klant = record.get("KLANT", "")
            datum = record.get("DATUM", "")
            client_month_key = generate_client_month_key(klant, datum)
            
            if client_month_key not in pending_by_client_month:
                pending_by_client_month[client_month_key] = []
            
            # Store COMPLETE TABLE DATA - exactly what we need for PDF generation
            pending_object = {
                "id": intern_id,
                "INTERNFACTUURNUMMER": intern_id,
                "PROCESSFACTUURNUMMER": record.get("PROCESSFACTUURNUMMER"),
                "RELATIECODE_KLANT": record.get("RELATIECODE_KLANT", ""),
                "REFERENTIE_KLANT": record.get("REFERENTIE_KLANT", ""),
                "RELATIECODE_LEVERANCIER": record.get("RELATIECODE_LEVERANCIER", ""),
                "DATUM": record.get("DATUM"),  # Keep YYYYMMDD format
                "KLANT": record.get("KLANT"),
                "CLIENT_NAAM": record.get("CLIENT_NAAM"),
                "CLIENT_STRAAT_EN_NUMMER": record.get("CLIENT_STRAAT_EN_NUMMER"),
                "CLIENT_POSTCODE": record.get("CLIENT_POSTCODE"),
                "CLIENT_STAD": record.get("CLIENT_STAD"),
                "CLIENT_LANDCODE": record.get("CLIENT_LANDCODE"),
                "CLIENT_PLDA_OPERATORIDENTITY": record.get("CLIENT_PLDA_OPERATORIDENTITY"),
                "CLIENT_LANGUAGE": record.get("CLIENT_LANGUAGE"),
                "MRN": record.get("MRN"),
                "DECLARATIONID": record.get("DECLARATIONID"),
                "EXPORTERNAME": record.get("EXPORTERNAME"),
                "LINE_ITEMS": record.get("LINE_ITEMS")  # Keep original JSON string with FULL data
            }
            
            # Check if ID already in group
            existing_ids_in_group = [obj["id"] for obj in pending_by_client_month[client_month_key] if isinstance(obj, dict)]
            
            if intern_id not in existing_ids_in_group:
                pending_by_client_month[client_month_key].append(pending_object)
                logging.info(f"✅ Added record {intern_id} to group {client_month_key} with FULL table data")
    
    if new_records:
        state.setdefault("records", []).extend(new_records)
        state["pending_by_client_month"] = pending_by_client_month
        
        # Update statistics
        stats = state.get("statistics", {})
        stats["total_records"] = len(state["records"])
        stats["pending_bestdocs"] = sum(1 for r in state["records"] if not r["bestdoc"])
        state["statistics"] = stats
        
        save_state(state)
        logging.info(f"✅ Added {len(new_records)} new records with complete table data")


def get_unprocessed_pending_groups() -> Dict[str, List[Dict]]:
    """Get pending groups that contain only unprocessed records (bestdoc=false)"""
    state = get_state()
    pending_groups = state.get("pending_by_client_month", {})
    records = state.get("records", [])
    
    # Create lookup for record status
    record_status = {r["internfactuurnummer"]: r.get("bestdoc", False) for r in records}
    
    unprocessed_groups = {}
    
    for client_month_key, group_objects in pending_groups.items():
        unprocessed_objects = []
        
        for obj in group_objects:
            obj_id = obj["id"]
            if not record_status.get(obj_id, False):  # bestdoc=false or not found
                unprocessed_objects.append(obj)
        
        if unprocessed_objects:
            unprocessed_groups[client_month_key] = unprocessed_objects
            logging.info(f"📋 Group {client_month_key}: {len(unprocessed_objects)}/{len(group_objects)} unprocessed")
    
    return unprocessed_groups


def update_after_processing(processed_groups: Dict[str, List[int]], generated_files: List[Dict]) -> None:
    """Update state after successful processing - mark records as processed"""
    state = get_state()
    now_iso = datetime.utcnow().isoformat() + "Z"
    
    # Get all processed IDs
    all_processed_ids = []
    for ids_list in processed_groups.values():
        all_processed_ids.extend(ids_list)
    
    # Update records array - mark as processed
    records = state.get("records", [])
    for record in records:
        if record["internfactuurnummer"] in all_processed_ids:
            record["bestdoc"] = True
            record["bestdoc_generated_at"] = now_iso
            
            # Find filename
            for file_info in generated_files:
                metadata = file_info.get("metadata", {})
                file_ids = metadata.get("internfactuurnummer", [])
                if isinstance(file_ids, list):
                    if record["internfactuurnummer"] in file_ids:
                        record["bestdoc_filename"] = file_info["filename"]
                        break
                elif record["internfactuurnummer"] == file_ids:
                    record["bestdoc_filename"] = file_info["filename"]
                    break
    
    # Keep pending_by_client_month unchanged for historical reference
    
    # Update statistics
    stats = state.get("statistics", {})
    stats["generated_bestdocs"] = stats.get("generated_bestdocs", 0) + len(all_processed_ids)
    stats["pending_bestdocs"] = sum(1 for r in records if not r["bestdoc"])
    stats["last_5pm_run"] = now_iso
    stats["last_5pm_processed_count"] = len(all_processed_ids)
    
    state["records"] = records
    state["statistics"] = stats
    
    save_state(state)
    logging.info(f"✅ Updated {len(all_processed_ids)} records: bestdoc=true")


def filter_already_processed(incoming_data: List[Dict]) -> List[Dict]:
    """Filter out already processed records based on bestdoc=true status"""
    if not incoming_data:
        return []
    
    state = get_state()
    processed_ids = {r["internfactuurnummer"] for r in state.get("records", []) if r.get("bestdoc", False)}
    
    unprocessed = []
    skipped = []
    
    for record in incoming_data:
        intern_id = record.get("INTERNFACTUURNUMMER")
        if intern_id in processed_ids:
            skipped.append(intern_id)
            logging.info(f"⏭️ SKIP: ID {intern_id} already processed (bestdoc=true)")
        else:
            unprocessed.append(record)
    
    if skipped:
        logging.info(f"🛡️ Duplicate prevention: skipped {len(skipped)} processed records")
    
    return unprocessed