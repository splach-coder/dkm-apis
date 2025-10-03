import logging
import azure.functions as func
import json
from datetime import datetime
from collections import defaultdict
from azure.storage.blob import BlobServiceClient
import os

# Blob Storage Configuration
CONTAINER_NAME = "document-intelligence"
FOLDER_NAME = "container-weight-checker"

def get_blob_client():
    logging.info(f"Getting blob client")
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

def load_json_from_blob(blob_name):
    try:
        _, container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{blob_name}"
        blob = container.get_blob_client(blob_path)
        download = blob.download_blob().readall()
        return json.loads(download)
    except Exception as e:
        logging.warning(f"Could not load {blob_name}: {str(e)}")
        return None

def save_json_to_blob(blob_name, data):
    try:
        _, container = get_blob_client()
        blob_path = f"{FOLDER_NAME}/{blob_name}"
        blob = container.get_blob_client(blob_path)
        blob.upload_blob(json.dumps(data, indent=2), overwrite=True)
        logging.info(f"Successfully saved {blob_name}")
    except Exception as e:
        logging.error(f"Error saving {blob_name}: {str(e)}")
        raise

def process_data(raw_data):
    """Group and process declaration data"""
    grouped = defaultdict(lambda: {
        'declaration': {},
        'containers': set(),
        'items': {}
    })
    
    for row in raw_data:
        decl_id = row['DECLARATIONID']
        
        # Store declaration info (first occurrence)
        if not grouped[decl_id]['declaration']:
            grouped[decl_id]['declaration'] = {
                'declarationId': decl_id,
                'declarationGuid': row['DECLARATIONGUID'],
                'company': row['ACTIVECOMPANY'],
                'status': row['MESSAGESTATUS'],
                'declarationType': row['TYPEDECLARATIONSSW'],
                'dateOfAcceptance': row['DATEOFACCEPTANCE'],
                'totalGrossMass': row['TOTALGROSSMASS']
            }
        
        # Track unique containers
        if row['CONTAINERGUID']:
            grouped[decl_id]['containers'].add(row['CONTAINERGUID'])
        
        # Track items and their containers
        item_guid = row['ITEMGUID']
        if item_guid not in grouped[decl_id]['items']:
            grouped[decl_id]['items'][item_guid] = {
                'itemGuid': item_guid,
                'itemSequence': row['ITEM_SEQUENCE'],
                'grossMass': row['ITEM_WEIGHT'],
                'containers': []
            }
        
        # Add container to item (avoid duplicates)
        container_info = {
            'containerGuid': row['CONTAINERGUID'],
            'containerSequence': row['CONTAINER_SEQUENCE'],
            'containerNumber': row['CONTAINER_NUMBER']
        }
        if container_info not in grouped[decl_id]['items'][item_guid]['containers']:
            grouped[decl_id]['items'][item_guid]['containers'].append(container_info)
    
    # Build final result with violation checks
    result = []
    for decl_id, data in grouped.items():
        decl = data['declaration']
        container_count = len(data['containers'])
        avg_weight = decl['totalGrossMass'] / container_count if container_count > 0 else 0
        
        has_violation = avg_weight > 25000
        exceeds_by = avg_weight - 25000 if has_violation else 0
        
        result.append({
            **decl,
            'containerCount': container_count,
            'avgWeightPerContainer': round(avg_weight, 2),
            'violation': {
                'hasViolation': has_violation,
                'exceedsBy': round(exceeds_by, 2),
                'message': f"Average weight ({round(avg_weight/1000, 2)} tons) exceeds 25 ton limit" if has_violation else "Within limit"
            },
            'items': list(data['items'].values()),
            'checkedAt': datetime.utcnow().isoformat() + "Z"
        })
    
    return result

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    
    if method == "POST":
        try:
            # Get data from Logic Apps
            body = req.get_json()
            raw_data = body.get('Table1', [])
            
            if not raw_data:
                return func.HttpResponse(
                    json.dumps({"success": True, "message": "No new data to process"}),
                    status_code=200,
                    mimetype="application/json"
                )
            
            logging.info(f"Processing {len(raw_data)} rows")
            
            # 3. PROCESS DATA
            processed_results = process_data(raw_data)
            violations = [r for r in processed_results if r['violation']['hasViolation']]
            new_processed_ids = list(set([r['declarationId'] for r in processed_results]))
            
            logging.info(f"Found {len(violations)} violations out of {len(processed_results)} declarations")
            
            # 4. STORE VIOLATIONS
            existing_violations = load_json_from_blob('violations.json') or {'violations': []}
            existing_violations['violations'].extend(violations)
            save_json_to_blob('violations.json', existing_violations)
            
            # 5. UPDATE PROCESSED IDs
            processed_file = load_json_from_blob('processed_declarations.json') or {'processedIds': []}
            processed_file['processedIds'].extend(new_processed_ids)
            processed_file['processedIds'] = list(set(processed_file['processedIds']))
            save_json_to_blob('processed_declarations.json', processed_file)
            
            # 6. UPDATE LAST RUN
            last_run = {
                'lastRun': datetime.utcnow().isoformat() + "Z",
                'recordsProcessed': len(processed_results),
                'violationsFound': len(violations)
            }
            save_json_to_blob('last_run.json', last_run)
            
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "processedCount": len(processed_results),
                    "violationsFound": len(violations),
                    "newProcessedIds": new_processed_ids
                }),
                status_code=200,
                mimetype="application/json"
            )
            
        except Exception as e:
            logging.error(f"POST error: {str(e)}")
            return func.HttpResponse(
                json.dumps({"success": False, "error": str(e)}),
                status_code=500,
                mimetype="application/json"
            )
    
    elif method == "GET":
        try:
            violations_data = load_json_from_blob('violations.json') or {'violations': []}
            
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "totalViolations": len(violations_data['violations']),
                    "data": violations_data['violations']
                }),
                status_code=200,
                mimetype="application/json"
            )
        except Exception as e:
            logging.error(f"GET error: {str(e)}")
            return func.HttpResponse(
                json.dumps({"success": False, "error": str(e)}),
                status_code=500,
                mimetype="application/json"
            )
    
    elif method == "DELETE":
        try:
            # Get declaration ID from query parameter
            declaration_id_str = req.params.get('declarationId')
            
            if not declaration_id_str:
                return func.HttpResponse(
                    json.dumps({"success": False, "error": "declarationId parameter is required"}),
                    status_code=400,
                    mimetype="application/json"
                )
            
            # Convert to integer for comparison (since declarationId is stored as int in JSON)
            try:
                declaration_id = int(declaration_id_str)
            except ValueError:
                return func.HttpResponse(
                    json.dumps({"success": False, "error": "declarationId must be a valid number"}),
                    status_code=400,
                    mimetype="application/json"
                )
            
            # Load existing violations
            violations_data = load_json_from_blob('violations.json') or {'violations': []}
            
            # Find and remove the violation with matching declarationId
            original_count = len(violations_data['violations'])
            violations_data['violations'] = [
                v for v in violations_data['violations'] 
                if v.get('declarationId') != declaration_id  # Now comparing int to int
            ]
            
            new_count = len(violations_data['violations'])
            removed_count = original_count - new_count
            
            if removed_count == 0:
                return func.HttpResponse(
                    json.dumps({
                        "success": False, 
                        "error": f"No violation found with declarationId: {declaration_id}"
                    }),
                    status_code=404,
                    mimetype="application/json"
                )
            
            # Save updated violations back to blob
            save_json_to_blob('violations.json', violations_data)
            
            logging.info(f"Deleted violation with declarationId: {declaration_id}")
            
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": f"Violation with declarationId {declaration_id} deleted successfully",
                    "removedCount": removed_count,
                    "remainingViolations": new_count
                }),
                status_code=200,
                mimetype="application/json"
            )
            
        except Exception as e:
            logging.error(f"DELETE error: {str(e)}")
            return func.HttpResponse(
                json.dumps({"success": False, "error": str(e)}),
                status_code=500,
                mimetype="application/json"
            )
    
    return func.HttpResponse("Method not allowed", status_code=405)