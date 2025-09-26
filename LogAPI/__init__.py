import logging
import azure.functions as func
import re
import json
import requests
from datetime import datetime
from LogAPI.functions import (
    load_logs, 
    save_logs, 
    load_recent_logs, 
    load_pending_logs, 
    update_workflow_status, 
    check_and_timeout_pending,
    call_declaration_lookup_logic_app
)

# Configuration for Stream Database Logic App
STREAM_DB_LOGIC_APP_URL = "https://prod-212.westeurope.logic.azure.com:443/workflows/e8483d6d1f404cc69f1cb01caad1e84b/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=WN6T2iMv8bxTs9pfqAhOjGM9C45BXN0Pl46n9cl_1-g"  # Replace with your actual URL

def call_stream_database(declaration_ids):
    """Call Stream Database Logic App to get declaration status"""
    try:
        payload = {"declarationIds": declaration_ids}
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(STREAM_DB_LOGIC_APP_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        return response.json()
    except Exception as e:
        logging.error(f"Error calling Stream Database Logic App: {str(e)}")
        return None

def process_declaration_data(declaration):
    """Process declaration to extract what we need"""
    full_history = json.loads(declaration.get("fullHistory", []))
    
    # Find last real user (not BATCHPROC)
    last_real_user = None
    last_real_activity = None
    touched = False
    
    # Go through history to find last human user
    for entry in reversed(full_history):  # Start from most recent
        user = entry.get("USERCODE", "")
        if user and user != "BATCHPROC":
            if not last_real_user:  # First real user we find (most recent)
                last_real_user = user
                last_real_activity = entry.get("HISTORYDATETIME")
            touched = True
    
    # If no real user found, use latest activity from BATCHPROC
    if not last_real_activity and full_history:
        last_real_activity = full_history[-1].get("HISTORYDATETIME")
    
    return {
        "status": declaration.get("status"),
        "lastActivity": last_real_activity,
        "lastUser": last_real_user,
        "touched": touched
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    company = req.route_params.get('companyName')
    
    # Self-contained Database update
    if method == "PUT":
        try:
            # Step 1: Load recent logs directly
            logs = load_recent_logs(100)

            if not logs:
                return func.HttpResponse(
                    json.dumps({"error": "No recent logs found"}),
                    status_code=404,
                    mimetype="application/json"
                )

            # Step 2: Process each log that has a declaration ID
            updated_count = 0
            analysis_result = []
            updated_logs_info = []

            for log in logs:
                # Check if this log has a declaration ID
                if type(log.get("declarationId")) is str:
                    declaration_id = log.get("declarationId")

                    # Step 3: Get stream data for this specific declaration ID
                    stream_data = call_stream_database([declaration_id])

                    if stream_data and stream_data.get("declarations"):
                        declaration = stream_data["declarations"][0]  # Should be only one

                        # Step 4: Process declaration data
                        processed_data = process_declaration_data(declaration)

                        # Step 5: Update this log directly
                        log["streamDatabase"] = processed_data
                        updated_count += 1

                        # Track updated log info
                        updated_logs_info.append({
                            "declarationId": declaration_id,
                            "companyName": log.get("companyName"),
                            "fileRef": log.get("fileRef")
                        })

                        # Step 6: Check if this file needs attention
                        if not processed_data.get("touched", True):
                            analysis_result.append({
                                "declarationId": log.get("declarationId"),
                                "companyName": log.get("companyName"),
                                "fileRef": log.get("fileRef"),
                                "issueType": "untouched",
                                "lastUser": processed_data.get("lastUser"),
                                "lastActivity": processed_data.get("lastActivity"),
                                "touched": processed_data.get("touched")
                            })

            # Step 7: Save updated logs
            if updated_count > 0:
                save_logs(logs)

            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": f"Processed {updated_count} logs individually",
                    "updatedLogs": updated_count,
                    "updatedLogsDetails": updated_logs_info,
                    "issuesFound": len(analysis_result),
                    "issues": analysis_result
                }),
                status_code=200,
                mimetype="application/json"
            )

        except Exception as e:
            logging.error(f"PUT error: {str(e)}")
            return func.HttpResponse(
                json.dumps({"error": f"Stream Database update failed: {str(e)}"}),
                status_code=500,
                mimetype="application/json"
            )

    if method == "POST":
        try:
            new_data = req.get_json()
            logs = load_logs()
            
            # Check original steps status
            has_failed_step = False
            has_pending_step = False
            
            for step in new_data.get("Steps", []):
                for step_name, step_data in step.items():
                    status = step_data.get("status", "").lower()
                    if status == "failed":
                        has_failed_step = True
                        break
                    elif status == "pending":
                        has_pending_step = True
                if has_failed_step:
                    break
            
            # Determine workflow status and add finalStep accordingly
            if has_failed_step:
                checker = "Anas"
                workflow_status = "failed"
                all_steps_succeeded = False
                
                final_step = {
                    "finalStep": {
                        "status": "failed",
                        "description": "Workflow failed - one or more steps unsuccessful",
                        "timestamp": datetime.utcnow().isoformat() + "Z"
                    }
                }
                new_data["Steps"].append(final_step)
                
            elif has_pending_step:
                checker = "Anas"
                workflow_status = "pending"
                all_steps_succeeded = False
                
            else:
                checker = "Luc"
                workflow_status = "pending"
                all_steps_succeeded = False
                
                final_step = {
                    "finalStep": {
                        "status": "pending",
                        "description": "Awaiting final validation",
                        "timestamp": datetime.utcnow().isoformat() + "Z"
                    }
                }
                new_data["Steps"].append(final_step)
            
            # Initialize finalResult
            if "finalResult" not in new_data:
                new_data["finalResult"] = {}
            
            # Set final result properties
            new_data["finalResult"]["workflowStatus"] = workflow_status
            new_data["finalResult"]["allStepsSucceeded"] = all_steps_succeeded
            new_data["finalResult"]["checker"] = checker
            new_data["companyName"] = company
            new_data["createdAt"] = datetime.utcnow().isoformat() + "Z"
            
            logs.append(new_data)
            save_logs(logs)
            
            return func.HttpResponse(
                json.dumps({"message": "Log saved ✅", "workflowStatus": workflow_status}),
                status_code=200,
                mimetype="application/json"
            )
            
        except Exception as e:
            logging.error(f"POST error: {str(e)}")
            return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    elif method == "GET":
        try:
            # Get query parameters
            limit = int(req.params.get('limit', 50))
            status_filter = req.params.get('status')
            company_filter = req.params.get('company')
            recent_only = req.params.get('recent', 'true').lower() == 'true'
            
            # Check for timeouts first
            timed_out = check_and_timeout_pending()
            
            # Load data
            if recent_only:
                logs = load_recent_logs(limit * 2)
            else:
                logs = load_logs()
            
            # Apply filters
            if status_filter:
                logs = [log for log in logs if log.get("finalResult", {}).get("workflowStatus") == status_filter]
            
            if company_filter:
                logs = [log for log in logs if log.get("companyName", "").lower() == company_filter.lower()]
            
            # Apply limit
            logs = logs[:limit]
            
            response_data = {
                "logs": logs,
                "total": len(logs),
                "timedOutWorkflows": timed_out,
                "cached": recent_only
            }
            
            return func.HttpResponse(json.dumps(response_data), mimetype="application/json")
            
        except Exception as e:
            logging.error(f"GET error: {str(e)}")
            return func.HttpResponse(f"Error loading logs: {str(e)}", status_code=500)

    elif method == "PATCH":
        try:
            req_data = req.get_json()
            file_ref = req_data.get("fileRef")
            
            if file_ref:
                file_ref = re.sub(r"\s+", "", file_ref.lower())

            if not file_ref:
                return func.HttpResponse("Missing fileRef in request body", status_code=400)

            # Load the specific workflow to update steps
            logs = load_logs()
            target_log = None
            for log in logs:
                LogFileRef = log.get("fileRef", "")
                LogFileRef = re.sub(r"\s+", "", LogFileRef.lower())
                if file_ref in LogFileRef or LogFileRef == file_ref:
                    target_log = log
                    break
                
            if not target_log:
                return func.HttpResponse(f"No log found with fileRef: {file_ref}", status_code=404)

            # Check current status
            current_status = target_log.get("finalResult", {}).get("workflowStatus")
            if current_status != "pending":
                return func.HttpResponse(f"Workflow is not in pending status. Current status: {current_status}", status_code=400)

            # Update finalStep
            steps = target_log.get("Steps", [])
            final_step_updated = False

            for step in steps:
                if "finalStep" in step:
                    if step["finalStep"].get("status") == "pending":
                        step["finalStep"]["status"] = "success"
                        step["finalStep"]["completedAt"] = datetime.utcnow().isoformat() + "Z"
                        final_step_updated = True
                        break
                    else:
                        return func.HttpResponse("finalStep is not in pending status", status_code=400)

            if not final_step_updated:
                return func.HttpResponse("No pending finalStep found in this workflow", status_code=404)

            # Prepare update data
            update_data = {
                "Steps": steps,
                "finalResult": {
                    **target_log.get("finalResult", {}),
                    "workflowStatus": "success",
                    "allStepsSucceeded": True,
                    "completedAt": datetime.utcnow().isoformat() + "Z"
                }
            }

            # Update workflow status FIRST
            success = update_workflow_status(file_ref, "success", update_data)

            if not success:
                return func.HttpResponse("Failed to update workflow", status_code=500)

            # Get declaration ID
            try:
                clean_commercial_ref = file_ref.replace('.xlsx', '').replace('.xls', '').replace('.pdf', '').replace(' ', '')
                declaration_result = call_declaration_lookup_logic_app(clean_commercial_ref)

                if declaration_result.get('found'):
                    additional_data = {
                        "declarationId": declaration_result.get('declarationId'),
                        "commercialReference": clean_commercial_ref
                    }
                    update_workflow_status(file_ref, "success", additional_data)
                    logging.info(f"Found and stored declaration ID: {declaration_result.get('declarationId')} for {file_ref}")
                else:
                    logging.warning(f"No declaration ID found for commercial reference: {clean_commercial_ref}")

            except Exception as e:
                logging.error(f"Failed to get declaration ID for {file_ref}: {str(e)}")

            return func.HttpResponse("Workflow completed successfully ✅", status_code=200)

        except Exception as e:
            logging.error(f"PATCH error: {str(e)}")
            return func.HttpResponse(f"Error completing workflow: {str(e)}", status_code=500)
        
    return func.HttpResponse("Method not allowed", status_code=405)