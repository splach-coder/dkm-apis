import logging
import azure.functions as func
import json
from datetime import datetime
from LogAPI.functions import load_logs, save_logs

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    company = req.route_params.get('companyName')

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
                # One or more original steps failed
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
                # Some original steps are still pending
                checker = "Anas"
                workflow_status = "pending"
                all_steps_succeeded = False
                
                # Don't add finalStep yet - workflow still in progress
                
            else:
                # All original steps succeeded - add pending finalStep for validation
                checker = "Luc"
                workflow_status = "pending"
                all_steps_succeeded = False  # Not succeeded until final validation complete
                
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
            
            # Add timestamp for tracking
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
            logs = load_logs()
            return func.HttpResponse(json.dumps(logs), mimetype="application/json")
        except Exception as e:
            logging.error(f"GET error: {str(e)}")
            return func.HttpResponse(f"Error loading logs: {str(e)}", status_code=500)

    elif method == "PATCH":
        try:
            # Handle completion route
            req_data = req.get_json()
            file_ref = req_data.get("fileRef")
            
            if not file_ref:
                return func.HttpResponse("Missing fileRef in request body", status_code=400)
            
            # Load existing logs
            logs = load_logs()
            
            # Find the matching object by fileRef
            found_log = None
            log_index = None
            
            for i, log in enumerate(logs):
                if log.get("fileRef") == file_ref:
                    found_log = log
                    log_index = i
                    break
            
            if not found_log:
                return func.HttpResponse(f"No log found with fileRef: {file_ref}", status_code=404)
            
            # Find and update the finalStep status from pending to success
            steps = logs[log_index].get("Steps", [])
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
            
            # Update workflow status and allStepsSucceeded
            logs[log_index]["finalResult"]["workflowStatus"] = "success"
            logs[log_index]["finalResult"]["allStepsSucceeded"] = True
            
            # Save updated logs
            save_logs(logs)
            
            return func.HttpResponse("Workflow completed successfully ✅", status_code=200)
                
        except Exception as e:
            logging.error(f"PATCH error: {str(e)}")
            return func.HttpResponse(f"Error completing workflow: {str(e)}", status_code=500)
        
    return func.HttpResponse(
        json.dumps({"error": "Method not allowed"}),
        status_code=405,
        mimetype="application/json"
    )