import logging
import azure.functions as func
import json
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


def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    company = req.route_params.get('companyName')

    if method == "POST":
        try:
            new_data = req.get_json()
            logs = load_logs()  # This now uses optimized loading
            
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
            save_logs(logs)  # This now saves to multiple optimized locations
            
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
            # Get query parameters for optimization
            limit = int(req.params.get('limit', 50))
            status_filter = req.params.get('status')
            company_filter = req.params.get('company')
            recent_only = req.params.get('recent', 'true').lower() == 'true'
            
            # Check for timeouts first
            timed_out = check_and_timeout_pending()
            
            # Load data based on request
            if recent_only:
                logs = load_recent_logs(limit * 2)  # Load a bit more to account for filtering
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

            if not file_ref:
                return func.HttpResponse("Missing fileRef in request body", status_code=400)

            # Load the specific workflow to update steps
            logs = load_logs()
            target_log = None
            for log in logs:
                if file_ref in log.get("fileRef", "") or log.get("fileRef") == file_ref:
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

            # AFTER successful update, clean fileRef and get declaration ID
            try:
                # Clean the fileRef - remove file extension
                clean_commercial_ref = file_ref.replace('.xlsx', '').replace('.xls', '').replace('.pdf', '')
                logging.info(f"Cleaned commercial reference: {clean_commercial_ref} from {file_ref}")

                # Call Logic App with cleaned reference
                declaration_result = call_declaration_lookup_logic_app(clean_commercial_ref)

                if declaration_result.get('found'):
                    # Update workflow with declaration ID
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
                # Continue - workflow is already marked as success

            return func.HttpResponse("Workflow completed successfully ✅", status_code=200)

        except Exception as e:
            logging.error(f"PATCH error: {str(e)}")
            return func.HttpResponse(f"Error completing workflow: {str(e)}", status_code=500)
        
    return func.HttpResponse(
        json.dumps({"error": "Method not allowed"}),
        status_code=405,
        mimetype="application/json"
    )