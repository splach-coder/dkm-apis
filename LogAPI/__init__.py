import logging
import azure.functions as func
import json
from LogAPI.functions import load_logs, save_logs

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    company = req.route_params.get('companyName')

    if method == "POST":
        try:
            # Get the new data and create a copy to avoid reference issues
            new_data = req.get_json()
            
            # First load existing logs
            logs = load_logs()
            
            # Process only the new entry's finalResult
            all_steps_succeeded = True
            checker = "Anas"  # Default checker if any step fails

            # Loop through the steps to check their statuses
            for step in new_data.get("Steps", []):
                for step_name, step_data in step.items():
                    if step_data.get("status") == "Failed":
                        all_steps_succeeded = False
                        break
                if not all_steps_succeeded:
                    break
                
            # If all steps succeeded, set the checker to "Luc"
            if all_steps_succeeded:
                checker = "Luc"

            # Initialize finalResult if it doesn't exist
            if "finalResult" not in new_data:
                new_data["finalResult"] = {}

            # Make sure to assign BOOLEAN value, not a string
            new_data["finalResult"]["allStepsSucceeded"] = all_steps_succeeded  # Use direct assignment with boolean
            new_data["finalResult"]["checker"] = checker  # Use direct assignment
            new_data["companyName"] = company  # Use direct assignment

            # Append the new log and save
            logs.append(new_data)

            # Save updated logs to the blob storage
            save_logs(logs)
            return func.HttpResponse("Log saved ✅", status_code=200)
        except Exception as e:
            return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    elif method == "GET":
        logs = load_logs()
        return func.HttpResponse(json.dumps(logs), mimetype="application/json")

    elif method == "PATCH":
        # Check if this is the completion route
        path = req.url.split('/')[-1]  # Get the last part of the URL
        
        if path == "complete":
            try:
                # Handle completion route
                req_data = req.get_json()
                file_ref = req_data.get("fileRef")
                
                if not file_ref:
                    return func.HttpResponse("Missing fileRef in request body", status_code=400)
                
                # Load existing logs
                logs = load_logs()
                
                # Find the matching object by ref field
                found_log = None
                log_index = None
                
                for i, log in enumerate(logs):
                    if log.get("ref") == file_ref:
                        found_log = log
                        log_index = i
                        break
                
                if not found_log:
                    return func.HttpResponse(f"No log found with fileRef: {file_ref}", status_code=404)
                
                # Add the final step
                final_step = {
                    "finalStep": {
                        "status": "success"
                    }
                }
                
                # Add to the Steps array
                if "Steps" not in logs[log_index]:
                    logs[log_index]["Steps"] = []
                
                logs[log_index]["Steps"].append(final_step)
                
                # Save updated logs
                save_logs(logs)
                
                return func.HttpResponse("Workflow completed successfully ✅", status_code=200)
                
            except Exception as e:
                return func.HttpResponse(f"Error completing workflow: {str(e)}", status_code=500)
        else:
            return func.HttpResponse("PATCH method not supported for this route", status_code=405)

    return func.HttpResponse("Not allowed", status_code=405)