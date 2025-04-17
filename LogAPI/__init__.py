import logging
import azure.functions as func
import json
from LogAPI.functions import load_logs, save_logs

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    company = req.route_params.get('companyName')
    run_id = req.route_params.get('runId')

    if method == "POST":
        try:
            # Get the new data and create a copy to avoid reference issues
            new_data = req.get_json()
            
            # First load existing logs
            logs = load_logs(company)
            
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
            new_data["companyName"] = "company"  # Use direct assignment

            # Append the new log and save
            logs.append(new_data)

            # Save updated logs to the blob storage
            save_logs(company, logs)
            return func.HttpResponse("Log saved ✅", status_code=200)
        except Exception as e:
            return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    elif method == "GET" and run_id:
        logs = load_logs(company)
        result = next((log for log in logs if log["runId"] == run_id), None)
        if result:
            return func.HttpResponse(json.dumps(result), mimetype="application/json")
        return func.HttpResponse("Log not found", status_code=404)

    elif method == "GET":
        logs = load_logs(company)
        return func.HttpResponse(json.dumps(logs), mimetype="application/json")

    return func.HttpResponse("Not allowed", status_code=405)
