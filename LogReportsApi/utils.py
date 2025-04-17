import json
from json import decoder
import logging
import uuid
from LogReportsApi.functions import read_json, write_json, upload_file_to_blob, generate_report
import azure.functions as func

# GET reports (all or by user)
def handle_get_reports(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user = req.params.get("user")
        reports = read_json()

        if user:
            reports = [r for r in reports if r.get("email") == user]

        return func.HttpResponse(json.dumps(reports), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"GET error: {str(e)}")
        return func.HttpResponse("Error fetching reports", status_code=500)

# POST new report
def handle_post_report(req: func.HttpRequest) -> func.HttpResponse:
    try:
        content_type = req.headers.get('Content-Type')
        if not content_type or not content_type.startswith('multipart/form-data'):
            return func.HttpResponse("Unsupported Content-Type", status_code=400)

        body = req.get_body()
        multipart_data = decoder.MultipartDecoder(body, content_type)

        form_data = {}
        file_urls = []
        report_id = str(uuid.uuid4())

        for part in multipart_data.parts:
            content_disposition = part.headers.get(b'Content-Disposition').decode()

            if 'filename=' in content_disposition:
                # It's a file
                filename = content_disposition.split("filename=")[1].split(";")[0].replace('"', '')
                class FileObj:
                    def __init__(self, name, stream, mime):
                        self.filename = name
                        self.stream = stream
                        self.content_type = mime

                file_obj = FileObj(
                    filename,
                    stream=part.content,  # raw bytes
                    mime=part.headers.get(b'Content-Type', b'application/octet-stream').decode()
                )

                url = upload_file_to_blob(file_obj, report_id)
                file_urls.append(url)

            else:
                # It's a regular field
                name = content_disposition.split("name=")[1].replace('"', '')
                form_data[name] = part.text

        user_email = form_data.get("email")
        if not user_email:
            return func.HttpResponse("Missing 'email' field", status_code=400)

        # Inject file URLs into the form_data
        form_data["files"] = file_urls

        # Generate report with helper
        report = generate_report(form_data, user_email)

        # Save to reports.json
        reports = read_json()
        reports.append(report)
        write_json(reports)

        return func.HttpResponse(json.dumps(report), mimetype="application/json", status_code=201)

    except Exception as e:
        logging.error(f"POST error: {str(e)}")
        return func.HttpResponse("Error submitting report", status_code=500)

# PATCH report status or comment (admin only)
def handle_patch_report(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        report_id = data.get("id")
        status = data.get("status")
        comment = data.get("comment")

        reports = read_json()
        found = False

        for r in reports:
            if r["id"] == report_id:
                if status:
                    r["status"] = status
                if comment:
                    r["comment"] = comment
                found = True
                break

        if not found:
            return func.HttpResponse("Report not found", status_code=404)

        write_json(reports)
        return func.HttpResponse("Report updated", status_code=200)

    except Exception as e:
        logging.error(f"PATCH error: {str(e)}")
        return func.HttpResponse("Error updating report", status_code=500)