import logging
import azure.functions as func
from LogReportsApi.utils import handle_get_reports, handle_post_report, handle_patch_report

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method

    if method == "GET":
        return handle_get_reports(req)
    elif method == "POST":
        return handle_post_report(req)
    elif method == "PATCH":
        return handle_patch_report(req)
    else:
        return func.HttpResponse("Method not allowed", status_code=405)