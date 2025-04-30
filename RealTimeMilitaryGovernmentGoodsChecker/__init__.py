import logging
import azure.functions as func
from RealTimeMilitaryGovernmentGoodsChecker.utils import handle_POST_CHECKER_REQ, handle_GET_MATCHED_ADDRESSES_REQ, handle_PATCH_CHECKER_REQ

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method

    if method == "GET":
        return handle_GET_MATCHED_ADDRESSES_REQ(req)
    elif method == "POST":
        return handle_POST_CHECKER_REQ(req)
    elif method == "PATCH":
        return handle_PATCH_CHECKER_REQ(req)
    else:
        return func.HttpResponse("Method not allowed", status_code=405)