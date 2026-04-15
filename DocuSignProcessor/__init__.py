import logging
import json
import azure.functions as func

from .services.auth_service import DocuSignAuthService, DocuSignAuthError
from .services.docusign_service import DocuSignService, DocuSignServiceError, EnvelopeRequest
from .models.response_model import DocuSignResponse

# Module-level auth service so token is cached across warm invocations
_auth_service = DocuSignAuthService()


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP handler — receives contract PDF + recipient info, sends via DocuSign.

    Expected POST body:
    {
        "pdf_base64": "<base64 encoded PDF>",
        "recipient_email": "client@example.com",
        "recipient_name": "John Doe",
        "signer_function": "CEO",
        "document_name": "Contract 2024",       // optional
        "email_subject": "Please sign"          // optional
    }
    """
    logging.info("DocuSignProcessor function triggered")

    if req.method != "POST":
        return _error_response("Method not allowed", 405)

    # --- Parse body ---
    try:
        body = req.get_json()
    except ValueError:
        return _error_response("Invalid JSON body", 400)

    pdf_base64 = body.get("pdf_base64")
    recipient_email = body.get("recipient_email")
    recipient_name = body.get("recipient_name")
    signer_function = body.get("signer_function")

    if not all([pdf_base64, recipient_email, recipient_name, signer_function]):
        return _error_response(
            "Missing required fields: pdf_base64, recipient_email, recipient_name, signer_function",
            400
        )

    # --- Auth ---
    try:
        access_token = _auth_service.get_access_token()
    except DocuSignAuthError as e:
        logging.error(f"DocuSign auth failed: {e}")
        return _error_response(f"Authentication failed: {e}", 500)

    # --- Send envelope ---
    try:
        service = DocuSignService(access_token)
        envelope_request = EnvelopeRequest(
            pdf_base64=pdf_base64,
            recipient_email=recipient_email,
            recipient_name=recipient_name,
            signer_function=signer_function,
            document_name=body.get("document_name", "Contract"),
            email_subject=body.get("email_subject", "Please sign your contract")
        )
        result = service.send_envelope(envelope_request)
    except DocuSignServiceError as e:
        logging.error(f"DocuSign envelope failed: {e}")
        _auth_service.clear_cache()  # Token may be stale — clear it
        return _error_response(f"Envelope creation failed: {e}", 502)

    response = DocuSignResponse(
        success=True,
        message="Envelope sent successfully",
        envelope_id=result.envelope_id,
        envelope_status=result.status
    )
    return func.HttpResponse(
        json.dumps(response.to_dict()),
        status_code=200,
        mimetype="application/json"
    )


def _error_response(message: str, status_code: int) -> func.HttpResponse:
    response = DocuSignResponse(success=False, message=message, error=message)
    return func.HttpResponse(
        json.dumps(response.to_dict()),
        status_code=status_code,
        mimetype="application/json"
    )
