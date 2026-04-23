import logging
import json
import base64
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from .services.auth_service import DocuSignAuthService, DocuSignAuthError
from .services.docusign_service import DocuSignService, DocuSignServiceError, EnvelopeRequest
from .models.response_model import DocuSignResponse

# Module-level auth service so token is cached across warm invocations
_auth_service = DocuSignAuthService()


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP handler — receives contract PDF + recipient info, sends via DocuSign.

    Expected POST body (minimal or full):
    {
        "declaration_id": 252885,
        "processfactuurnummer": 2026000003
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

    document_payload = body.get("document") if isinstance(body.get("document"), dict) else {}
    recipient_payload = body.get("recipient") if isinstance(body.get("recipient"), dict) else {}

    pdf_base64 = body.get("pdf_base64") or document_payload.get("pdf_base64")
    pdf_blob_path = (
        body.get("pdf_blob_path")
        or body.get("storage_path")
        or document_payload.get("blob_path")
        or document_payload.get("storage_path")
    )
    pdf_blob_container = (
        body.get("pdf_blob_container")
        or document_payload.get("blob_container")
        or "document-intelligence"
    )
    recipient_email = body.get("recipient_email") or recipient_payload.get("email")
    recipient_name = body.get("recipient_name") or recipient_payload.get("name")
    signer_function = body.get("signer_function") or recipient_payload.get("function")
    declaration_id = body.get("declaration_id")
    processfactuurnummer = body.get("processfactuurnummer")
    delete_blob_after_send = body.get("delete_blob_after_send")
    if delete_blob_after_send is None:
        delete_blob_after_send = document_payload.get("delete_after_send")
    if delete_blob_after_send is None:
        delete_blob_after_send = True

    client_naam = body.get("client_naam")
    # Status can be 'sent' or 'created' (Draft). Default is 'sent'.
    envelope_status = body.get("status") or "sent"
    
    if not pdf_blob_path and (declaration_id is not None or processfactuurnummer is not None):
        try:
            resolved = _resolve_pdf_from_ids(
                pdf_blob_container,
                declaration_id,
                processfactuurnummer
            )
            pdf_blob_path = resolved.get("blob_path")
            recipient_email = recipient_email or resolved.get("recipient_email")
            recipient_name = recipient_name or resolved.get("recipient_name")
            signer_function = signer_function or resolved.get("signer_function")
            
            client_landcode = resolved.get("client_landcode", "")
            client_plda = resolved.get("client_plda_operatoridentity", "")
            
            if client_landcode and client_plda:
                # Use strict VAT/Tax ID string as the keyword
                client_naam = f"{client_landcode}{client_plda}".strip()
            else:
                client_naam = client_naam or resolved.get("client_naam")
                
        except Exception as e:
            logging.error(f"Failed to resolve PDF from IDs: {e}")
            return _error_response("Failed to resolve document from declaration_id/processfactuurnummer", 404)

    if not pdf_base64 and not pdf_blob_path:
        return _error_response("Provide pdf_base64, pdf_blob_path, or declaration_id/processfactuurnummer", 400)

    # --- Auth ---
    try:
        access_token = _auth_service.get_access_token()
    except DocuSignAuthError as e:
        logging.error(f"DocuSign auth failed: {e}")
        return _error_response(f"Authentication failed: {e}", 500)

    # --- Setup Service & Lookups ---
    service = DocuSignService(access_token)
    
    # If we don't have the email but we do have a name to search by, query DocuSign Contacts
    if not recipient_email:
        search_target = client_naam or recipient_name
        if search_target:
            logging.info(f"Email missing, searching DocuSign Contacts for: {search_target}")
            recipient_email = service.get_client_email(search_target)
            if not recipient_email:
                logging.warning(f"Could not find strict email match for '{search_target}' in contacts.")
    else:
        logging.info(f"Using provided recipient email: {recipient_email}. Skipping DocuSign Contacts search.")

    # Apply defaults 
    recipient_name = recipient_name or client_naam or "Client"
    signer_function = signer_function or "Importer"

    if not recipient_email:
        return _error_response(
            "Missing recipient_email and no match found in DocuSign Contacts. Ensure email is provided or contact exists.",
            400
        )

    if not pdf_base64 and pdf_blob_path:
        try:
            pdf_base64 = _read_pdf_base64_from_blob(pdf_blob_container, pdf_blob_path)
        except Exception as e:
            logging.error(f"Failed to load PDF from blob: {e}")
            return _error_response(f"Failed to load PDF from blob path '{pdf_blob_path}'", 500)

    # --- Send envelope ---
    try:
        service = DocuSignService(access_token)
        envelope_request = EnvelopeRequest(
            pdf_base64=pdf_base64,
            recipient_email=recipient_email,
            recipient_name=recipient_name,
            signer_function=signer_function,
            document_name=body.get("document_name", "Contract"),
            email_subject=body.get("email_subject", "Please sign your contract"),
            status=envelope_status
        )
        result = service.send_envelope(envelope_request)

        if pdf_blob_path and delete_blob_after_send:
            _delete_blob(pdf_blob_container, pdf_blob_path)
            logging.info(f"Deleted blob after DocuSign send: {pdf_blob_container}/{pdf_blob_path}")
    except DocuSignServiceError as e:
        logging.error(f"DocuSign envelope failed: {e}")
        _auth_service.clear_cache()  # Token may be stale — clear it
        return _error_response(f"Envelope creation failed: {e}", 502)
    except Exception as e:
        logging.error(f"Post-send processing failed: {e}")
        return _error_response(f"Post-send processing failed: {e}", 500)

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


def _read_pdf_base64_from_blob(container_name: str, blob_path: str) -> str:
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing AzureWebJobsStorage")

    blob_service = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)
    if not blob_client.exists():
        raise ValueError("Blob not found")

    pdf_bytes = blob_client.download_blob().readall()
    return base64.b64encode(pdf_bytes).decode("utf-8")


def _delete_blob(container_name: str, blob_path: str) -> None:
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing AzureWebJobsStorage")

    blob_service = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)
    if blob_client.exists():
        blob_client.delete_blob()


def _split_csv_set(value: str) -> set:
    if not value:
        return set()
    return {v.strip() for v in value.split(",") if v and v.strip()}


def _resolve_pdf_from_ids(container_name: str, declaration_id, processfactuurnummer) -> dict:
    connect_str = os.getenv("AzureWebJobsStorage")
    if not connect_str:
        raise ValueError("Missing AzureWebJobsStorage")

    decl = str(declaration_id).strip() if declaration_id is not None else ""
    proc = str(processfactuurnummer).strip() if processfactuurnummer is not None else ""
    if not decl and not proc:
        raise ValueError("No IDs provided")

    blob_service = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service.get_container_client(container_name)

    best_match = None
    best_last_modified = None

    for blob in container_client.list_blobs(
        name_starts_with="Bestemmingsrapport/Generated/",
        include=["metadata"]
    ):
        metadata = blob.metadata or {}
        decl_set = _split_csv_set(metadata.get("declaration_ids", ""))
        proc_set = _split_csv_set(metadata.get("processfactuurnummers", ""))

        if decl and decl not in decl_set:
            continue
        if proc and proc not in proc_set:
            continue

        if best_match is None or (blob.last_modified and blob.last_modified > best_last_modified):
            best_match = blob
            best_last_modified = blob.last_modified

    if not best_match:
        raise ValueError("No matching blob found")

    metadata = best_match.metadata or {}
    return {
        "blob_path": best_match.name,
        "recipient_email": metadata.get("recipient_email", ""),
        "recipient_name": metadata.get("recipient_name", ""),
        "signer_function": metadata.get("signer_function", ""),
        "client_naam": metadata.get("client_naam", ""),
        "client_straat_en_nummer": metadata.get("client_straat_en_nummer", ""),
        "client_postcode": metadata.get("client_postcode", ""),
        "client_stad": metadata.get("client_stad", ""),
        "client_landcode": metadata.get("client_landcode", ""),
        "client_plda_operatoridentity": metadata.get("client_plda_operatoridentity", "")
    }
