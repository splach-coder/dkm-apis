import logging
import json
import os
import requests
import azure.functions as func

from DocuSignProcessor.services.auth_service import DocuSignAuthService, DocuSignAuthError

DOCUSIGN_BASE_URL = "https://eu.docusign.net/restapi/v2.1"

# Reuse the same module-level cached auth service
_auth_service = DocuSignAuthService()


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    GET /api/docusign/contacts

    Returns all contacts stored in the DocuSign account.
    Optional query params:
      ?search_text=John       → filter contacts by name/email
      ?include_count=true     → include total count in response
    """
    logging.info("DocuSignContacts function triggered")

    search_text = req.params.get("search_text", "").strip()
    include_count = req.params.get("include_count", "false").lower() == "true"

    # --- Auth ---
    try:
        access_token = _auth_service.get_access_token()
    except DocuSignAuthError as e:
        logging.error(f"DocuSign auth failed: {e}")
        return _error_response(f"Authentication failed: {e}", 500)

    # --- Fetch contacts owned by DV ---
    account_id = os.environ["DOCUSIGN_ACCOUNT_ID"]
    url = f"{DOCUSIGN_BASE_URL}/accounts/{account_id}/contacts"
    
    params = {}
    if search_text:
        params["search_text"] = search_text

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        contacts_raw = []
        current_url = url
        
        while current_url:
            response = requests.get(current_url, headers=headers, params=params if current_url == url else None, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # Merge contacts from this page
            page_contacts = data.get("contacts", [])
            contacts_raw.extend(page_contacts)
            
            # Check for next page
            next_uri = data.get("nextUri")
            if next_uri:
                if "accounts/" in next_uri:
                    path = next_uri.split("accounts/")[1]
                    current_url = f"{DOCUSIGN_BASE_URL}/accounts/{path}"
                else:
                    current_url = next_uri
            else:
                current_url = None
                
    except requests.exceptions.HTTPError as e:
        body_text = e.response.text if e.response else ""
        status_code = e.response.status_code if e.response else 502
        logging.error(f"DocuSign Contacts API error: {e} — {body_text}")
        _auth_service.clear_cache()  # token may be stale
        return _error_response(f"DocuSign API error: {body_text}", status_code)
    except Exception as e:
        logging.error(f"Unexpected error fetching contacts: {e}")
        return _error_response(f"Unexpected error: {e}", 500)

    # --- Shape the response ---
    contacts = [_shape_contact(c) for c in contacts_raw]

    result = {
        "success": True,
        "contacts": contacts,
    }

    if include_count:
        result["total"] = len(contacts)

    if search_text:
        result["search_text"] = search_text

    return func.HttpResponse(
        json.dumps(result, ensure_ascii=False),
        status_code=200,
        mimetype="application/json"
    )

def _shape_contact(raw: dict) -> dict:
    """Flatten a DocuSign contact into a clean, usable dict."""
    emails = raw.get("emails") or []
    primary_email = emails[0] if len(emails) > 0 else ""

    return {
        "contact_id":   raw.get("contactId", ""),
        "name":         raw.get("name", ""),
        "email":        primary_email,
        "all_emails":   emails,
        "organization": raw.get("organization", ""),
        "shared":       raw.get("shared", False) == "true" or raw.get("shared") is True
    }

def _error_response(message: str, status_code: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"success": False, "error": message}),
        status_code=status_code,
        mimetype="application/json"
    )
