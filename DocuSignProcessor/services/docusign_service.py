"""
DocuSign envelope service.
Sends dynamic PDF contracts using Composite Templates:
  - Document: generated PDF (base64) with variable content
  - Field positions: defined once in the DocuSign template
"""
import os
import logging
import time
import requests
from dataclasses import dataclass

_CONTACTS_CACHE = None
_CONTACTS_CACHE_TIME = None
CACHE_EXPIRY_SECONDS = 3600  # Cache contacts for 1 hour
LIVE_CONTACTS_LOOKUP_ENV = "DOCUSIGN_ENABLE_LIVE_CONTACTS_LOOKUP"


DOCUSIGN_BASE_URL = "https://eu.docusign.net/restapi/v2.1"

# Must match the recipient role name set in the DocuSign template
RECIPIENT_ROLE_NAME = "Client"


@dataclass
class EnvelopeRequest:
    pdf_base64: str           # Dynamically generated PDF (base64)
    recipient_email: str
    recipient_name: str
    signer_function: str
    document_name: str = "Contract"
    email_subject: str = "Please sign your contract"
    status: str = "sent"  # 'sent' or 'created' (Draft)


@dataclass
class EnvelopeResult:
    envelope_id: str
    status: str
    uri: str


class DocuSignService:
    """
    Sends DocuSign envelopes using Composite Templates.
    The PDF is dynamic (generated per invoice), field positions come from the template.
    """

    def __init__(self, access_token: str):
        self._access_token = access_token or ""
        self._headers = {"Content-Type": "application/json"}
        if self._access_token:
            self._headers["Authorization"] = f"Bearer {self._access_token}"

    def send_envelope(self, request: EnvelopeRequest) -> EnvelopeResult:
        """
        Create and send a DocuSign envelope with a dynamic PDF.

        Raises:
            DocuSignServiceError: if envelope creation fails
        """
        account_id = os.environ["DOCUSIGN_ACCOUNT_ID"]
        template_id = os.environ["DOCUSIGN_TEMPLATE_ID"]
        url = f"{DOCUSIGN_BASE_URL}/accounts/{account_id}/envelopes"
        body = {
            "emailSubject": request.email_subject,
            "status": request.status,
            "compositeTemplates": [
                {
                    # Template provides the field/tab positions (signer_name, signer_function, sign_here)
                    "serverTemplates": [
                        {
                            "sequence": "1",
                            "templateId": template_id
                        }
                    ],
                    # Inline template assigns the recipient and pre-fills tab values
                    "inlineTemplates": [
                        {
                            "sequence": "2",
                            "recipients": {
                                "signers": [
                                    {
                                        "roleName": RECIPIENT_ROLE_NAME,
                                        "recipientId": "1",
                                        "email": request.recipient_email,
                                        "name": request.recipient_name,
                                        "tabs": {
                                            "textTabs": [
                                                {
                                                    "tabLabel": "signer_function",
                                                    "value": request.signer_function
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    # Dynamic document overrides the static template document
                    "document": {
                        "documentBase64": request.pdf_base64,
                        "name": request.document_name,
                        "fileExtension": "pdf",
                        "documentId": "1"
                    }
                }
            ]
        }

        try:
            response = requests.post(url, headers=self._headers, json=body, timeout=30)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Envelope sent: {data.get('envelopeId')} — {data.get('status')}")
            return EnvelopeResult(
                envelope_id=data["envelopeId"],
                status=data["status"],
                uri=data.get("uri", "")
            )
        except requests.exceptions.HTTPError as e:
            body_text = e.response.text if e.response else ""
            raise DocuSignServiceError(f"Envelope creation failed: {e} — {body_text}")
        except Exception as e:
            raise DocuSignServiceError(f"Unexpected error sending envelope: {e}")

    def get_envelope_status(self, envelope_id: str) -> dict:
        """Check the current status of an envelope."""
        url = f"{DOCUSIGN_BASE_URL}/accounts/{os.environ['DOCUSIGN_ACCOUNT_ID']}/envelopes/{envelope_id}"
        try:
            response = requests.get(url, headers=self._headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise DocuSignServiceError(f"Failed to get envelope status: {e}")

    def _fetch_all_contacts(self) -> dict:
        """Fetch the fully synchronized JSON contacts database from Azure Blob Storage."""
        from azure.storage.blob import BlobServiceClient
        import json
        
        contacts_dict = {}
        try:
            connect_str = os.getenv("AzureWebJobsStorage")
            if not connect_str:
                logging.error("Missing AzureWebJobsStorage")
                return contacts_dict
                
            blob_service = BlobServiceClient.from_connection_string(connect_str)
            blob_client = blob_service.get_blob_client(container="document-intelligence", blob="contacts_db.json")
            
            if not blob_client.exists():
                logging.error("contacts_db.json not found in blob storage. Run download script first.")
                return contacts_dict
                
            data = blob_client.download_blob().readall()
            contacts_list = json.loads(data)
            
            for c in contacts_list:
                name = c.get("name", "")
                emails = c.get("emails", [])
                if name and emails and len(emails) > 0:
                    contacts_dict[name.lower()] = emails[0]
                    
        except Exception as e:
            logging.error(f"Failed to read contacts_db.json from blob: {e}")
            
        return contacts_dict

    def _live_lookup_enabled(self) -> bool:
        """
        Live DocuSign contacts lookup is opt-in.
        Default is disabled so send/precheck stays fast and relies on blob-synced contacts only.
        """
        raw_value = os.getenv(LIVE_CONTACTS_LOOKUP_ENV, "").strip().lower()
        return raw_value in {"1", "true", "yes", "on"}

    def get_client_email(self, client_name: str) -> str:
        """
        Resolve client email using:
        1) Blob-synced contacts cache (fast)
        2) Optional live DocuSign Contacts API fallback when explicitly enabled
        """
        if not client_name:
            return ""
            
        global _CONTACTS_CACHE, _CONTACTS_CACHE_TIME
        now = time.time()
        
        if _CONTACTS_CACHE is None or _CONTACTS_CACHE_TIME is None or (now - _CONTACTS_CACHE_TIME > CACHE_EXPIRY_SECONDS):
            logging.info("Downloading all contacts from Blob DB to warm the memory cache...")
            try:
                _CONTACTS_CACHE = self._fetch_all_contacts()
                _CONTACTS_CACHE_TIME = now
                logging.info(f"Successfully cached {len(_CONTACTS_CACHE)} contacts from blob.")
            except Exception as e:
                logging.error(f"Failed to cache complete contacts list: {e}")
                return ""
        
        # Exact match first
        target = client_name.lower().strip()
        if target in _CONTACTS_CACHE:
            return _CONTACTS_CACHE[target]
            
        # Strict substring Match (no fuzzy-wuzzy)
        for name, email in _CONTACTS_CACHE.items():
            if target in name:
                return email

        if not self._live_lookup_enabled():
            logging.info(
                "No blob contact match for '%s'; live DocuSign contacts lookup is disabled.",
                client_name
            )
            return ""

        # Fallback to live DocuSign contacts if access token is available
        live_email = self._search_contact_email_live(target)
        if live_email:
            _CONTACTS_CACHE[target] = live_email
            return live_email

        return ""

    def _search_contact_email_live(self, target: str) -> str:
        """
        Query live DocuSign contacts API for a specific search target.
        Returns the best email match or empty string.
        """
        if not self._access_token:
            return ""

        account_id = os.environ.get("DOCUSIGN_ACCOUNT_ID", "")
        if not account_id:
            logging.warning("DOCUSIGN_ACCOUNT_ID missing; skipping live contacts lookup.")
            return ""

        base_url = f"{DOCUSIGN_BASE_URL}/accounts/{account_id}/contacts"
        params = {"search_text": target}
        contacts_raw = []
        current_url = base_url

        try:
            while current_url:
                response = requests.get(
                    current_url,
                    headers=self._headers,
                    params=params if current_url == base_url else None,
                    timeout=15
                )
                response.raise_for_status()
                data = response.json()
                contacts_raw.extend(data.get("contacts", []))

                next_uri = data.get("nextUri")
                if not next_uri:
                    current_url = None
                elif "accounts/" in next_uri:
                    path = next_uri.split("accounts/")[1]
                    current_url = f"{DOCUSIGN_BASE_URL}/accounts/{path}"
                else:
                    current_url = next_uri

            exact_match_email = ""
            partial_match_email = ""
            for raw in contacts_raw:
                name = str(raw.get("name", "")).strip().lower()
                emails = raw.get("emails") or []
                email = emails[0] if emails else ""
                if not email:
                    continue
                if name == target:
                    exact_match_email = email
                    break
                if not partial_match_email and target in name:
                    partial_match_email = email

            return exact_match_email or partial_match_email
        except Exception as e:
            logging.warning(f"Live contacts lookup failed for '{target}': {e}")
            return ""


class DocuSignServiceError(Exception):
    """Raised when a DocuSign API call fails."""
    pass
