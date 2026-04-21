"""
DocuSign envelope service.
Sends dynamic PDF contracts using Composite Templates:
  - Document: generated PDF (base64) with variable content
  - Field positions: defined once in the DocuSign template
"""
import os
import logging
import requests
from dataclasses import dataclass


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
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

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
            "status": "sent",
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


class DocuSignServiceError(Exception):
    """Raised when a DocuSign API call fails."""
    pass
