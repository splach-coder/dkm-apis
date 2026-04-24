"""
DocuSign JWT authentication service.
Generates access tokens using RSA private key stored in Azure Key Vault.
"""
import os
import jwt
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# DocuSign production constants
DOCUSIGN_AUTH_URL = "https://account.docusign.com/oauth/token"
DOCUSIGN_AUDIENCE = "account.docusign.com"
TOKEN_EXPIRY_SECONDS = 3600
REFRESH_BUFFER_SECONDS = 300

# Key Vault
KEYVAULT_URL = "https://kv-functions-python.vault.azure.net"
PRIVATE_KEY_SECRET_NAME = "docusign-private-key"
DOCUSIGN_SENDER_USER_ID_ENV = "DOCUSIGN_SENDER_USER_ID"
DOCUSIGN_DEFAULT_USER_ID_ENV = "DOCUSIGN_USER_ID"


class DocuSignAuthService:
    """
    Manages DocuSign JWT authentication with token caching.
    Private key is loaded from Azure Key Vault.
    """

    def __init__(self):
        self._token_cache: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

    def get_access_token(self) -> str:
        """
        Returns a valid DocuSign access token (cached or freshly obtained).

        Raises:
            DocuSignAuthError: if token retrieval fails
        """
        if self._is_token_valid():
            logging.info("Using cached DocuSign token")
            return self._token_cache

        logging.info("Requesting new DocuSign access token via JWT")
        private_key = self._load_private_key()
        jwt_assertion = self._build_jwt(private_key)
        access_token = self._exchange_jwt(jwt_assertion)

        expires_at = datetime.utcnow() + timedelta(
            seconds=TOKEN_EXPIRY_SECONDS - REFRESH_BUFFER_SECONDS
        )
        self._token_cache = access_token
        self._token_expires_at = expires_at
        logging.info(f"DocuSign token cached, expires at {expires_at.isoformat()}")
        return access_token

    def _is_token_valid(self) -> bool:
        if not self._token_cache or not self._token_expires_at:
            return False
        return datetime.utcnow() < self._token_expires_at

    def _load_private_key(self) -> str:
        """Load RSA private key from Azure Key Vault and ensure PEM format."""
        try:
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=KEYVAULT_URL, credential=credential)
            secret = client.get_secret(PRIVATE_KEY_SECRET_NAME)
            raw = secret.value.strip().replace("\\n", "\n")

            # If stored without PEM headers, wrap it
            if not raw.startswith("-----"):
                # Normalize: strip any existing whitespace, re-chunk into 64-char lines
                body = raw.replace(" ", "").replace("\n", "")
                chunked = "\n".join(body[i:i+64] for i in range(0, len(body), 64))
                raw = "-----BEGIN RSA PRIVATE KEY-----\n" + chunked + "\n-----END RSA PRIVATE KEY-----"

            return raw
        except Exception as e:
            raise DocuSignAuthError(f"Failed to load private key from Key Vault: {e}")

    def _build_jwt(self, private_key: str) -> str:
        """Build signed JWT assertion for DocuSign."""
        now = int(time.time())
        sender_user_id = (
            os.getenv(DOCUSIGN_SENDER_USER_ID_ENV)
            or os.getenv(DOCUSIGN_DEFAULT_USER_ID_ENV)
        )
        if not sender_user_id:
            raise DocuSignAuthError(
                f"Missing {DOCUSIGN_SENDER_USER_ID_ENV} or {DOCUSIGN_DEFAULT_USER_ID_ENV}"
            )

        payload = {
            "iss": os.environ["DOCUSIGN_INTEGRATION_KEY"],
            "sub": sender_user_id,
            "aud": DOCUSIGN_AUDIENCE,
            "iat": now,
            "exp": now + TOKEN_EXPIRY_SECONDS,
            "scope": "signature impersonation"
        }
        try:
            return jwt.encode(payload, private_key, algorithm="RS256")
        except Exception as e:
            raise DocuSignAuthError(f"Failed to build JWT: {e}")

    def _exchange_jwt(self, assertion: str) -> str:
        """Exchange JWT for an access token."""
        try:
            response = requests.post(
                DOCUSIGN_AUTH_URL,
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            return data["access_token"]
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response else ""
            status = e.response.status_code if e.response else ""
            logging.error(f"DocuSign token exchange failed — status: {status}, body: {body}")
            raise DocuSignAuthError(f"DocuSign token exchange failed: {e} — {body}")
        except Exception as e:
            raise DocuSignAuthError(f"Unexpected error during token exchange: {e}")

    def clear_cache(self):
        self._token_cache = None
        self._token_expires_at = None


class DocuSignAuthError(Exception):
    """Raised when DocuSign JWT authentication fails."""
    pass
