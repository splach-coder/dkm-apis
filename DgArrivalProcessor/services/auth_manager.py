"""
OAuth authentication manager with token caching
"""
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional


class AuthManager:
    """
    Manages OAuth authentication with token caching to reduce API calls
    """
    
    TOKEN_URL = "https://obibatch.ad.dkm-customs.com:5002/connect/token"
    CLIENT_ID = "client.ssw.dkm"
    CLIENT_SECRET = "vZFAWP5E18Lq"  # TODO: Move to Key Vault
    SCOPE = "be.ncts.integration"
    GRANT_TYPE = "client_credentials"
    
    # Token refresh buffer (refresh 5 minutes before expiry)
    REFRESH_BUFFER_SECONDS = 300
    
    def __init__(self):
        """Initialize auth manager with empty cache"""
        self.token_cache = {
            "access_token": None,
            "expires_at": None
        }
    
    def get_token(self) -> str:
        """
        Get valid access token (cached or fresh)
        
        Returns:
            Bearer token string
            
        Raises:
            AuthenticationError: If token retrieval fails
        """
        # Check if cached token is still valid
        if self._is_token_valid():
            logging.info("Using cached OAuth token")
            return self.token_cache["access_token"]
        
        # Request new token
        logging.info("Requesting new OAuth token")
        token_data = self._request_new_token()
        
        # Cache the new token
        self._cache_token(token_data)
        
        return self.token_cache["access_token"]
    
    def _is_token_valid(self) -> bool:
        """
        Check if cached token is still valid
        
        Returns:
            True if token exists and not expired
        """
        if not self.token_cache["access_token"]:
            return False
        
        if not self.token_cache["expires_at"]:
            return False
        
        # Check if token will expire soon
        now = datetime.utcnow()
        return now < self.token_cache["expires_at"]
    
    def _request_new_token(self) -> Dict:
        """
        Request new token from OAuth server
        
        Returns:
            Token response data
            
        Raises:
            AuthenticationError: If request fails
        """
        payload = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "scope": self.SCOPE,
            "grant_type": self.GRANT_TYPE
        }
        
        try:
            response = requests.post(
                self.TOKEN_URL,
                data=payload,
                timeout=10
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            logging.info("Successfully obtained OAuth token")
            return token_data
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error during authentication: {e}"
            if e.response:
                error_msg += f" - {e.response.text}"
            logging.error(error_msg)
            raise AuthenticationError(error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error during authentication: {e}"
            logging.error(error_msg)
            raise AuthenticationError(error_msg)
        
        except ValueError as e:
            error_msg = f"Invalid JSON response from auth server: {e}"
            logging.error(error_msg)
            raise AuthenticationError(error_msg)
    
    def _cache_token(self, token_data: Dict):
        """
        Store token with expiry time
        
        Args:
            token_data: Token response from OAuth server
        """
        access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
        
        # Calculate expiry time with buffer
        expires_at = datetime.utcnow() + timedelta(
            seconds=expires_in - self.REFRESH_BUFFER_SECONDS
        )
        
        self.token_cache = {
            "access_token": access_token,
            "expires_at": expires_at
        }
        
        logging.info(f"Token cached, expires at {expires_at.isoformat()}")
    
    def clear_cache(self):
        """Clear token cache (useful for testing or forced refresh)"""
        self.token_cache = {
            "access_token": None,
            "expires_at": None
        }
        logging.info("Token cache cleared")


class AuthenticationError(Exception):
    """Raised when OAuth authentication fails"""
    pass