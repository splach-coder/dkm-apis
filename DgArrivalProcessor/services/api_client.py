"""
HTTP client for ObiBatch API with retry logic
"""
import logging
import requests
import time
from typing import Dict, Callable
from functools import wraps


def retry(max_attempts: int = 3, backoff: float = 2.0, exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_attempts: Maximum number of retry attempts
        backoff: Base backoff time in seconds (will be exponential)
        exceptions: Tuple of exceptions to catch
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    
                    wait_time = backoff ** attempt
                    logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
        return wrapper
    return decorator


class ObiBatchClient:
    """
    HTTP client for ObiBatch API with retry logic and error handling
    """
    
    BASE_URL = "https://obibatch.ad.dkm-customs.com/api/be-ncts/batch"
    TENANT_ID = "DKM_VP"
    TIMEOUT_SECONDS = 30
    
    def __init__(self, auth_manager):
        """
        Initialize API client
        
        Args:
            auth_manager: AuthManager instance for token management
        """
        self.auth_manager = auth_manager
        self.session = requests.Session()
    
    def send_arrival(self, payload: Dict) -> Dict:
        """
        Send arrival notification to API
        
        Args:
            payload: NCTS-compliant JSON
            
        Returns:
            API response dict
            
        Raises:
            APIError: If API call fails
        """
        logging.info("Sending arrival notification to ObiBatch API")
        
        try:
            # Get authentication token
            token = self.auth_manager.get_token()
            
            # Build headers
            headers = self._build_headers(token)
            
            # Build URL
            url = f"{self.BASE_URL}/arrivals/"
            
            # Send request with retry logic
            response = self._send_request(url, payload, headers)
            
            # Handle response
            return self._handle_response(response)
            
        except Exception as e:
            logging.error(f"Failed to send arrival notification: {str(e)}")
            raise
    
    def _build_headers(self, token: str) -> Dict:
        """
        Build request headers with auth and tenant
        
        Args:
            token: Bearer token
            
        Returns:
            Headers dict
        """
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "x-Tenant-Id": self.TENANT_ID
        }
    
    @retry(
        max_attempts=3,
        backoff=2.0,
        exceptions=(requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    )
    def _send_request(self, url: str, payload: Dict, headers: Dict):
        """
        Send HTTP request with retry logic
        
        Args:
            url: API endpoint URL
            payload: JSON payload
            headers: Request headers
            
        Returns:
            Response object
            
        Raises:
            Various requests exceptions
        """
        logging.info(f"PUT {url}")
        
        response = self.session.put(
            url,
            json=payload,
            headers=headers,
            timeout=self.TIMEOUT_SECONDS
        )
        
        return response
    
    def _handle_response(self, response) -> Dict:
        """
        Handle API response with error checking
        
        Args:
            response: Response object from requests
            
        Returns:
            Parsed response dict
            
        Raises:
            APIError: If response indicates failure
        """
        try:
            # Check for HTTP errors
            if response.status_code >= 400:
                error_detail = ""
                try:
                    error_data = response.json()
                    error_detail = error_data.get("message", response.text)
                except:
                    error_detail = response.text
                
                raise APIError(
                    f"API returned status {response.status_code}: {error_detail}",
                    status_code=response.status_code,
                    response_text=response.text
                )
            
            # Parse JSON response
            if response.content:
                response_data = response.json()
            else:
                response_data = {"success": True}
            
            logging.info(f"API call successful: {response.status_code}")
            return {
                "success": True,
                "status_code": response.status_code,
                "data": response_data,
                "submissionId": response_data.get("id", response_data.get("lrn"))
            }
            
        except APIError:
            raise
        except ValueError as e:
            logging.error(f"Failed to parse API response: {e}")
            raise APIError(f"Invalid JSON response from API: {e}")
        except Exception as e:
            logging.error(f"Unexpected error handling response: {e}")
            raise APIError(f"Failed to process API response: {e}")
    
    def close(self):
        """Close HTTP session"""
        self.session.close()


class APIError(Exception):
    """Raised when ObiBatch API call fails"""
    
    def __init__(self, message: str, status_code: int = None, response_text: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text