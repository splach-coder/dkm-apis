"""
D&G Arrival Processor - Main Azure Function
Orchestrates the arrival request processing workflow
"""
import logging
import azure.functions as func
import json
import uuid
from datetime import datetime

from .services.api_client import ObiBatchClient, APIError, AuthenticationError
from .services.auth_manager import AuthManager
from .services.validator import ArrivalValidator
from .services.transformer import NCTSTransformer

# Initialize services (singleton pattern for token caching)
auth_manager = AuthManager()
validator = ArrivalValidator()
transformer = NCTSTransformer()


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main HTTP trigger for D&G Arrival Request processing
    
    Workflow:
    1. Parse and validate input
    2. Transform to NCTS schema
    3. Authenticate with OAuth
    4. Send to ObiBatch API
    5. Return response
    """
    # Generate request ID for tracing
    request_id = str(uuid.uuid4())
    logging.info(f"[{request_id}] DgArrivalProcessor triggered")
    
    # Only accept POST requests
    if req.method != "POST":
        return create_error_response(
            request_id=request_id,
            error="Method not allowed",
            status_code=405
        )
    
    try:
        # STEP 1: Parse incoming request
        body = req.get_json()
        logging.info(f"[{request_id}] Received form data with {len(body.get('mrns', []))} MRNs")
        
        # STEP 2: Validate input
        validation_result = validator.validate(body)
        if not validation_result.valid:
            logging.warning(f"[{request_id}] Validation failed: {validation_result.errors}")
            return create_validation_error_response(
                request_id=request_id,
                errors=validation_result.errors
            )
        
        logging.info(f"[{request_id}] Validation successful")
        
        # STEP 3: Transform to NCTS schema
        try:
            ncts_payload = transformer.transform(body)
            logging.info(f"[{request_id}] Transformation successful")
        except Exception as e:
            logging.error(f"[{request_id}] Transformation failed: {str(e)}")
            return create_error_response(
                request_id=request_id,
                error="Transformation failed",
                details=str(e),
                status_code=500
            )
        
        # STEP 4: Send to ObiBatch API
        api_client = ObiBatchClient(auth_manager)
        try:
            api_response = api_client.send_arrival(ncts_payload)
            logging.info(f"[{request_id}] API call successful: {api_response.get('submissionId')}")
            
            # STEP 5: Return success response
            return create_success_response(
                request_id=request_id,
                submission_id=api_response.get("submissionId"),
                mrns=body.get("mrns", []),
                api_data=api_response.get("data", {})
            )
            
        except AuthenticationError as e:
            logging.error(f"[{request_id}] Authentication failed: {str(e)}")
            return create_error_response(
                request_id=request_id,
                error="Authentication failed",
                details=str(e),
                status_code=401
            )
        except APIError as e:
            logging.error(f"[{request_id}] API call failed: {str(e)}")
            return create_error_response(
                request_id=request_id,
                error="Database submission failed",
                details=str(e),
                status_code=e.status_code or 500
            )
        finally:
            api_client.close()
            
    except ValueError as e:
        logging.error(f"[{request_id}] Invalid JSON: {str(e)}")
        return create_error_response(
            request_id=request_id,
            error="Invalid request format",
            details=str(e),
            status_code=400
        )
    except Exception as e:
        logging.error(f"[{request_id}] Unexpected error: {str(e)}", exc_info=True)
        return create_error_response(
            request_id=request_id,
            error="Internal server error",
            details=str(e),
            status_code=500
        )


def create_success_response(
    request_id: str,
    submission_id: str,
    mrns: list,
    api_data: dict
) -> func.HttpResponse:
    """
    Create success response
    
    Args:
        request_id: Request tracking ID
        submission_id: Submission ID from API
        mrns: List of MRNs processed
        api_data: Additional data from API
        
    Returns:
        HTTP 200 response
    """
    response_body = {
        "success": True,
        "message": "Arrival request submitted successfully",
        "submissionId": submission_id,
        "mrns": mrns,
        "requestId": request_id,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    return func.HttpResponse(
        json.dumps(response_body),
        status_code=200,
        mimetype="application/json"
    )


def create_validation_error_response(
    request_id: str,
    errors: list
) -> func.HttpResponse:
    """
    Create validation error response
    
    Args:
        request_id: Request tracking ID
        errors: List of validation error messages
        
    Returns:
        HTTP 400 response
    """
    response_body = {
        "success": False,
        "error": "Validation failed",
        "details": errors,
        "requestId": request_id,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    return func.HttpResponse(
        json.dumps(response_body),
        status_code=400,
        mimetype="application/json"
    )


def create_error_response(
    request_id: str,
    error: str,
    details: str = None,
    status_code: int = 500
) -> func.HttpResponse:
    """
    Create generic error response
    
    Args:
        request_id: Request tracking ID
        error: Error message
        details: Detailed error information
        status_code: HTTP status code
        
    Returns:
        HTTP error response
    """
    response_body = {
        "success": False,
        "error": error,
        "requestId": request_id,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    if details:
        response_body["details"] = details
    
    return func.HttpResponse(
        json.dumps(response_body),
        status_code=status_code,
        mimetype="application/json"
    )