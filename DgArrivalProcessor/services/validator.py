"""
Input validation service for D&G Arrival Requests
"""
import logging
import re
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of validation operation"""
    valid: bool
    errors: List[str]


class ArrivalValidator:
    """
    Validates incoming arrival request data according to business rules
    """
    
    # Validation patterns
    MRN_PATTERN = re.compile(r'^[A-Z0-9]{8,20}$')
    REFERENCE_PATTERN = re.compile(r'^[A-Z0-9]{4,15}$')
    KLANT_PATTERN = re.compile(r'^[A-Za-z\s]{2,50}$')
    
    def validate(self, data: Dict) -> ValidationResult:
        """
        Validate all fields according to business rules
        
        Args:
            data: Raw form data from frontend
            
        Returns:
            ValidationResult with errors or success
        """
        errors = []
        
        # Validate MRNs
        mrn_errors = self._validate_mrns(data.get("mrns"))
        errors.extend(mrn_errors)
        
        # Validate Reference
        ref_error = self._validate_reference(data.get("reference"))
        if ref_error:
            errors.append(ref_error)
        
        # Validate Klant
        klant_error = self._validate_klant(data.get("klant"))
        if klant_error:
            errors.append(klant_error)
        
        # Validate optional fields
        timestamp_error = self._validate_timestamp(data.get("submissionTimestamp"))
        if timestamp_error:
            errors.append(timestamp_error)
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors
        )
    
    def _validate_mrns(self, mrns) -> List[str]:
        """
        Validate MRN format (8-20 alphanumeric uppercase)
        
        Args:
            mrns: List of MRN strings
            
        Returns:
            List of error messages
        """
        errors = []
        
        if not mrns:
            errors.append("At least one MRN number is required")
            return errors
        
        if not isinstance(mrns, list):
            errors.append("MRNs must be provided as an array")
            return errors
        
        if len(mrns) == 0:
            errors.append("At least one MRN number is required")
            return errors
        
        for mrn in mrns:
            if not mrn:
                errors.append("Empty MRN value not allowed")
                continue
            
            # Sanitize
            mrn = str(mrn).strip().upper()
            
            # Check format
            if not self.MRN_PATTERN.match(mrn):
                errors.append(f"Invalid MRN format: '{mrn}'. Must be 8-20 alphanumeric characters (uppercase)")
        
        return errors
    
    def _validate_reference(self, reference: str) -> str:
        """
        Validate reference format (4-15 alphanumeric uppercase)
        
        Args:
            reference: Reference string
            
        Returns:
            Error message or None
        """
        if not reference:
            return "Reference number is required"
        
        # Sanitize
        reference = str(reference).strip().upper()
        
        # Check format
        if not self.REFERENCE_PATTERN.match(reference):
            return f"Invalid reference format: '{reference}'. Must be 4-15 alphanumeric characters (uppercase)"
        
        return None
    
    def _validate_klant(self, klant: str) -> str:
        """
        Validate client name (2-50 letters/spaces)
        
        Args:
            klant: Client name string
            
        Returns:
            Error message or None
        """
        if not klant:
            return "Client name (Klant) is required"
        
        # Don't uppercase klant - keep original case
        klant = str(klant).strip()
        
        # Check format
        if not self.KLANT_PATTERN.match(klant):
            return f"Invalid client name format: '{klant}'. Must be 2-50 characters (letters and spaces only)"
        
        return None
    
    def _validate_timestamp(self, timestamp: str) -> str:
        """
        Validate ISO 8601 timestamp format (optional field)
        
        Args:
            timestamp: ISO timestamp string
            
        Returns:
            Error message or None
        """
        if not timestamp:
            return None  # Optional field
        
        try:
            from datetime import datetime
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return None
        except (ValueError, AttributeError):
            return f"Invalid timestamp format: '{timestamp}'. Must be ISO 8601 format (e.g., 2025-01-24T10:30:00Z)"
    
    @staticmethod
    def sanitize_input(value: str, field_type: str) -> str:
        """
        Remove dangerous characters from input
        
        Args:
            value: Input string
            field_type: Type of field (mrn, reference, klant)
            
        Returns:
            Sanitized string
        """
        if not value:
            return ""
        
        value = str(value).strip()
        
        if field_type in ['mrn', 'reference']:
            # Only uppercase alphanumeric
            value = value.upper()
            value = re.sub(r'[^A-Z0-9]', '', value)
        elif field_type == 'klant':
            # Only letters and spaces
            value = re.sub(r'[^A-Za-z\s]', '', value)
            value = ' '.join(value.split())  # Normalize spaces
        
        return value