"""
NCTS schema transformation service for D&G Arrival Requests
"""
import logging
import json
from datetime import datetime
from typing import Dict, List


class NCTSTransformer:
    """
    Transforms validated form data to NCTS arrival notification schema
    """
    
    # Default values
    DEFAULT_CUSTOMS_OFFICE = "BE000600"  # Belgian customs office
    DEFAULT_UNLOCODE = "BEANR"  # Antwerp
    DEFAULT_LOCATION_TYPE = "B"
    DEFAULT_QUALIFIER = "U"
    DEFAULT_RELATION_GROUP = "DKM"
    DEFAULT_LANGUAGE = "EN"
    
    def transform(self, form_data: Dict) -> Dict:
        """
        Main transformation method - converts form data to NCTS schema
        
        Args:
            form_data: Validated form data
            
        Returns:
            NCTS-compliant JSON payload
        """
        logging.info("Starting NCTS transformation")
        
        try:
            ncts_payload = {
                "$type": "Arrival.Notification",
                "format": "ncts",
                "language": self.DEFAULT_LANGUAGE,
                "declaration": self._build_declaration_section(form_data),
                "master": self._build_master_section(form_data),
                "integration": self._build_integration_section(form_data)
            }
            
            logging.info("NCTS transformation completed successfully")
            return ncts_payload
            
        except Exception as e:
            logging.error(f"Transformation error: {str(e)}")
            raise TransformationError(f"Failed to transform data: {str(e)}")
    
    def _build_declaration_section(self, data: Dict) -> Dict:
        """
        Build declaration section of NCTS schema
        
        Args:
            data: Form data
            
        Returns:
            Declaration section dict
        """
        mrns = data.get("mrns", [])
        reference = data.get("reference", "")
        klant = data.get("klant", "")
        timestamp = data.get("submissionTimestamp") or datetime.utcnow().isoformat() + "Z"
        
        return {
            "lrn": reference,  # Local Reference Number
            "mrn": mrns[0] if mrns else "",  # Primary MRN
            "simplifiedProcedure": False,
            "incidentFlag": False,
            "arrivalNotificationDateTime": timestamp,
            "authorisation": [],
            "customsOfficeOfDestination": {
                "referenceNumber": self.DEFAULT_CUSTOMS_OFFICE
            },
            "traderAtDestination": {
                "references": {
                    "internal": reference
                },
                "name": klant,
                "phoneNumber": "",
                "identificationNumber": "",
                "emailAddress": "",
                "communicationLanguageAtDestination": self.DEFAULT_LANGUAGE
            }
        }
    
    def _build_master_section(self, data: Dict) -> Dict:
        """
        Build master section with location of goods
        
        Args:
            data: Form data
            
        Returns:
            Master section dict
        """
        reference = data.get("reference", "")
        
        return {
            "locationOfGoods": {
                "internalReference": reference,
                "unlocode": self.DEFAULT_UNLOCODE,
                "typeOfLocation": self.DEFAULT_LOCATION_TYPE,
                "qualifierOfIdentification": self.DEFAULT_QUALIFIER,
                "authorisationNumber": "",
                "additionalIdentifier": ""
            }
        }
    
    def _build_integration_section(self, data: Dict) -> Dict:
        """
        Build integration section with DKM-specific settings
        
        Args:
            data: Form data
            
        Returns:
            Integration section dict
        """
        reference = data.get("reference", "")
        klant = data.get("klant", "")
        mrns = data.get("mrns", [])
        
        # Handle multiple MRNs
        external_refs = self._handle_multiple_mrns(mrns)
        
        return {
            "language": self.DEFAULT_LANGUAGE,
            "sendingMode": "BATCH",
            "templateCode": "ARRIVAL_NCTS",
            "printGroup": "DEFAULT",
            "externalReferences": external_refs,
            "createDeclaration": True,
            "autoSendDeclaration": True,
            "simplifiedProcedure": False,
            "consolidateBeforeSending": False,
            "principal": {
                "references": {
                    "internal": reference
                },
                "contactPerson": {
                    "references": {
                        "internal": reference
                    },
                    "name": klant
                },
                "sendMail": False,
                "contactPersonExportConfirmation": {
                    "references": {
                        "internal": reference
                    },
                    "name": klant
                },
                "sendMailExportConfirmation": False
            },
            "control": {
                "packages": 0,
                "grossmass": 0.0,
                "netmass": 0.0
            },
            "relationGroup": self.DEFAULT_RELATION_GROUP,
            "commercialReference": reference,
            "variableFields": [],
            "procedureType": "NCTS",
            "declarationCreatedBy": "DKM_ARRIVAL_FORM",
            "attachment": []
        }
    
    def _handle_multiple_mrns(self, mrns: List[str]) -> Dict:
        """
        Handle multiple MRNs by storing additional ones in metadata
        
        Args:
            mrns: List of MRN strings
            
        Returns:
            External references dict
        """
        external_refs = {
            "LinkIdErp1": None,
            "LinkIdErp2": None,
            "LinkIdErp3": None,
            "LinkIdErp4": None,
            "LinkIdErp5": None
        }
        
        # If multiple MRNs, store additional ones in LinkIdErp1
        if len(mrns) > 1:
            additional_mrns = mrns[1:]
            external_refs["LinkIdErp1"] = json.dumps({
                "additional_mrns": additional_mrns,
                "total_count": len(mrns)
            })
            logging.info(f"Stored {len(additional_mrns)} additional MRNs in metadata")
        
        return external_refs


class TransformationError(Exception):
    """Raised when schema transformation fails"""
    pass