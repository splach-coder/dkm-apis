import json
import logging
from datetime import datetime
from typing import List, Dict
from ..models.bestemmings_data import (
    BestemmingsData, LineItem, ClientInfo, RecordInfo
)


def transform_client_group(client_month_key: str, records: List[Dict]) -> BestemmingsData:
    """
    Transform a group of records for the same client-month into BestemmingsData.
    
    Args:
        client_month_key: Key like "EUROFINS_202511"
        records: List of records with FULL table data from state
    
    Returns:
        BestemmingsData with merged line items and record info
    """
    if not records:
        raise ValueError("No records provided for transformation")
    
    try:
        # Use first record for client info (should be same for all in group)
        first_record = records[0]
        
        # Create client info
        client = ClientInfo(
            naam=first_record.get('CLIENT_NAAM', ''),
            straat_en_nummer=first_record.get('CLIENT_STRAAT_EN_NUMMER', ''),
            postcode=first_record.get('CLIENT_POSTCODE', ''),
            stad=first_record.get('CLIENT_STAD', ''),
            landcode=first_record.get('CLIENT_LANDCODE', ''),
            plda_operatoridentity=first_record.get('CLIENT_PLDA_OPERATORIDENTITY', ''),
            language=first_record.get('CLIENT_LANGUAGE', 'EN')
        )
        
        # Process all records
        record_infos = []
        all_line_items = []
        
        for record in records:
            # Format date - HANDLE YYYYMMDD FORMAT
            datum = str(record.get('DATUM', ''))
            
            try:
                if len(datum) == 8 and datum.isdigit():
                    # Format: YYYYMMDD
                    date_obj = datetime.strptime(datum, "%Y%m%d")
                else:
                    # Fallback - use current date
                    date_obj = datetime.now()
                    logging.warning(f"Could not parse date {datum}, using current date")
                
                formatted_date = date_obj.strftime("%d/%m/%Y")
                date_short = date_obj.strftime("%d/%m/%y")
                
            except ValueError as e:
                logging.error(f"Date parsing error for {datum}: {e}")
                date_obj = datetime.now()
                formatted_date = date_obj.strftime("%d/%m/%Y")
                date_short = date_obj.strftime("%d/%m/%y")
            
            # Clean reference
            reference = record.get('REFERENTIE_KLANT', '')
            reference = str(reference).replace('\r\n', '\n').replace('\r', '\n')
            
            # Create record info
            record_info = RecordInfo(
                internfactuurnummer=int(record.get('INTERNFACTUURNUMMER', 0)),
                processfactuurnummer=int(record.get('PROCESSFACTUURNUMMER', 0)),
                datum=date_short,
                formatted_date=formatted_date,
                mrn=str(record.get('MRN', '')),
                declarationid=int(record.get('DECLARATIONID', 0)),
                exportername=str(record.get('EXPORTERNAME', '')),
                reference=reference
            )
            record_infos.append(record_info)
            
            # Parse and add line items from stored JSON string
            line_items_str = record.get('LINE_ITEMS', '[]')
            if isinstance(line_items_str, str):
                try:
                    line_items = json.loads(line_items_str)
                except json.JSONDecodeError:
                    logging.error(f"Failed to parse LINE_ITEMS JSON: {line_items_str[:100]}")
                    line_items = []
            else:
                line_items = line_items_str if isinstance(line_items_str, list) else []
            
            # Add each line item with source tracking
            for item in line_items:
                try:
                    line_item = LineItem(
                        goederenomschrijving=str(item.get('goederenomschrijving', '')),
                        goederencode=str(item.get('goederencode', '')),
                        aantal_gewicht=float(item.get('aantal_gewicht', 0)),
                        verkoopwaarde=float(item.get('verkoopwaarde', 0)),
                        zendtarieflijnnummer=int(item.get('zendtarieflijnnummer', 0)),
                        netmass=float(item.get('netmass', 0)),
                        source_internfactuurnummer=int(record.get('INTERNFACTUURNUMMER', 0))
                    )
                    all_line_items.append(line_item)
                except (ValueError, TypeError) as e:
                    logging.error(f"Failed to create LineItem from {item}: {e}")
                    continue
        
        logging.info(f"✅ Transformed group {client_month_key}: {len(record_infos)} records, {len(all_line_items)} line items")
        
        return BestemmingsData(
            client=client,
            records=record_infos,
            line_items=all_line_items
        )
        
    except Exception as e:
        logging.error(f"Transform error for group {client_month_key}: {str(e)}")
        raise


def validate_group_consistency(records: List[Dict]) -> bool:
    """
    Validate that all records in a group have consistent client info.
    
    Args:
        records: List of records for same group
    
    Returns:
        True if consistent, False otherwise
    """
    if not records:
        logging.warning("validate_group_consistency: No records provided")
        return False
    
    if len(records) == 1:
        return True
    
    first_record = records[0]
    reference_client = {
        'CLIENT_NAAM': first_record.get('CLIENT_NAAM', ''),
        'CLIENT_LANDCODE': first_record.get('CLIENT_LANDCODE', ''),
        'CLIENT_LANGUAGE': first_record.get('CLIENT_LANGUAGE', ''),
        'KLANT': first_record.get('KLANT', '')
    }
    
    for i, record in enumerate(records[1:], 1):
        for field, expected_value in reference_client.items():
            record_value = record.get(field, '')
            if record_value != expected_value:
                logging.warning(f"Inconsistent {field} in record {i}: expected '{expected_value}' got '{record_value}'")
                return False
    
    logging.info(f"✅ Group consistency validated for {len(records)} records")
    return True