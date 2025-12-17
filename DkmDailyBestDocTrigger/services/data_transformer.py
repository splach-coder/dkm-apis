import json
import logging
from datetime import datetime
from typing import List, Dict
from ..models.bestemmings_data import (
    BestemmingsData, LineItem, ClientInfo, RecordInfo
)

def transform_client_group(client_month_key: str, records: List[Dict]) -> BestemmingsData:
    """
    Transform a group of records (Historical + New) into BestemmingsData.
    """
    if not records:
        raise ValueError("No records provided for transformation")
    
    try:
        # Use first record for client info (Header info is consistent per client)
        first_record = records[0]
        
        client = ClientInfo(
            naam=first_record.get('CLIENT_NAAM', ''),
            straat_en_nummer=first_record.get('CLIENT_STRAAT_EN_NUMMER', ''),
            postcode=first_record.get('CLIENT_POSTCODE', ''),
            stad=first_record.get('CLIENT_STAD', ''),
            landcode=first_record.get('CLIENT_LANDCODE', ''),
            plda_operatoridentity=first_record.get('CLIENT_PLDA_OPERATORIDENTITY', ''),
            language=first_record.get('CLIENT_LANGUAGE', 'EN')
        )
        
        record_infos = []
        all_line_items = []
        
        for record in records:
            # Format date
            datum = str(record.get('DATUM', ''))
            try:
                if len(datum) == 8 and datum.isdigit():
                    date_obj = datetime.strptime(datum, "%Y%m%d")
                else:
                    date_obj = datetime.now()
                
                formatted_date = date_obj.strftime("%d/%m/%Y")
                date_short = date_obj.strftime("%d/%m/%y")
            except ValueError:
                date_obj = datetime.now()
                formatted_date = date_obj.strftime("%d/%m/%Y")
                date_short = date_obj.strftime("%d/%m/%y")
            
            reference = str(record.get('REFERENTIE_KLANT', '')).replace('\r\n', '\n').replace('\r', '\n')
            
            # Create record info
            record_info = RecordInfo(
                internfactuurnummer=int(record.get('INTERNFACTUURNUMMER', 0)),
                processfactuurnummer=int(record.get('PROCESSFACTUURNUMMER', 0)),
                datum=date_short,
                formatted_date=formatted_date,
                mrn=str(record.get('MRN', '')),
                declarationid=int(record.get('DECLARATIONID', 0)),
                exportername=str(record.get('EXPORTERNAME', '')),
                reference=reference,
                klant=str(record.get('KLANT', '')) # <--- MAPPED HERE
            )
            record_infos.append(record_info)
            
            # Parse line items
            line_items_str = record.get('LINE_ITEMS', '[]')
            if isinstance(line_items_str, str):
                try:
                    line_items = json.loads(line_items_str)
                except json.JSONDecodeError:
                    line_items = []
            else:
                line_items = line_items_str if isinstance(line_items_str, list) else []
            
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
                except (ValueError, TypeError):
                    continue
        
        # Sort items numerically by Item Number (zendtarieflijnnummer)
        all_line_items.sort(key=lambda x: x.zendtarieflijnnummer)
        
        return BestemmingsData(
            client=client,
            records=record_infos,
            line_items=all_line_items
        )
        
    except Exception as e:
        logging.error(f"Transform error for group {client_month_key}: {str(e)}")
        raise

def validate_group_consistency(records: List[Dict]) -> bool:
    if not records: return False
    return True
