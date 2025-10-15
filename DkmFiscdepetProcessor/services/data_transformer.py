import json
import logging
from datetime import datetime
from typing import List
from ..models.debenote_data import DebenoteData, LineItem, ClientInfo
from .number_to_words import amount_to_words

def transform_row(row: dict) -> DebenoteData:
    """
    Transform SQL row to DebenoteData object
    
    Args:
        row: Dictionary from SQL ResultSet
        
    Returns:
        DebenoteData object ready for PDF generation
    """
    try:
        # Parse LINE_ITEMS JSON string
        line_items = parse_line_items(row.get('LINE_ITEMS', '[]'))
        
        # Format date
        formatted_date = format_date(row['DATUM'])
        
        # Create client info
        client = ClientInfo(
            relatiecode=row['RELATIECODE_KLANT'],
            fullName=row['IMPORTERNAME'],
            naam=row['KLANT'],
            straat_en_nummer=row.get('CLIENT_STRAAT_EN_NUMMER', ''),
            postcode=row.get('CLIENT_POSTCODE', ''),
            stad=row.get('CLIENT_STAD', ''),
            landcode=row.get('CLIENT_LANDCODE', ''),
            plda_operatoridentity=row.get('CLIENT_PLDA_OPERATORIDENTITY', ''),
            language=row.get('CLIENT_LANGUAGE', 'EN')
        )
        
        # Calculate amount in words
        amount_words = amount_to_words(
            row['FACTUURTOTAAL'],
            row['MUNT'],
            client.language
        )
        
        # Create VATNOTE (PROCESSFACTUURNUMMER-DATE)
        vatnote = f"{row['PROCESSFACTUURNUMMER']}-{formatted_date}"
        
        # Format total amount
        formatted_total = format_amount(row['FACTUURTOTAAL'], row['MUNT'])
        
        # Build DebenoteData object
        debenote = DebenoteData(
            internfactuurnummer=row['INTERNFACTUURNUMMER'],
            processfactuurnummer=row['PROCESSFACTUURNUMMER'],
            btwnummer=row['BTWNUMMER'],
            datum=formatted_date,
            jaar=row['JAAR'],
            periode=row['PERIODE'],
            factuurtotaal=row['FACTUURTOTAAL'],
            munt=row['MUNT'],
            commercialreference=row.get('COMMERCIALREFERENCE', ''),
            referentie_klant=row.get('REFERENTIE_KLANT', ''),
            c88nummer=row['C88NUMMER'],
            client=client,
            email=row.get('NAME', ''),
            relatiecode_leverancier=row['RELATIECODE_LEVERANCIER'],
            leverancier_naam=row['LEVERANCIERSNAAM'],
            line_items=line_items,
            amount_in_words=amount_words,
            vatnote=vatnote,
            formatted_total=formatted_total
        )
        
        return debenote
        
    except Exception as e:
        logging.error(f"Error transforming row: {str(e)}")
        raise


def parse_line_items(line_items_str: str) -> List[LineItem]:
    """
    Parse LINE_ITEMS JSON string to array of LineItem objects
    
    Args:
        line_items_str: JSON string from Oracle (e.g., '[{"goederencode":"..."}]')
        
    Returns:
        List of LineItem objects
    """
    try:
        items_data = json.loads(line_items_str)
        
        items = []
        for item in items_data:
            line_item = LineItem(
                goederencode=item.get('goederencode', ''),
                goederenomschrijving=item.get('goederenomschrijving', ''),
                aantal_gewicht=float(item.get('aantal_gewicht', 0)),
                verkoopwaarde=float(item.get('verkoopwaarde', 0)),
                netmass=float(item.get('netmass', 0)),
                supplementaryunits=float(item.get('supplementaryunits', 0)),
                zendtarieflijnnummer=int(item.get('zendtarieflijnnummer', 0)),
                typepackages=str(item.get('typepackages', 0))
            )
            items.append(line_item)
        
        return items
        
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse LINE_ITEMS JSON: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Error parsing line items: {str(e)}")
        return []


def format_date(date_str: str) -> str:
    """
    Format date from YYYYMMDD to DD/MM/YYYY
    
    Args:
        date_str: Date string like "20251008"
        
    Returns:
        Formatted date like "08/10/2025"
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj.strftime("%d/%m/%Y")
    except Exception as e:
        logging.warning(f"Failed to format date {date_str}: {str(e)}")
        return date_str


def format_amount(amount: float, currency: str) -> str:
    """
    Format amount with currency symbol
    
    Args:
        amount: Numeric amount (e.g., 58154.95)
        currency: Currency code (e.g., "EUR")
        
    Returns:
        Formatted string like "€58,154.95"
    """
    currency_symbols = {
        "EUR": "€",
        "USD": "$",
        "GBP": "£"
    }
    
    symbol = currency_symbols.get(currency, currency)
    return f"{symbol}{amount:,.2f}"


def clean_text(text: str) -> str:
    """
    Clean text by removing extra whitespace and line breaks
    
    Args:
        text: Raw text with \r\n and extra spaces
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Replace \r\n with spaces
    cleaned = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    
    # Remove multiple spaces
    cleaned = ' '.join(cleaned.split())
    
    return cleaned.strip()