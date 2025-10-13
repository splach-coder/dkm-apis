from dataclasses import dataclass
from typing import List

@dataclass
class LineItem:
    """Individual line item in debenote"""
    goederencode: str
    goederenomschrijving: str
    aantal_gewicht: float
    verkoopwaarde: float
    netmass: float
    supplementaryunits: float
    zendtarieflijnnummer: int
    typepackages: int

@dataclass
class ClientInfo:
    """Client information from RELATIONS table"""
    relatiecode: str
    naam: str
    straat_en_nummer: str
    postcode: str
    stad: str
    landcode: str
    plda_operatoridentity: str
    language: str

@dataclass
class DebenoteData:
    """Complete debenote data structure"""
    # Header
    internfactuurnummer: int
    processfactuurnummer: int
    btwnummer: str
    datum: str  # formatted: 08/10/2025
    jaar: str
    periode: str
    factuurtotaal: float
    munt: str
    
    # References
    commercialreference: str
    referentie_klant: str
    c88nummer: int
    
    # Client & Supplier
    client: ClientInfo
    relatiecode_leverancier: str
    leverancier_naam: str
    
    # Line items
    line_items: List[LineItem]
    
    # Computed fields
    amount_in_words: str
    vatnote: str  # PROCESSFACTUURNUMMER-DATE
    formatted_total: str  # €58,154.95