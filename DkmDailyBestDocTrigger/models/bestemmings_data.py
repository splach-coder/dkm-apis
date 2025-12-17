from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class LineItem:
    """Individual line item from LINE_ITEMS JSON"""
    goederenomschrijving: str
    goederencode: str
    aantal_gewicht: float
    verkoopwaarde: float
    zendtarieflijnnummer: int
    netmass: float
    source_internfactuurnummer: Optional[int] = None

@dataclass
class ClientInfo:
    """Client information"""
    naam: str
    straat_en_nummer: str
    postcode: str
    stad: str
    landcode: str
    plda_operatoridentity: str
    language: str

@dataclass
class RecordInfo:
    """Individual record info for merged PDFs"""
    internfactuurnummer: int
    processfactuurnummer: int
    datum: str
    formatted_date: str
    mrn: str
    declarationid: int
    exportername: str
    reference: str
    klant: str  # <--- ADDED THIS FIELD

@dataclass
class BestemmingsData:
    """Complete Bestemmingsdocument data - supports multiple records"""
    client: ClientInfo
    records: List[RecordInfo]
    line_items: List[LineItem]
    
    @property
    def internfactuurnummer_list(self) -> List[int]:
        return [record.internfactuurnummer for record in self.records]
    
    @property
    def primary_record(self) -> RecordInfo:
        return self.records[0] if self.records else None
    
    @property
    def total_value(self) -> float:
        return sum(item.verkoopwaarde for item in self.line_items)
    
    @property
    def date_range(self) -> str:
        if not self.records:
            return ""
        dates = [record.datum for record in self.records]
        unique_dates = sorted(set(dates))
        if len(unique_dates) == 1:
            return unique_dates[0]
        else:
            return f"{unique_dates[0]} - {unique_dates[-1]}"
