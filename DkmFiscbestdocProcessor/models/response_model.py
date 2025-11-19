from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class PDFResponse:
    """Individual PDF response for a client-month group"""
    client_month_key: str
    filename: str
    pdf_base64: str
    size_bytes: int
    metadata: Dict
    internfactuurnummer_list: List[int]  # All IDs included in this PDF

@dataclass
class ProcessingGroup:
    """Processing result for one client-month group"""
    client_month_key: str
    success: bool
    pdf_response: Optional[PDFResponse] = None
    error: Optional[str] = None
    record_count: int = 0

@dataclass
class APIResponse:
    """Complete API response for grouped processing"""
    success: bool
    timestamp: str
    processed_groups: int
    total_records: int
    pdfs: List[Dict]
    errors: List[Dict]
    processing_summary: Dict[str, int]  # Stats by client-month
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.processed_groups == 0:
            return 0.0
        successful = sum(1 for pdf in self.pdfs if pdf.get("success", False))
        return successful / self.processed_groups