from dataclasses import dataclass
from typing import List, Dict

@dataclass
class PDFResponse:
    """Individual PDF response"""
    internfactuurnummer: int
    filename: str
    pdf_base64: str
    size_bytes: int
    metadata: Dict

@dataclass
class APIResponse:
    """Complete API response"""
    success: bool
    timestamp: str
    processed_count: int
    processed_ids: List[int]
    last_processed_id: int
    pdfs: List[Dict]
    errors: List[Dict]
