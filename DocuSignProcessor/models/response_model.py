"""
Response models for DocuSignProcessor.
"""
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class DocuSignResponse:
    success: bool
    message: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    envelope_id: Optional[str] = None
    envelope_status: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
