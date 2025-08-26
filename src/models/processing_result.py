"""
Data models for processing results and status tracking.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime


@dataclass
class ProcessingResult:
    """Result of a file processing operation."""

    success: bool
    file_name: str
    cos_key: Optional[str] = None
    error_message: Optional[str] = None
    tables_processed: int = 0
    archive_path: Optional[str] = None
    processing_time: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class TriggerInfo:
    """Information extracted from trigger events."""

    filename: str
    cos_key: str
    job_run_id: str
    job_name: str
    environment: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class FileMetadata:
    """File metadata from Cloud Object Storage."""

    size: int
    last_modified: Optional[datetime] = None
    content_type: str = "unknown"
    etag: str = ""
    metadata: Dict[str, str] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
