"""
File utility functions for common file operations.
"""

import os
import tempfile
from pathlib import Path
from typing import List, Optional
from datetime import datetime


def is_excel_file(filename: str) -> bool:
    """Check if file is an Excel file based on extension."""
    excel_extensions = [".xlsx", ".xls", ".xlsm", ".xlsb"]
    return any(filename.lower().endswith(ext) for ext in excel_extensions)


def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def setup_temp_directory() -> str:
    """Setup temporary directory structure for processing."""
    temp_dir = tempfile.mkdtemp(prefix="cos_excel_processor_")

    directories = {
        "input_dir": os.path.join(temp_dir, "input"),
        "output_dir": os.path.join(temp_dir, "output"),
        "archive_dir": os.path.join(temp_dir, "archive"),
        "logs_dir": os.path.join(temp_dir, "logs"),
    }

    for dir_path in directories.values():
        os.makedirs(dir_path, exist_ok=True)

    return temp_dir


def cleanup_temp_directory(temp_dir: str) -> None:
    """Clean up temporary directory."""
    try:
        import shutil

        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    except Exception:
        pass


def get_filename_from_path(file_path: str) -> str:
    """Extract filename from full path."""
    return os.path.basename(file_path)


def create_archive_filename(original_filename: str, success: bool = True) -> str:
    """Create archive filename with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    status = "success" if success else "failed"
    name, ext = os.path.splitext(original_filename)
    return f"{name}_{timestamp}{ext}"
