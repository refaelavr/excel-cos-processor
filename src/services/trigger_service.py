"""
Trigger service for handling IBM Cloud Code Engine trigger events.
"""

import os
import sys
from typing import Optional
from models.processing_result import TriggerInfo
from utils.environment_utils import (
    extract_filename_from_trigger,
    get_job_info,
    get_environment,
    is_code_engine_job,
)


class TriggerService:
    """Service for handling trigger events and extracting file information."""

    def __init__(self, logger):
        self.logger = logger

    def extract_trigger_info(self) -> Optional[TriggerInfo]:
        """Extract trigger information from IBM Cloud Code Engine event."""
        if not is_code_engine_job():
            self.logger.warning("Not running as Code Engine job")
            return None

        # Get job information
        job_info = get_job_info()
        environment = get_environment()

        # Extract filename from trigger
        filename = extract_filename_from_trigger()

        if not filename:
            self.logger.error(
                "CRITICAL: Could not extract filename from trigger event data"
            )
            self.logger.error(
                "This could cause race conditions with simultaneous file uploads"
            )
            self.logger.error(
                "Please check IBM Cloud Code Engine trigger configuration"
            )
            return None

        # Create trigger info
        trigger_info = TriggerInfo(
            filename=os.path.basename(filename),
            cos_key=filename,
            job_run_id=job_info.get("job_run_id", "unknown"),
            job_name=job_info.get("job_name", "unknown"),
            environment=environment,
            timestamp=job_info.get("timestamp"),
            metadata=job_info,
        )

        self.logger.info(f"Extracted trigger info: {trigger_info.filename}")
        return trigger_info

    def is_production_mode(self) -> bool:
        """Check if running in production mode."""
        return is_code_engine_job() and get_environment() == "prod"

    def get_filename_from_args(self) -> Optional[str]:
        """Get filename from command line arguments."""
        if len(sys.argv) > 1:
            # Join all arguments to handle filenames with spaces
            filename = " ".join(sys.argv[1:])
            self.logger.info(f"Filename from command line: {filename}")
            return filename
        return None

    def get_processing_filename(self) -> Optional[str]:
        """Get the filename to process based on environment."""
        if self.is_production_mode():
            # Production: Get filename from trigger
            trigger_info = self.extract_trigger_info()
            if trigger_info:
                return trigger_info.cos_key
            else:
                self.logger.error("Failed to extract trigger information")
                return None
        else:
            # Test mode: Get filename from command line arguments
            filename = self.get_filename_from_args()
            if not filename:
                self.logger.error("No filename provided in test mode")
                self.logger.error("Usage: python app_cloud.py filename.xlsx")
                return None
            return filename

    def log_trigger_debug_info(self) -> None:
        """Log debug information for trigger troubleshooting."""
        self.logger.info("=== Trigger Debug Information ===")

        # Log environment variables
        trigger_vars = [
            "CE_JOB",
            "CE_JOBRUN",
            "CE_SUBJECT",
            "CE_DATA",
            "CE_PROJECT_ID",
            "CE_REGION",
            "ENVIRONMENT",
        ]

        for var in trigger_vars:
            value = os.getenv(var)
            if value:
                if var == "CE_DATA" and len(value) > 100:
                    self.logger.info(f"{var}: {value[:100]}...")
                else:
                    self.logger.info(f"{var}: {value}")
            else:
                self.logger.warning(f"{var}: Not set")

        # Check stdin
        try:
            if not sys.stdin.isatty():
                stdin_data = sys.stdin.read().strip()
                if stdin_data:
                    self.logger.info(f"stdin data: {stdin_data[:200]}...")
                else:
                    self.logger.warning("stdin is available but empty")
            else:
                self.logger.warning("stdin is not available (not running from trigger)")
        except Exception as e:
            self.logger.warning(f"Error reading stdin: {str(e)}")

        self.logger.info("=== End Trigger Debug Information ===")
