"""
Logging service for centralized log management.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from utils.environment_utils import get_job_info


class LoggingService:
    """Centralized logging service for the application."""

    def __init__(self, service_name: str = "ExcelProcessor"):
        self.service_name = service_name
        self.logger = self._setup_logger()
        self._log_startup_info()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging with immediate console output for IBM Cloud Code Engine."""
        logger = logging.getLogger(self.service_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Console handler for immediate IBM Cloud console visibility
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)

            # Force immediate flushing for real-time log visibility
            console_handler.setStream(sys.stdout)

            # Formatter for console with job run ID
            job_info = get_job_info()
            job_run_id = job_info.get("job_run_id", "unknown")
            console_formatter = logging.Formatter(
                f"%(asctime)s - JOB_RUN:{job_run_id} - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            console_handler.setFormatter(console_formatter)

            # Add console handler first for immediate visibility
            logger.addHandler(console_handler)

            # File handler for local logs (only if we can create the directory)
            try:
                today = datetime.now().strftime("%d%m%Y")
                log_dir = Path("logs") / today
                log_dir.mkdir(parents=True, exist_ok=True)

                log_file = log_dir / "complete_excel_processor.log"

                file_handler = logging.FileHandler(
                    filename=str(log_file), mode="a", encoding="utf-8"
                )
                file_handler.setLevel(logging.INFO)

                file_formatter = logging.Formatter(
                    "%(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
                file_handler.setFormatter(file_formatter)

                logger.addHandler(file_handler)

            except Exception as e:
                # If file logging fails, at least we have console logging
                print(f"Warning: Could not setup file logging: {str(e)}")

        return logger

    def _log_startup_info(self) -> None:
        """Log startup information."""
        job_info = get_job_info()
        self.logger.info("=== EXCEL PROCESSOR STARTING ===")
        self.logger.info(f"JOB_RUN_ID: {job_info.get('job_run_id')}")
        self.logger.info(f"JOB_NAME: {job_info.get('job_name')}")
        self.logger.info("=== PROCESSING START ===")
        sys.stdout.flush()

    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)
        sys.stdout.flush()

    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)
        sys.stderr.flush()

    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)
        sys.stdout.flush()

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)
        sys.stdout.flush()

    def log_processing_result(
        self, success: bool, file_name: str, error_message: Optional[str] = None
    ) -> None:
        """Log processing result."""
        if success:
            self.info(f"Successfully processed: {file_name}")
            self.info("=== PROCESSING END - SUCCESS ===")
        else:
            self.error(f"Failed to process: {file_name}")
            if error_message:
                self.error(f"Error: {error_message}")
            self.info("=== PROCESSING END - FAILED ===")

    def log_environment_info(self) -> None:
        """Log environment information."""
        from utils.environment_utils import log_environment_variables

        log_environment_variables(self.logger)

    def flush(self) -> None:
        """Force flush all log handlers."""
        sys.stdout.flush()
        sys.stderr.flush()
        for handler in self.logger.handlers:
            handler.flush()
