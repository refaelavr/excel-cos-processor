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

    def __init__(
        self, service_name: str = "ExcelProcessor", processed_filename: str = None
    ):
        self.service_name = service_name
        self.processed_filename = processed_filename
        self.logger = self._setup_logger()

        # Create file logger immediately if filename is provided
        if self.processed_filename:
            self.create_file_logger(self.processed_filename)

        self._log_startup_info()

    def capture_all_output(self) -> None:
        """Capture all stdout and stderr to the current log file."""
        import sys
        from io import StringIO

        class LoggingStream:
            def __init__(self, original_stream, logger, level):
                self.original_stream = original_stream
                self.logger = logger
                self.level = level
                self.buffer = ""

            def write(self, text):
                # Write to original stream
                self.original_stream.write(text)
                self.original_stream.flush()

                # Add to buffer
                self.buffer += text

                # If we have a complete line, log it
                if "\n" in self.buffer:
                    lines = self.buffer.split("\n")
                    for line in lines[
                        :-1
                    ]:  # All but the last (which might be incomplete)
                        if line.strip():  # Only log non-empty lines
                            self.logger.log(self.level, f"STDOUT: {line}")
                    self.buffer = lines[-1]  # Keep the last (possibly incomplete) line

            def flush(self):
                self.original_stream.flush()
                if self.buffer.strip():
                    self.logger.log(self.level, f"STDOUT: {self.buffer}")
                    self.buffer = ""

        # Replace stdout and stderr
        sys.stdout = LoggingStream(sys.stdout, self.logger, logging.INFO)
        sys.stderr = LoggingStream(sys.stderr, self.logger, logging.ERROR)

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
            # Note: File-specific logger will be created later via create_file_logger()
            # This ensures we only have one log file per processing run

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

    def create_file_logger(self, processed_filename: str) -> None:
        """Create a new file handler with the processed filename."""
        try:
            import os

            self.logger.info(f"Creating file logger for: {processed_filename}")
            self.logger.info(f"Current working directory: {os.getcwd()}")
            self.logger.info(f"Current directory contents: {os.listdir('.')}")

            today = datetime.now().strftime("%Y%m%d")
            log_dir = Path("logs") / today
            self.logger.info(f"Log directory: {log_dir}")
            self.logger.info(f"Absolute log directory: {log_dir.absolute()}")

            log_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Log directory created/verified: {log_dir}")
            self.logger.info(f"Log directory exists: {log_dir.exists()}")
            self.logger.info(
                f"Log directory is writable: {os.access(log_dir, os.W_OK)}"
            )

            # Clean the filename for use in log filename
            # First, extract just the filename without path
            import os

            self.logger.info(f"Original processed_filename: '{processed_filename}'")
            base_filename = os.path.basename(processed_filename)
            self.logger.info(f"Extracted base_filename: '{base_filename}'")

            # Then clean the filename
            clean_filename = "".join(
                c for c in base_filename if c.isalnum() or c in (" ", "-", "_")
            ).rstrip()
            clean_filename = clean_filename.replace(" ", "_")
            self.logger.info(f"Cleaned filename: '{clean_filename}'")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"{clean_filename}_{timestamp}.log"

            log_file = log_dir / log_filename
            self.logger.info(f"Log file path: {log_file}")

            file_handler = logging.FileHandler(
                filename=str(log_file), mode="a", encoding="utf-8"
            )
            file_handler.setLevel(logging.INFO)

            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_formatter)

            self.logger.addHandler(file_handler)
            self.logger.info(f"Successfully created file logger: {log_filename}")
            self.logger.info(f"Log file will be saved to: {log_file}")

            # Test write to the log file
            try:
                test_message = (
                    f"=== LOG FILE CREATED AT {datetime.now().isoformat()} ==="
                )
                self.logger.info(test_message)
                self.logger.info(f"Current handlers: {len(self.logger.handlers)}")
                for i, handler in enumerate(self.logger.handlers):
                    self.logger.info(f"Handler {i}: {type(handler).__name__}")
            except Exception as test_e:
                self.logger.error(f"Error testing log file write: {test_e}")

            # Capture all stdout and stderr to the log file
            self._capture_stdout_stderr(log_file)

        except Exception as e:
            import traceback

            self.logger.error(f"Could not create file logger: {str(e)}")
            self.logger.error(f"Exception details: {traceback.format_exc()}")

    def _capture_stdout_stderr(self, log_file: Path) -> None:
        """Capture all stdout and stderr to the log file."""
        import sys
        from io import StringIO

        class LoggingStream:
            def __init__(self, original_stream, logger, level):
                self.original_stream = original_stream
                self.logger = logger
                self.level = level
                self.buffer = ""

            def write(self, text):
                # Write to original stream
                self.original_stream.write(text)
                self.original_stream.flush()

                # Add to buffer
                self.buffer += text

                # If we have a complete line, log it
                if "\n" in self.buffer:
                    lines = self.buffer.split("\n")
                    for line in lines[
                        :-1
                    ]:  # All but the last (which might be incomplete)
                        if line.strip():  # Only log non-empty lines
                            self.logger.log(self.level, f"STDOUT: {line}")
                    self.buffer = lines[-1]  # Keep the last (possibly incomplete) line

            def flush(self):
                self.original_stream.flush()
                if self.buffer.strip():
                    self.logger.log(self.level, f"STDOUT: {self.buffer}")
                    self.buffer = ""

        # Replace stdout and stderr
        sys.stdout = LoggingStream(sys.stdout, self.logger, logging.INFO)
        sys.stderr = LoggingStream(sys.stderr, self.logger, logging.ERROR)
