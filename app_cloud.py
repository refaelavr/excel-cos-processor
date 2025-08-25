#!/usr/bin/env python3
"""
COS Excel Processor for IBM Code Engine - Trigger-Based Single File Processing

Processes the specific Excel file that triggered the Code Engine job.
Each trigger processes exactly one file - no batch processing.

Environment-based file source selection:
- ENVIRONMENT=test: Process files from data/input/ directory (local development)
- ENVIRONMENT=prod: Process specific file from COS bucket (production deployment)

Usage:
- Production (trigger): Automatically receives filename from COS event
- Local testing: python app_cloud.py "test_file.xlsx"
"""

import os
import sys
import json
import logging
import io
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
from dotenv import load_dotenv

# Add src directory to Python path for accessing the existing services
current_dir = Path(__file__).parent
src_path = current_dir / "src"
config_path = current_dir / "config"

sys.path.insert(0, str(src_path))
sys.path.insert(0, str(config_path))

# Terminal capture utilities for log collection
_TERMINAL_CAPTURE_BUFFER: Optional[io.StringIO] = None
_ORIG_STDOUT = None
_ORIG_STDERR = None


class _TeeStream:
    """Stream that writes to both original stream and a buffer for log capture."""

    def __init__(self, original_stream, buffer: io.StringIO):
        self.original_stream = original_stream
        self.buffer = buffer

    def write(self, data):
        try:
            self.buffer.write(data)
        except Exception:
            pass
        return self.original_stream.write(data)

    def flush(self):
        try:
            self.buffer.flush()
        except Exception:
            pass
        return self.original_stream.flush()

    @property
    def encoding(self):
        return getattr(self.original_stream, "encoding", "utf-8")


def _start_terminal_capture():
    """Start capturing all terminal output for log upload."""
    global _TERMINAL_CAPTURE_BUFFER, _ORIG_STDOUT, _ORIG_STDERR
    if _TERMINAL_CAPTURE_BUFFER is not None:
        return
    _TERMINAL_CAPTURE_BUFFER = io.StringIO()
    _ORIG_STDOUT = sys.stdout
    _ORIG_STDERR = sys.stderr
    sys.stdout = _TeeStream(_ORIG_STDOUT, _TERMINAL_CAPTURE_BUFFER)
    sys.stderr = _TeeStream(_ORIG_STDERR, _TERMINAL_CAPTURE_BUFFER)


def _stop_terminal_capture():
    """Stop terminal capture and restore original streams."""
    global _TERMINAL_CAPTURE_BUFFER, _ORIG_STDOUT, _ORIG_STDERR
    if _TERMINAL_CAPTURE_BUFFER is None:
        return
    try:
        sys.stdout = _ORIG_STDOUT
        sys.stderr = _ORIG_STDERR
    finally:
        _ORIG_STDOUT = None
        _ORIG_STDERR = None


# Import existing Excel processing services
try:
    from config_manager import get_config, ConfigManager
    from excel_service import ExcelProcessingService
    from database_service import DatabaseService
    from logger import (
        print_success,
        print_error,
        print_warning,
        print_normal,
        setup_logging,
    )

    print("Successfully imported Excel processing services")
except ImportError as e:
    print(f"Error importing services: {e}")
    print(f"Current directory: {current_dir}")
    print(f"Src path: {src_path}")
    raise


class COSExcelProcessor:
    """
    Single-file Excel processor for trigger-based processing.

    Each instance processes exactly one file that triggered the Code Engine job.
    No batch processing - optimized for cloud event-driven architecture.
    """

    def __init__(self):
        """Initialize processor with environment-specific configuration."""
        self.run_start_time = datetime.now()

        # Load environment variables for local testing
        if not os.getenv("CE_JOB"):  # If not running in Code Engine
            env_files = [".env.cloud", ".env", ".env.local"]
            for env_file in env_files:
                if os.path.exists(env_file):
                    load_dotenv(env_file)
                    print(f"Loaded environment from: {env_file}")
                    break

        self.logger = self._setup_logger()

        # Determine environment mode
        self.environment = os.getenv("ENVIRONMENT", "prod").lower()
        self.logger.info(f"Running in {self.environment.upper()} environment")

        if self.environment == "prod":
            self._validate_environment()
            self._setup_cos_client()
        else:
            self.logger.info("TEST mode: Skipping COS client initialization")
            self.cos_client = None
            self.bucket_name = None

        self._setup_excel_services()
        self.temp_dir = None

    def _upload_captured_logs_to_cos(self):
        """Upload captured terminal output to COS for production audit trail."""
        if self.environment != "prod" or not self.cos_client:
            self.logger.info("Skipping log upload (not in PROD mode or no COS client)")
            return

        try:
            global _TERMINAL_CAPTURE_BUFFER
            if _TERMINAL_CAPTURE_BUFFER is None:
                self.logger.warning(
                    "No terminal capture buffer found; skipping log upload"
                )
                return

            log_text = _TERMINAL_CAPTURE_BUFFER.getvalue()
            if not log_text:
                self.logger.warning(
                    "Terminal capture buffer is empty; skipping log upload"
                )
                return

            date_part = self.run_start_time.strftime("%Y%m%d")
            ts_part = self.run_start_time.strftime("%Y%m%d_%H%M%S")
            object_key = f"logs/{date_part}/excel_processor_{ts_part}.log"

            self.cos_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=log_text.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            self.logger.info(f"Uploaded processing logs to '{object_key}'")
        except Exception as e:
            try:
                self.logger.error(f"Failed to upload logs to COS: {str(e)}")
            except Exception:
                pass

    def _validate_environment(self):
        """Validate required environment variables for cloud deployment."""
        required_vars = ["IAM_API_KEY", "COS_INSTANCE_ID", "BUCKET_NAME"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables for PROD mode: {', '.join(missing_vars)}"
            )

        # Log masked values for debugging
        for var in required_vars:
            value = os.getenv(var)
            if "API_KEY" in var:
                masked = f"{value[:8]}...{value[-4:]}" if len(value) > 12 else "***"
                self.logger.info(f"Environment variable {var}: {masked}")
            else:
                self.logger.info(f"Environment variable {var}: {value}")

    def _setup_cos_client(self):
        """Initialize COS client with IAM authentication."""
        try:
            # Use appropriate endpoint based on environment
            if os.getenv("CE_JOB"):
                endpoint = os.getenv(
                    "COS_INTERNAL_ENDPOINT",
                    "https://s3.private.eu-de.cloud-object-storage.appdomain.cloud",
                )
                self.logger.info("Using Code Engine private endpoint")
            else:
                endpoint = os.getenv(
                    "COS_ENDPOINT",
                    "https://s3.eu-de.cloud-object-storage.appdomain.cloud",
                )
                self.logger.info("Using public endpoint for local testing")

            self.logger.info(f"COS Endpoint: {endpoint}")

            # Create client with timeout configuration
            self.cos_client = ibm_boto3.client(
                "s3",
                ibm_api_key_id=os.getenv("IAM_API_KEY"),
                ibm_service_instance_id=os.getenv("COS_INSTANCE_ID"),
                config=Config(
                    signature_version="oauth", connect_timeout=30, read_timeout=30
                ),
                endpoint_url=endpoint,
            )

            self.bucket_name = os.getenv("BUCKET_NAME")

            self.logger.info(
                f"Successfully initialized COS client with IAM authentication (Endpoint: {endpoint})"
            )

            # Test connection with timeout
            self._test_cos_connection()

        except Exception as e:
            self.logger.error(f"Failed to initialize COS client: {str(e)}")
            raise

    def _test_cos_connection(self):
        """Test COS connectivity with timeout protection."""
        try:
            self.logger.info("Testing COS connection with 10 second timeout...")
            import signal

            def timeout_handler(signum, frame):
                raise TimeoutError("COS connection test timed out")

            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(10)

            try:
                response = self.cos_client.list_buckets()
                bucket_count = len(response.get("Buckets", []))
                self.logger.info(
                    f"COS connection test successful - found {bucket_count} buckets"
                )

                # Verify target bucket exists
                bucket_names = [b["Name"] for b in response.get("Buckets", [])]
                if self.bucket_name in bucket_names:
                    self.logger.info(f"Target bucket '{self.bucket_name}' confirmed")
                else:
                    self.logger.warning(f"Target bucket '{self.bucket_name}' not found")
                    self.logger.info(f"Available buckets: {bucket_names}")

            finally:
                signal.alarm(0)

        except (TimeoutError, Exception) as test_error:
            self.logger.warning(f"COS connection test failed: {str(test_error)}")
            self.logger.info(
                "Continuing without connection test - will test during actual operations"
            )

    def _setup_excel_services(self):
        """Initialize Excel processing and database services."""
        try:
            self.config = get_config()
            self.logger.info("Configuration loaded successfully")

            # Fix SSL certificate path for database
            if hasattr(self.config.database, "sslrootcert"):
                ssl_cert_path = self.config.database.sslrootcert
                if not os.path.isabs(ssl_cert_path):
                    ssl_cert_path = os.path.join(
                        current_dir, "config", "ibm-cloud-cert.crt"
                    )
                    self.config.database.sslrootcert = ssl_cert_path
                    self.logger.info(
                        f"Updated SSL certificate path to: {ssl_cert_path}"
                    )

                if not os.path.exists(ssl_cert_path):
                    self.logger.warning(
                        f"SSL certificate not found at: {ssl_cert_path}"
                    )
                    self.logger.info(
                        "You can disable SSL verification by setting sslmode to 'disable' in db_config.py"
                    )
                else:
                    self.logger.info(f"SSL certificate found at: {ssl_cert_path}")

            # Initialize Excel processing service
            self.excel_service = ExcelProcessingService(self.config)
            self.logger.info("Excel processing service initialized")

            # Initialize database service if enabled
            if self.config.processing.enable_database:
                self.db_service = DatabaseService(self.config.database.to_dict())
                if self.db_service.test_connection():
                    self.logger.info("Database service initialized and connected")
                else:
                    self.logger.warning(
                        "Database connection failed - continuing without DB"
                    )
                    self.config.processing.enable_database = False
            else:
                self.logger.info("Database processing disabled in configuration")

        except Exception as e:
            self.logger.error(f"Failed to initialize Excel services: {str(e)}")
            raise

    def _setup_logger(self) -> logging.Logger:
        """Setup logging with daily log files."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Create daily log directory
            today = datetime.now().strftime("%d%m%Y")
            log_dir = Path("logs") / today
            log_dir.mkdir(parents=True, exist_ok=True)

            log_file = log_dir / "excel_processor.log"

            # File handler - append mode for daily logs
            file_handler = logging.FileHandler(
                filename=str(log_file), mode="a", encoding="utf-8"
            )
            file_handler.setLevel(logging.INFO)

            # Console handler for Code Engine logs
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Formatter
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger

    def _is_excel_file(self, filename: str) -> bool:
        """Check if file is an Excel file based on extension."""
        excel_extensions = [".xlsx", ".xls", ".xlsm", ".xlsb"]
        return any(filename.lower().endswith(ext) for ext in excel_extensions)

    def _get_file_metadata(self, object_key: str) -> Optional[Dict[str, Any]]:
        """Get file metadata from COS."""
        try:
            response = self.cos_client.head_object(
                Bucket=self.bucket_name, Key=object_key
            )

            return {
                "size": response.get("ContentLength", 0),
                "last_modified": response.get("LastModified"),
                "content_type": response.get("ContentType", "unknown"),
                "etag": response.get("ETag", "").strip('"'),
                "metadata": response.get("Metadata", {}),
            }

        except ClientError as e:
            self.logger.error(f"Failed to get metadata for {object_key}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(
                f"Unexpected error getting metadata for {object_key}: {str(e)}"
            )
            return None

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    def _setup_temp_processing_directory(self):
        """Setup temporary directory structure for Excel processing."""
        try:
            self.temp_dir = tempfile.mkdtemp(prefix="excel_processor_")

            directories = {
                "input_dir": os.path.join(self.temp_dir, "input"),
                "output_dir": os.path.join(self.temp_dir, "output"),
                "archive_dir": os.path.join(self.temp_dir, "archive"),
                "logs_dir": os.path.join(self.temp_dir, "logs"),
            }

            for dir_name, dir_path in directories.items():
                os.makedirs(dir_path, exist_ok=True)

            # Update config to use temporary directories
            self.config.processing.input_dir = "input"
            self.config.processing.output_dir = "output"
            self.config.processing.archive_dir = "archive"
            self.config.processing.logs_dir = "logs"

            # Update the absolute paths method to return our temp directories
            def get_temp_absolute_paths():
                return directories

            self.config.processing.get_absolute_paths = get_temp_absolute_paths

            self.logger.info(f"Created temporary processing directory: {self.temp_dir}")
            return directories["input_dir"]

        except Exception as e:
            self.logger.error(f"Error setting up temp directory: {str(e)}")
            raise

    def _cleanup_temp_directory(self):
        """Clean up temporary directory."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            except Exception as e:
                self.logger.warning(f"Error cleaning up temp directory: {str(e)}")

    def _archive_file_to_cos(self, filename: str):
        """Archive processed file to COS archive directory."""
        if self.environment != "prod" or not self.cos_client:
            return

        try:
            archive_date = datetime.now().strftime("%Y%m%d")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            name_without_ext, ext = os.path.splitext(filename)
            archived_filename = f"{name_without_ext}_{timestamp}{ext}"
            archive_key = f"archive/{archive_date}/{archived_filename}"

            # Server-side copy then delete original
            self.cos_client.copy_object(
                Bucket=self.bucket_name,
                Key=archive_key,
                CopySource={"Bucket": self.bucket_name, "Key": filename},
            )

            self.cos_client.delete_object(Bucket=self.bucket_name, Key=filename)
            self.logger.info(f"Archived '{filename}' to '{archive_key}'")

        except Exception as e:
            self.logger.error(f"Error archiving {filename}: {str(e)}")

    def _get_environment_info(self) -> Dict[str, str]:
        """Get environment information for logging."""
        return {
            "environment": self.environment,
            "ce_job": os.getenv("CE_JOB", "unknown"),
            "ce_jobrun": os.getenv("CE_JOBRUN", "unknown"),
            "ce_project_id": os.getenv("CE_PROJECT_ID", "unknown"),
            "hostname": os.getenv("HOSTNAME", "unknown"),
            "cloud_region": os.getenv("CLOUD_REGION", "eu-de"),
        }

    def _process_excel_file(self, file_path: str) -> bool:
        """
        Process a single Excel file using excel_service internal methods.
        This processes ONLY the specified file with all the existing logic.
        """
        try:
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)

            self.logger.info(
                f"Processing Excel file: {filename} ({self._format_file_size(file_size)})"
            )

            # Get configuration for this specific file
            config_key = self.excel_service._get_config_key_from_filename(filename)
            if not config_key:
                self.logger.error(f"No configuration found for file: {filename}")
                return False

            file_config = self.config.get_file_config(config_key)
            if not file_config:
                self.logger.error(
                    f"Configuration missing for resolved key: {config_key}"
                )
                return False

            # Load and process the Excel file using excel_service internal methods
            import pandas as pd

            xl = pd.ExcelFile(file_path)

            tables_for_merge = {}
            file_level_key_values = {}

            total_tables = 0
            total_rows = 0

            # Process each sheet in the file using excel_service methods
            for sheet_name, sheet_config in file_config.items():
                try:
                    sheet_result = self.excel_service._process_sheet(
                        xl,
                        sheet_name,
                        sheet_config,
                        config_key,
                        tables_for_merge,
                        file_level_key_values,
                    )
                    total_tables += sheet_result.get("tables_processed", 0)

                except Exception as e:
                    self.logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
                    continue

            # Process any merge operations
            if tables_for_merge:
                merge_results = self.excel_service._process_merge_operations(
                    tables_for_merge
                )
                self.logger.info(f"Merge operations completed: {merge_results}")

            # Archive the processed file locally (TEST mode only)
            if self.environment == "test":
                self.excel_service._archive_processed_file(file_path)

            self.logger.info(
                f"Successfully processed {filename}: {total_tables} tables, {total_rows} rows"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {str(e)}")
            return False

    def process_file(self, filename: str) -> bool:
        """Process the specific file that triggered this job."""
        try:
            env_info = self._get_environment_info()
            self.logger.info(f"Environment info: {env_info}")
            self.logger.info(f"Processing triggered file: {filename}")

            if self.environment == "test":
                # TEST MODE: Process specific local file
                abs_paths = self.config.processing.get_absolute_paths()
                input_dir = abs_paths["input_dir"]
                file_path = os.path.join(input_dir, filename)

                if not os.path.exists(file_path):
                    self.logger.error(f"File not found in local directory: {filename}")
                    return False

                if not self._is_excel_file(filename):
                    self.logger.info(f"Ignoring non-Excel file: {filename}")
                    return True

                self.logger.info(f"Processing local file: {file_path}")
                return self._process_excel_file(file_path)

            else:
                # PROD MODE: Download and process specific COS file
                return self._process_cos_file(filename)

        except Exception as e:
            self.logger.error(f"Error processing file {filename}: {str(e)}")
            return False

    def _process_cos_file(self, filename: str) -> bool:
        """Download and process the specific file from COS."""
        try:
            if not self._is_excel_file(filename):
                self.logger.info(f"Ignoring non-Excel file: {filename}")
                return True

            # Get file metadata
            metadata = self._get_file_metadata(filename)
            if not metadata:
                self.logger.error(f"Could not retrieve metadata for {filename}")
                return False

            file_size = self._format_file_size(metadata["size"])
            self.logger.info(f"Processing COS file: {filename} ({file_size})")

            # Setup temp directory and download the specific file
            input_dir = self._setup_temp_processing_directory()
            local_path = os.path.join(input_dir, filename)

            # Download only the triggered file
            self.cos_client.download_file(
                Bucket=self.bucket_name, Key=filename, Filename=local_path
            )

            if not os.path.exists(local_path):
                self.logger.error(f"Download failed for {filename}")
                return False

            self.logger.info(f"Downloaded {filename} for processing")

            # Process the single file
            success = self._process_excel_file(local_path)

            if success:
                # Archive the file in COS
                self._archive_file_to_cos(filename)

            return success

        except Exception as e:
            self.logger.error(f"Error processing COS file {filename}: {str(e)}")
            return False
        finally:
            # Cleanup temp directory
            self._cleanup_temp_directory()

    def run(self, filename: str) -> int:
        """Main entry point for processing the triggered file."""
        try:
            self.logger.info("=== COS Excel Processor - Trigger Mode ===")
            self.logger.info(f"Processing file: {filename}")

            if self.environment == "test":
                self.logger.info("TEST MODE: Processing local file")
            else:
                self.logger.info("PROD MODE: Processing COS file")

            # Process the triggered file
            success = self.process_file(filename)

            if success:
                self.logger.info(f"Successfully processed {filename}")
                return 0
            else:
                self.logger.error(f"Failed to process {filename}")
                return 1

        except Exception as e:
            self.logger.error(f"Unexpected error processing {filename}: {str(e)}")
            return 1
        finally:
            # Upload logs (only in PROD mode)
            self._upload_captured_logs_to_cos()


def get_triggered_filename() -> str:
    """
    Extract the filename that triggered this job.

    In production: Filename comes from COS event trigger
    In testing: Filename comes from command line argument
    """
    if len(sys.argv) > 1:
        # Filename provided as command line argument
        return sys.argv[1]

    # Try to get filename from Code Engine event environment variables
    # These are typically set by the COS trigger
    event_source = os.getenv("CE_SOURCE", "")
    if "object" in event_source:
        try:
            import json

            source_data = json.loads(event_source)
            return source_data.get("object", {}).get("key", "")
        except:
            pass

    # Try alternative environment variable patterns
    triggered_file = os.getenv("TRIGGERED_FILE", "")
    if triggered_file:
        return triggered_file

    object_key = os.getenv("OBJECT_KEY", "")
    if object_key:
        return object_key

    # If no filename found, this is an error
    raise ValueError(
        "No filename found. Please provide filename as argument or ensure COS trigger is configured correctly."
    )


def main():
    """
    Main entry point for trigger-based Excel processing.

    Usage:
        python app_cloud.py "filename.xlsx"  # Local testing
        # Production: Filename automatically extracted from trigger
    """
    try:
        _start_terminal_capture()

        environment = os.getenv("ENVIRONMENT", "prod").lower()
        print(f"=== Excel File Processor ===")
        print(f"Environment: {environment.upper()}")

        # Get the filename that triggered this job
        try:
            filename = get_triggered_filename()
            print(f"Processing triggered file: {filename}")
        except ValueError as e:
            print(f"Error: {e}")
            print("Usage: python app_cloud.py 'filename.xlsx'")
            return 1

        # Initialize processor and process the file
        processor = COSExcelProcessor()
        return processor.run(filename)

    except KeyboardInterrupt:
        print("\nProcessor stopped by user")
        return 0
    except Exception as e:
        print(f"Processor failed: {str(e)}")
        return 1
    finally:
        try:
            if "processor" in locals():
                processor._upload_captured_logs_to_cos()
        except Exception:
            pass
        _stop_terminal_capture()


if __name__ == "__main__":
    exit(main())
