#!/usr/bin/env python3
"""
Complete COS Excel Processor for IBM Code Engine

Combines the working COS connection logic with full Excel processing capabilities.
Downloads Excel files from COS bucket, processes them with existing logic, and exports to database.
Uses IAM authentication for secure cloud-to-cloud communication.
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
from typing import Dict, Any, Optional, List

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

# --- Terminal capture utilities ---
_TERMINAL_CAPTURE_BUFFER: Optional[io.StringIO] = None
_ORIG_STDOUT = None
_ORIG_STDERR = None


class _TeeStream:
    """A simple tee stream that writes to both original stream and a buffer."""

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
    global _TERMINAL_CAPTURE_BUFFER, _ORIG_STDOUT, _ORIG_STDERR
    if _TERMINAL_CAPTURE_BUFFER is not None:
        return
    _TERMINAL_CAPTURE_BUFFER = io.StringIO()
    _ORIG_STDOUT = sys.stdout
    _ORIG_STDERR = sys.stderr
    sys.stdout = _TeeStream(_ORIG_STDOUT, _TERMINAL_CAPTURE_BUFFER)
    sys.stderr = _TeeStream(_ORIG_STDERR, _TERMINAL_CAPTURE_BUFFER)


def _stop_terminal_capture():
    global _TERMINAL_CAPTURE_BUFFER, _ORIG_STDOUT, _ORIG_STDERR
    if _TERMINAL_CAPTURE_BUFFER is None:
        return
    try:
        sys.stdout = _ORIG_STDOUT
        sys.stderr = _ORIG_STDERR
    finally:
        _ORIG_STDOUT = None
        _ORIG_STDERR = None
        # Keep buffer for upload; do not close here


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


class COSExcelProcessorComplete:
    """
    Complete Excel processor that combines COS operations with Excel processing logic.

    This class integrates the working COS connection from app_cloud.py with the full
    Excel processing capabilities from the existing codebase.
    """

    def __init__(self):
        # Mark run start time for consistent log file naming
        self.run_start_time = datetime.now()
        # Load environment variables from .env.cloud for local testing
        # In Code Engine, environment variables are set directly
        if not os.getenv("CE_JOB"):  # If not running in Code Engine
            env_files = [".env.cloud", ".env", ".env.local"]
            for env_file in env_files:
                if os.path.exists(env_file):
                    load_dotenv(env_file)
                    print(f"Loaded environment from: {env_file}")
                    break

        # Initialize log capture before setting up logger
        self.log_messages = []

        self.logger = self._setup_logger()
        self._validate_environment()
        self._setup_cos_client()
        self._setup_excel_services()

        # Temporary directory for file processing
        self.temp_dir = None

    def _upload_captured_logs_to_cos(self):
        """Upload everything printed to terminal during this run to COS logs prefix."""
        try:
            # Access global terminal capture buffer
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
            # Intentional name as requested: excel_proccessor_...
            object_key = f"logs/{date_part}/excel_proccessor_{ts_part}.log"

            self.cos_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=log_text.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            self.logger.info(f"Uploaded run logs to '{object_key}'")
        except Exception as e:
            # Avoid raising; log and continue
            try:
                self.logger.error(f"Failed to upload logs to COS: {str(e)}")
            except Exception:
                pass

    def _log_and_capture(self, level: str, message: str):
        """Log message and capture it for later upload"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] [{level}] {message}"

        # Add to capture list
        self.log_messages.append(formatted_message)

        # Also log normally
        if level == "INFO":
            self.logger.info(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "DEBUG":
            print(f"DEBUG: {message}")

        # Also print to console for immediate feedback
        print(formatted_message)

    def _validate_environment(self):
        """Validate required environment variables for cloud deployment"""
        required_vars = ["IAM_API_KEY", "COS_INSTANCE_ID", "BUCKET_NAME"]

        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
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
        """Initialize COS client with IAM authentication (same as working app_cloud.py)"""
        try:
            # Use appropriate endpoint based on environment
            if os.getenv("CE_JOB"):
                # Running in Code Engine - use private endpoint
                endpoint = os.getenv(
                    "COS_INTERNAL_ENDPOINT",
                    "https://s3.private.eu-de.cloud-object-storage.appdomain.cloud",
                )
                self.logger.info("Using Code Engine private endpoint")
            else:
                # Running locally - use public endpoint
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
                f"Successfully initialized COS client with IAM authentication "
                f"(Endpoint: {endpoint})"
            )

            # Test connection with timeout
            try:
                self.logger.info("Testing COS connection with 10 second timeout...")
                import signal

                def timeout_handler(signum, frame):
                    raise TimeoutError("COS connection test timed out")

                # Set timeout for the test
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(10)  # 10 second timeout

                try:
                    response = self.cos_client.list_buckets()
                    bucket_count = len(response.get("Buckets", []))
                    self.logger.info(
                        f"COS connection test successful - found {bucket_count} buckets"
                    )

                    # Verify target bucket exists
                    bucket_names = [b["Name"] for b in response.get("Buckets", [])]
                    if self.bucket_name in bucket_names:
                        self.logger.info(
                            f"Target bucket '{self.bucket_name}' confirmed"
                        )
                    else:
                        self.logger.warning(
                            f"Target bucket '{self.bucket_name}' not found"
                        )
                        self.logger.info(f"Available buckets: {bucket_names}")

                finally:
                    signal.alarm(0)  # Cancel the alarm

            except (TimeoutError, Exception) as test_error:
                self.logger.warning(f"COS connection test failed: {str(test_error)}")
                self.logger.info(
                    "Continuing without connection test - will test during actual operations"
                )
                # Don't raise - let it continue and fail later if there's a real problem

        except Exception as e:
            self.logger.error(f"Failed to initialize COS client: {str(e)}")
            raise

    def _setup_excel_services(self):
        """Initialize Excel processing and database services"""
        try:
            # Get configuration using existing config manager
            self.config = get_config()
            self.logger.info("Configuration loaded successfully")

            # Fix SSL certificate path for database
            if hasattr(self.config.database, "sslrootcert"):
                # Check if it's a relative path
                ssl_cert_path = self.config.database.sslrootcert
                if not os.path.isabs(ssl_cert_path):
                    # Make it relative to the current directory + config folder
                    ssl_cert_path = os.path.join(
                        current_dir, "config", "ibm-cloud-cert.crt"
                    )
                    self.config.database.sslrootcert = ssl_cert_path
                    self.logger.info(
                        f"Updated SSL certificate path to: {ssl_cert_path}"
                    )

                # Verify the certificate exists
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
        """Setup logging with daily log files (from working app_cloud.py)"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Create daily log directory
            today = datetime.now().strftime("%d%m%Y")
            log_dir = Path("logs") / today
            log_dir.mkdir(parents=True, exist_ok=True)

            # Create log file for file events
            log_file = log_dir / "complete_excel_processor.log"

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
        """Check if file is an Excel file"""
        excel_extensions = [".xlsx", ".xls", ".xlsm", ".xlsb"]
        return any(filename.lower().endswith(ext) for ext in excel_extensions)

    def _get_file_metadata(self, object_key: str) -> Optional[Dict[str, Any]]:
        """Get file metadata from COS (from working app_cloud.py)"""
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
        """Format file size in human readable format"""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    def _setup_temp_processing_directory(self):
        """Setup temporary directory structure for Excel processing"""
        try:
            self.temp_dir = tempfile.mkdtemp(prefix="cos_excel_processor_")

            # Create directory structure expected by Excel service
            directories = {
                "input_dir": os.path.join(self.temp_dir, "input"),
                "output_dir": os.path.join(self.temp_dir, "output"),
                "archive_dir": os.path.join(self.temp_dir, "archive"),
                "logs_dir": os.path.join(self.temp_dir, "logs"),
            }

            for dir_name, dir_path in directories.items():
                os.makedirs(dir_path, exist_ok=True)

            # Update config to use temporary directories
            abs_paths = self.config.processing.get_absolute_paths()
            self.config.processing.input_dir = "input"  # Relative to temp_dir
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
        """Clean up temporary directory"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            except Exception as e:
                self.logger.warning(f"Error cleaning up temp directory: {str(e)}")

    def _download_excel_files_from_cos(self, input_dir: str) -> List[str]:
        """Download all Excel files from COS bucket to local input directory"""
        try:
            self.logger.info(
                f"Scanning COS bucket '{self.bucket_name}' for Excel files..."
            )

            # List all objects in bucket
            response = self.cos_client.list_objects_v2(Bucket=self.bucket_name)

            if "Contents" not in response:
                self.logger.info("Bucket is empty")
                return []

            # Filter Excel files (exclude anything under archive/)
            excel_files = []
            for obj in response["Contents"]:
                key = obj["Key"]
                if key.startswith("archive/"):
                    continue
                if self._is_excel_file(key):
                    excel_files.append(key)

            self.logger.info(f"Found {len(excel_files)} Excel files in bucket")

            # Download each Excel file
            downloaded_files = []
            for object_key in excel_files:
                try:
                    # Get metadata for logging
                    metadata = self._get_file_metadata(object_key)
                    if metadata:
                        file_size = self._format_file_size(metadata["size"])
                        self.logger.info(f"Processing: {object_key} ({file_size})")

                    # Download file
                    local_filename = os.path.basename(object_key)
                    local_path = os.path.join(input_dir, local_filename)

                    self.cos_client.download_file(
                        Bucket=self.bucket_name, Key=object_key, Filename=local_path
                    )

                    # Verify download
                    if os.path.exists(local_path):
                        actual_size = os.path.getsize(local_path)
                        self.logger.info(
                            f"Downloaded {object_key} ({actual_size} bytes)"
                        )
                        downloaded_files.append(local_path)
                    else:
                        self.logger.error(f"Download failed: {local_path} not found")

                except Exception as e:
                    self.logger.error(f"Error downloading {object_key}: {str(e)}")
                    continue

            self.logger.info(
                f"Successfully downloaded {len(downloaded_files)} Excel files"
            )
            return downloaded_files

        except Exception as e:
            self.logger.error(f"Error downloading Excel files: {str(e)}")
            return []

    def _archive_processed_files_to_cos(self, processed_files: List[str]):
        """Archive processed files back to COS under `archive/` using server-side copy.

        Objects are archived to: archive/YYYYMMDD/<original_name>_YYYYMMDD_HHMMSS<ext>
        This avoids name collisions and does not depend on the local temp files
        still existing after processing.
        """
        if not processed_files:
            return

        try:
            archive_date = datetime.now().strftime("%Y%m%d")

            def find_original_key_by_basename(target_basename: str) -> Optional[str]:
                continuation_token = None
                while True:
                    if continuation_token:
                        response = self.cos_client.list_objects_v2(
                            Bucket=self.bucket_name,
                            ContinuationToken=continuation_token,
                        )
                    else:
                        response = self.cos_client.list_objects_v2(
                            Bucket=self.bucket_name
                        )

                    if "Contents" in response:
                        for obj in response["Contents"]:
                            key_candidate = obj["Key"]
                            if key_candidate.startswith("archive/"):
                                continue
                            if os.path.basename(key_candidate) == target_basename:
                                return key_candidate

                    if response.get("IsTruncated"):
                        continuation_token = response.get("NextContinuationToken")
                    else:
                        break
                return None

            for file_path in processed_files:
                try:
                    original_filename = os.path.basename(file_path)

                    # Create archive key with timestamp to guarantee uniqueness (per file)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    name_without_ext, ext = os.path.splitext(original_filename)
                    archived_filename = f"{name_without_ext}_{timestamp}{ext}"
                    archive_key = f"archive/{archive_date}/{archived_filename}"

                    # Find original object to copy (server-side copy preferred)
                    original_key = find_original_key_by_basename(original_filename)

                    if original_key:
                        # Server-side copy then delete original
                        self.cos_client.copy_object(
                            Bucket=self.bucket_name,
                            Key=archive_key,
                            CopySource={
                                "Bucket": self.bucket_name,
                                "Key": original_key,
                            },
                        )
                        self.cos_client.delete_object(
                            Bucket=self.bucket_name, Key=original_key
                        )
                        self.logger.info(
                            f"Archived '{original_key}' to '{archive_key}'"
                        )
                    else:
                        # No original object found; local file may have been moved by processing
                        self.logger.warning(
                            f"Could not find original object to archive for '{original_filename}'"
                        )

                except Exception as e:
                    self.logger.error(f"Error archiving {file_path}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error in archive process: {str(e)}")

    def _get_environment_info(self) -> Dict[str, str]:
        """Get Code Engine environment information"""
        return {
            "ce_job": os.getenv("CE_JOB", "unknown"),
            "ce_jobrun": os.getenv("CE_JOBRUN", "unknown"),
            "ce_project_id": os.getenv("CE_PROJECT_ID", "unknown"),
            "hostname": os.getenv("HOSTNAME", "unknown"),
            "cloud_region": os.getenv("CLOUD_REGION", "eu-de"),
        }

    def process_all_excel_files(self) -> bool:
        """Main processing method - downloads and processes all Excel files"""
        try:
            # Log environment info
            env_info = self._get_environment_info()
            self.logger.info(f"Running in environment: {env_info}")

            # Setup temporary processing directory
            input_dir = self._setup_temp_processing_directory()

            # Download Excel files from COS
            downloaded_files = self._download_excel_files_from_cos(input_dir)

            if not downloaded_files:
                self.logger.info("No Excel files found to process")
                return True

            # Process Excel files using existing service
            self.logger.info("Starting Excel file processing...")
            results = self.excel_service.process_all_files()

            if results.get("success", False):
                # Log processing results
                stats = results.get("stats", {})
                self.logger.info(
                    f"Excel processing completed successfully: "
                    f"{stats.get('files_processed', 0)} files, "
                    f"{stats.get('tables_extracted', 0)} tables, "
                    f"{stats.get('rows_processed', 0)} rows"
                )

                # Archive processed files in COS
                self._archive_processed_files_to_cos(downloaded_files)

                return True
            else:
                self.logger.error(
                    f"Excel processing failed: {results.get('error', 'Unknown error')}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error in process_all_excel_files: {str(e)}")
            return False

        finally:
            # Always cleanup temp directory
            self._cleanup_temp_directory()

    def process_event(self, event_data: Dict[str, Any]) -> bool:
        """Process a specific COS event for a single file"""
        try:
            # Extract event information
            bucket_name = event_data.get("bucket")
            object_key = event_data.get("object")
            event_type = event_data.get("event_type", "unknown")

            self.logger.info(
                f"Processing event: {event_type} for {object_key} in bucket {bucket_name}"
            )

            # Validate bucket
            if bucket_name != self.bucket_name:
                self.logger.warning(f"Event from unexpected bucket: {bucket_name}")
                return False

            # Check if it's an Excel file
            if not self._is_excel_file(object_key):
                self.logger.info(f"Ignoring non-Excel file: {object_key}")
                return False

            # Get file metadata
            metadata = self._get_file_metadata(object_key)
            if not metadata:
                self.logger.error(f"Could not retrieve metadata for {object_key}")
                return False

            # Log the Excel file detection
            file_size = self._format_file_size(metadata["size"])
            self.logger.info(f"NEW EXCEL FILE DETECTED: {object_key} ({file_size})")

            # Setup temp directory and download specific file
            input_dir = self._setup_temp_processing_directory()

            local_filename = os.path.basename(object_key)
            local_path = os.path.join(input_dir, local_filename)

            try:
                self.cos_client.download_file(
                    Bucket=self.bucket_name, Key=object_key, Filename=local_path
                )

                if os.path.exists(local_path):
                    self.logger.info(f"Downloaded {object_key} for processing")

                    # Process the file
                    results = self.excel_service.process_all_files()

                    if results.get("success", False):
                        stats = results.get("stats", {})
                        self.logger.info(
                            f"Successfully processed {object_key}: "
                            f"{stats.get('tables_extracted', 0)} tables, "
                            f"{stats.get('rows_processed', 0)} rows"
                        )

                        # Archive the file
                        self._archive_processed_files_to_cos([local_path])
                        return True
                    else:
                        self.logger.error(f"Failed to process {object_key}")
                        return False
                else:
                    self.logger.error(f"Download failed for {object_key}")
                    return False

            except Exception as e:
                self.logger.error(
                    f"Error downloading/processing {object_key}: {str(e)}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error processing event: {str(e)}")
            return False

        finally:
            self._cleanup_temp_directory()

    def run_from_event(self, event_json: str) -> int:
        """Main entry point - ALWAYS processes all Excel files in root directory"""
        try:
            self.logger.info("=== Complete COS Excel Processor Started ===")
            self.logger.info("Processing ALL Excel files in bucket root directory")

            # ALWAYS process all Excel files - ignore event data
            success = self.process_all_excel_files()

            if success:
                self.logger.info("=== Processing completed successfully ===")
                return 0
            else:
                self.logger.error("=== Processing failed ===")
                return 1

        except Exception as e:
            self.logger.error(f"Unexpected error in run_from_event: {str(e)}")
            return 1
        finally:
            # Attempt to upload logs regardless of success
            self._upload_captured_logs_to_cos()

    def run_local_test(self, test_filename: str = None) -> int:
        """Local test method - ALWAYS processes all files"""
        self.logger.info("=== Running Local Test - Processing ALL Files ===")
        # Always process all files, ignore test_filename parameter
        return self.run_from_event("")


def main():
    """Main entry point - ALWAYS processes all Excel files"""
    try:
        _start_terminal_capture()
        print("=== Complete COS Excel Processor ===")
        print("Processing ALL Excel files in bucket root directory")
        print("Logs and archives will be saved to COS")

        processor = COSExcelProcessorComplete()

        # ALWAYS process all files - ignore any environment variables
        return processor.run_from_event("")

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
