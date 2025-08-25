#!/usr/bin/env python3
"""
Complete COS Excel Processor for IBM Code Engine

Excel data processing pipeline with dual-mode operation:
- Single-file mode: Process specific file from trigger event
- Batch mode: Process all files in source location

Environment-based file source selection:
- ENVIRONMENT=test: Process files from data/input/ directory (local development)
- ENVIRONMENT=prod: Process files from COS bucket (production deployment)

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


class COSExcelProcessorComplete:
    """
    Complete Excel processor with single-file and batch processing capabilities.

    Supports environment-based file source selection and trigger-based processing
    for optimal cloud deployment patterns.
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

        self.log_messages = []
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
            object_key = f"logs/{date_part}/excel_proccessor_{ts_part}.log"

            self.cos_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=log_text.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            self.logger.info(f"Uploaded run logs to '{object_key}'")
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

            # Validate configuration
            self._validate_configuration()

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

    def _validate_configuration(self):
        """Validate that required configuration is present."""
        try:
            # Check if file configurations exist
            if not hasattr(self.config, "file_configs") or not self.config.file_configs:
                self.logger.warning(
                    "No file configurations found - processing may fail"
                )

            # Check processing configuration
            if not hasattr(self.config, "processing"):
                raise ValueError("Missing processing configuration")

            # Log configuration summary
            self.logger.info(f"Configuration validation passed")

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {str(e)}")
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
            self.temp_dir = tempfile.mkdtemp(prefix="cos_excel_processor_")

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

    def _get_local_excel_files(self) -> List[str]:
        """Get Excel files from local data/input/ directory for TEST mode."""
        try:
            abs_paths = self.config.processing.get_absolute_paths()
            input_dir = abs_paths["input_dir"]

            self.logger.info(
                f"Scanning local directory '{input_dir}' for Excel files..."
            )

            if not os.path.exists(input_dir):
                os.makedirs(input_dir)
                self.logger.info(f"Created input directory: {input_dir}")
                return []

            excel_files = []
            for filename in os.listdir(input_dir):
                if self._is_excel_file(filename):
                    file_path = os.path.join(input_dir, filename)
                    excel_files.append(file_path)

            self.logger.info(f"Found {len(excel_files)} Excel files in local directory")
            for file_path in excel_files:
                file_size = os.path.getsize(file_path)
                self.logger.info(
                    f"  - {os.path.basename(file_path)} ({self._format_file_size(file_size)})"
                )

            return excel_files

        except Exception as e:
            self.logger.error(f"Error scanning local directory: {str(e)}")
            return []

    def _download_excel_files_from_cos(self, input_dir: str) -> List[str]:
        """Download all Excel files from COS bucket to local input directory."""
        try:
            self.logger.info(
                f"Scanning COS bucket '{self.bucket_name}' for Excel files..."
            )

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
                    metadata = self._get_file_metadata(object_key)
                    if metadata:
                        file_size = self._format_file_size(metadata["size"])
                        self.logger.info(f"Processing: {object_key} ({file_size})")

                    local_filename = os.path.basename(object_key)
                    local_path = os.path.join(input_dir, local_filename)

                    self.cos_client.download_file(
                        Bucket=self.bucket_name, Key=object_key, Filename=local_path
                    )

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

    def _archive_processed_files_to_cos(
        self, processed_files: List[str], success: bool = True
    ):
        """
        Archive processed files back to COS under archive/ directory.

        Args:
            processed_files: List of file paths that were processed
            success: True if processing was successful, False if failed
        """
        if not processed_files or self.environment != "prod" or not self.cos_client:
            if self.environment != "prod":
                self.logger.info("Skipping COS archival (not in PROD mode)")
            return

        try:
            archive_date = datetime.now().strftime("%Y%m%d")
            archive_folder = "success" if success else "failed"

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
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    name_without_ext, ext = os.path.splitext(original_filename)
                    archived_filename = f"{name_without_ext}_{timestamp}{ext}"
                    archive_key = (
                        f"archive/{archive_date}/{archive_folder}/{archived_filename}"
                    )

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

                        if success:
                            self.logger.info(
                                f"Successfully archived '{original_key}' to '{archive_key}'"
                            )
                        else:
                            self.logger.warning(
                                f"Archived failed file '{original_key}' to '{archive_key}'"
                            )
                    else:
                        self.logger.warning(
                            f"Could not find original object to archive for '{original_filename}'"
                        )

                except Exception as e:
                    self.logger.error(f"Error archiving {file_path}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error in archive process: {str(e)}")

    def _archive_single_file_to_cos(self, cos_key: str, success: bool = True):
        """
        Archive a single processed file to COS.

        Args:
            cos_key: The COS key of the file to archive
            success: True if processing was successful, False if failed
        """
        if self.environment != "prod" or not self.cos_client:
            return

        try:
            archive_date = datetime.now().strftime("%Y%m%d")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Extract filename from the full key for archival naming
            filename = os.path.basename(cos_key)
            name_without_ext, ext = os.path.splitext(filename)
            archived_filename = f"{name_without_ext}_{timestamp}{ext}"

            # Choose archive folder based on success/failure
            if success:
                archive_key = f"archive/{archive_date}/success/{archived_filename}"
                self.logger.info(f"Archiving successful processing: {cos_key}")
            else:
                archive_key = f"archive/{archive_date}/failed/{archived_filename}"
                self.logger.warning(f"Archiving failed processing: {cos_key}")

            # Server-side copy then delete original
            self.cos_client.copy_object(
                Bucket=self.bucket_name,
                Key=archive_key,
                CopySource={"Bucket": self.bucket_name, "Key": cos_key},
            )

            self.cos_client.delete_object(Bucket=self.bucket_name, Key=cos_key)

            if success:
                self.logger.info(
                    f"Successfully archived '{cos_key}' to '{archive_key}'"
                )
            else:
                self.logger.warning(
                    f"Archived failed file '{cos_key}' to '{archive_key}'"
                )

        except Exception as e:
            self.logger.error(f"Error archiving {cos_key}: {str(e)}")

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

    def _extract_filename_from_trigger(self) -> Optional[str]:
        """
        Extract filename from Code Engine trigger event data.

        Returns:
            str: The filename that triggered the event, or None if not found
        """
        try:
            # Method 1: Check for trigger event data in environment variables
            # IBM Cloud Code Engine might pass trigger data as environment variables
            trigger_env_vars = [
                "CE_TRIGGER_PAYLOAD",
                "COS_EVENT_DATA",
                "TRIGGER_PAYLOAD",
                "CE_EVENT_DATA",
            ]

            for var in trigger_env_vars:
                value = os.getenv(var)
                if value:
                    self.logger.info(
                        f"Found trigger data in environment variable {var}: {value[:200]}..."
                    )
                    try:
                        event_json = json.loads(value)
                        if "Records" in event_json:
                            for record in event_json["Records"]:
                                if "s3" in record and "object" in record["s3"]:
                                    key = record["s3"]["object"].get("key", "")
                                    if key and not key.startswith("archive/"):
                                        filename = os.path.basename(key)
                                        self.logger.info(
                                            f"Extracted filename from {var}: {filename} (full path: {key})"
                                        )
                                        return key
                    except json.JSONDecodeError:
                        self.logger.warning(f"Invalid JSON in {var}")
                        continue

            # Method 2: Read trigger event data from stdin
            if not sys.stdin.isatty():
                event_data = sys.stdin.read().strip()
                if event_data:
                    self.logger.info(
                        f"Received trigger event data from stdin: {event_data[:200]}..."
                    )

                    # Parse JSON event data
                    event_json = json.loads(event_data)
                    if "Records" in event_json:
                        for record in event_json["Records"]:
                            if "s3" in record and "object" in record["s3"]:
                                key = record["s3"]["object"].get("key", "")
                                if key and not key.startswith("archive/"):
                                    filename = os.path.basename(key)
                                    self.logger.info(
                                        f"Extracted filename from stdin trigger: {filename} (full path: {key})"
                                    )
                                    return key
                    else:
                        self.logger.warning(
                            "No 'Records' found in stdin trigger event data"
                        )
                else:
                    self.logger.warning("stdin is available but empty")
            else:
                self.logger.warning("stdin is not available (not running from trigger)")

            # Method 3: Check for direct filename in environment variables
            # Some triggers might pass the filename directly
            filename_vars = [
                "TRIGGER_FILENAME",
                "COS_FILENAME",
                "CE_FILENAME",
                "OBJECT_KEY",
            ]

            for var in filename_vars:
                value = os.getenv(var)
                if value:
                    if not value.startswith("archive/") and self._is_excel_file(value):
                        self.logger.info(f"Found filename in {var}: {value}")
                        return value

            # Method 4: Check IBM Cloud Code Engine specific variables
            # CE_SUBJECT contains the filename directly
            ce_subject = os.getenv("CE_SUBJECT")
            if ce_subject:
                self.logger.info(f"Found filename in CE_SUBJECT: {ce_subject}")
                if not ce_subject.startswith("archive/") and self._is_excel_file(
                    ce_subject
                ):
                    return ce_subject

            # Method 5: Parse CE_DATA (base64 encoded JSON)
            ce_data = os.getenv("CE_DATA")
            if ce_data:
                self.logger.info(f"Found CE_DATA: {ce_data[:100]}...")
                try:
                    import base64

                    decoded_data = base64.b64decode(ce_data).decode("utf-8")
                    self.logger.info(f"Decoded CE_DATA: {decoded_data[:200]}...")

                    event_json = json.loads(decoded_data)
                    if "key" in event_json:
                        key = event_json["key"]
                        if not key.startswith("archive/") and self._is_excel_file(key):
                            self.logger.info(f"Extracted filename from CE_DATA: {key}")
                            return key
                except Exception as e:
                    self.logger.warning(f"Error parsing CE_DATA: {str(e)}")

            # Method 4: Debug - log all environment variables to understand what's available
            self.logger.info("Debug: All environment variables for trigger detection:")
            trigger_related_vars = [
                var
                for var in os.environ.keys()
                if any(
                    keyword in var.upper()
                    for keyword in ["TRIGGER", "COS", "S3", "EVENT", "OBJECT", "CE_"]
                )
            ]

            for var in trigger_related_vars:
                value = os.environ[var]
                # Mask sensitive values
                if "KEY" in var.upper() or "SECRET" in var.upper():
                    masked_value = (
                        f"{value[:8]}...{value[-4:]}" if len(value) > 12 else "***"
                    )
                    self.logger.info(f"  {var}: {masked_value}")
                else:
                    self.logger.info(f"  {var}: {value}")

            # CRITICAL: Don't use fallback for simultaneous uploads
            # This could cause race conditions where multiple jobs process the same file
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

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in trigger event data: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting filename from trigger: {str(e)}")
            return None

    def process_single_file_from_trigger(self, filename: str) -> bool:
        """Process only the specific file that triggered the event."""
        try:
            self.logger.info(f"Processing triggered file: {filename}")

            if self.environment == "test":
                # TEST MODE: Process specific local file
                abs_paths = self.config.processing.get_absolute_paths()
                input_dir = abs_paths["input_dir"]
                file_path = os.path.join(input_dir, filename)

                # Debug: List available files in input directory
                if not os.path.exists(file_path):
                    self.logger.error(f"File not found in local directory: {filename}")
                    self.logger.info(f"Looking for file at: {file_path}")

                    # List all files in the input directory to help debug
                    if os.path.exists(input_dir):
                        available_files = os.listdir(input_dir)
                        self.logger.info(f"Available files in {input_dir}:")
                        for file in available_files:
                            self.logger.info(f"  - {file}")
                    else:
                        self.logger.error(
                            f"Input directory does not exist: {input_dir}"
                        )

                    return False

                if not self._is_excel_file(filename):
                    self.logger.info(f"Ignoring non-Excel file: {filename}")
                    return True

                self.logger.info(f"Processing local file: {file_path}")
                return self._process_specific_local_file(file_path)

            else:
                # PROD MODE: Download and process specific COS file
                return self._process_specific_cos_file(filename)

        except Exception as e:
            self.logger.error(f"Error processing single file {filename}: {str(e)}")
            return False

    def _process_specific_local_file(self, file_path: str) -> bool:
        """Process a specific local file using Excel service logic."""
        try:
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)

            self.logger.info(
                f"Processing local file: {filename} ({self._format_file_size(file_size)})"
            )

            # Get configuration for this file
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

            # Load and process the Excel file
            import pandas as pd

            xl = pd.ExcelFile(file_path)

            tables_for_merge = {}
            file_level_key_values = {}

            total_tables = 0

            # Process each sheet in the file
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

            # Archive the processed file locally
            self.excel_service._archive_processed_file(file_path)

            self.logger.info(
                f"Successfully processed {filename}: {total_tables} tables"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error processing local file {file_path}: {str(e)}")
            return False

    def _process_specific_cos_file(self, cos_key: str) -> bool:
        """Download and process a specific file from COS."""
        try:
            if not self._is_excel_file(cos_key):
                self.logger.info(f"Ignoring non-Excel file: {cos_key}")
                return True

            # Get file metadata
            metadata = self._get_file_metadata(cos_key)
            if not metadata:
                self.logger.error(f"Could not retrieve metadata for {cos_key}")
                return False

            file_size = self._format_file_size(metadata["size"])
            self.logger.info(f"Processing COS file: {cos_key} ({file_size})")

            # Setup temp directory and download specific file
            input_dir = self._setup_temp_processing_directory()
            # Use just the filename for local path, but full key for download
            local_filename = os.path.basename(cos_key)
            local_path = os.path.join(input_dir, local_filename)

            # Download the specific file using the full COS key
            self.cos_client.download_file(
                Bucket=self.bucket_name, Key=cos_key, Filename=local_path
            )

            if not os.path.exists(local_path):
                self.logger.error(f"Download failed for {cos_key}")
                return False

            self.logger.info(f"Downloaded {cos_key} to {local_path}")

            # Process the single file using the same logic as local processing
            success = self._process_specific_local_file(local_path)

            # Archive the file in COS (server-side copy) - always archive, but to different folders
            self._archive_single_file_to_cos(cos_key, success=success)

            return success

        except Exception as e:
            self.logger.error(f"Error processing COS file {cos_key}: {str(e)}")
            return False
        finally:
            # Cleanup temp directory
            self._cleanup_temp_directory()

    def process_all_excel_files(self) -> bool:
        """Process all Excel files based on environment mode (batch processing)."""
        try:
            env_info = self._get_environment_info()
            self.logger.info(f"Running in environment: {env_info}")

            if self.environment == "test":
                # TEST MODE: Process local files
                self.logger.info("TEST MODE: Processing all local files")

                excel_files = self._get_local_excel_files()

                if not excel_files:
                    self.logger.info(
                        "No Excel files found to process in local directory"
                    )
                    self.logger.info(
                        "Place Excel files in data/input/ directory for processing"
                    )
                    return True

                # Process Excel files using existing service
                self.logger.info("Starting Excel file processing...")
                results = self.excel_service.process_all_files()

            else:
                # PROD MODE: Download from COS and process
                self.logger.info("PROD MODE: Downloading and processing all COS files")

                input_dir = self._setup_temp_processing_directory()
                excel_files = self._download_excel_files_from_cos(input_dir)

                if not excel_files:
                    self.logger.info("No Excel files found to process in COS bucket")
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

                # Archive processed files to success folder (only in PROD mode with COS)
                if self.environment == "prod":
                    self._archive_processed_files_to_cos(excel_files, success=True)
                else:
                    self.logger.info("TEST mode: Skipping COS archival")

                return True
            else:
                self.logger.error(
                    f"Excel processing failed: {results.get('error', 'Unknown error')}"
                )

                # Archive processed files to failed folder (only in PROD mode with COS)
                if self.environment == "prod":
                    self._archive_processed_files_to_cos(excel_files, success=False)
                else:
                    self.logger.info("TEST mode: Skipping COS archival")

                return False

        except Exception as e:
            self.logger.error(f"Error in process_all_excel_files: {str(e)}")
            return False

        finally:
            # Always cleanup temp directory (only used in PROD mode)
            if self.environment == "prod":
                self._cleanup_temp_directory()

    def run_from_trigger(self, filename: str) -> int:
        """Main entry point for trigger-based processing of specific file."""
        try:
            self.logger.info("=== COS Excel Processor - Single File Mode ===")
            self.logger.info(f"Processing triggered file: {filename}")

            if self.environment == "test":
                self.logger.info("TEST MODE: Processing specific local file")
            else:
                self.logger.info("PROD MODE: Processing specific COS file")

            # Process only the triggered file
            success = self.process_single_file_from_trigger(filename)

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

    def run_from_event(self, event_json: str) -> int:
        """Main entry point for batch processing of all files."""
        try:
            self.logger.info("=== COS Excel Processor - Batch Mode ===")

            if self.environment == "test":
                self.logger.info(
                    "TEST MODE: Processing all files from data/input/ directory"
                )
            else:
                self.logger.info("PROD MODE: Processing all files from COS bucket")

            # Process all Excel files
            success = self.process_all_excel_files()

            if success:
                self.logger.info("Processing completed successfully")
                return 0
            else:
                self.logger.error("Processing failed")
                return 1

        except Exception as e:
            self.logger.error(f"Unexpected error in run_from_event: {str(e)}")
            return 1
        finally:
            # Upload logs (only in PROD mode)
            self._upload_captured_logs_to_cos()


def main():
    """
    Main entry point supporting both batch and single-file processing modes.

    Usage:
        python app_cloud.py                    # Process all files (batch mode)
        python app_cloud.py filename.xlsx     # Process specific file (trigger mode)
    """
    try:
        _start_terminal_capture()

        environment = os.getenv("ENVIRONMENT", "prod").lower()
        processor = COSExcelProcessorComplete()

        # Check if a specific filename was provided as argument
        if len(sys.argv) > 1:
            # Join all arguments to handle filenames with spaces
            filename = " ".join(sys.argv[1:])
            print("=== Single File Processing Mode ===")
            print(f"Environment: {environment.upper()}")
            print(f"Processing file: {filename}")

            return processor.run_from_trigger(filename)

        # Check if running from Code Engine trigger (COS event)
        elif os.getenv("CE_JOB") and environment == "prod":
            # Try to get filename from trigger event data
            filename = processor._extract_filename_from_trigger()
            if filename:
                print("=== Trigger-Based Single File Processing Mode ===")
                print(f"Environment: {environment.upper()}")
                print(f"Processing triggered file: {filename}")

                return processor.run_from_trigger(filename)
            else:
                print(
                    "=== Trigger Detected but No Filename Found - Fallback to Batch Mode ==="
                )
                print(f"Environment: {environment.upper()}")
                print("Processing all files from COS bucket")

                return processor.run_from_event("")
        else:
            # Batch processing mode
            print("=== Batch Processing Mode ===")
            print(f"Environment: {environment.upper()}")

            if environment == "test":
                print("Processing all files from local data/input/ directory")
            else:
                print("Processing all files from COS bucket")

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
