#!/usr/bin/env python3
"""
COS Event Processor for IBM Code Engine - Cloud Version

Listens for COS bucket events and logs new Excel file creations.
Uses IAM authentication for secure cloud-to-cloud communication.
Designed to run as a Code Engine Job triggered by COS events.
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
from dotenv import load_dotenv


class COSEventProcessorCloud:
    """Processes COS bucket events using IAM authentication for cloud deployment"""

    def __init__(self):
        # Load environment variables from .env.cloud for local testing
        # In Code Engine, environment variables are set directly
        if not os.getenv("CE_JOB"):  # If not running in Code Engine
            load_dotenv(".env.cloud")

        self.logger = self._setup_logger()
        self._validate_environment()
        self._setup_cos_client()

    def _validate_environment(self):
        """Validate required environment variables for cloud deployment"""
        required_vars = ["IAM_API_KEY", "COS_INSTANCE_ID", "BUCKET_NAME"]

        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

    def _setup_cos_client(self):
        """Initialize COS client with IAM authentication"""
        try:
            # Use internal IBM Cloud endpoint for better performance
            internal_endpoint = os.getenv(
                "COS_INTERNAL_ENDPOINT",
                "https://s3.private.eu-de.cloud-object-storage.appdomain.cloud",
            )

            self.cos_client = ibm_boto3.client(
                "s3",
                ibm_api_key_id=os.getenv("IAM_API_KEY"),
                ibm_service_instance_id=os.getenv("COS_INSTANCE_ID"),
                config=Config(signature_version="oauth"),
                endpoint_url=internal_endpoint,
            )

            self.bucket_name = os.getenv("BUCKET_NAME")

            self.logger.info(
                f"Successfully initialized COS client with IAM authentication "
                f"(Endpoint: {internal_endpoint})"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize COS client: {str(e)}")
            raise

    def _setup_logger(self) -> logging.Logger:
        """Setup logging with daily log files"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Create daily log directory
            today = datetime.now().strftime("%d%m%Y")
            log_dir = Path("logs") / today
            log_dir.mkdir(parents=True, exist_ok=True)

            # Create log file for file events
            log_file = log_dir / "file_events.log"

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
        """Get file metadata from COS"""
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

    def _download_file_sample(
        self, object_key: str, sample_size: int = 1024
    ) -> Optional[bytes]:
        """Download a small sample of the file for validation"""
        try:
            response = self.cos_client.get_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Range=f"bytes=0-{sample_size-1}",
            )

            return response["Body"].read()

        except Exception as e:
            self.logger.error(f"Failed to download sample of {object_key}: {str(e)}")
            return None

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    def _get_environment_info(self) -> Dict[str, str]:
        """Get Code Engine environment information"""
        return {
            "ce_job": os.getenv("CE_JOB", "unknown"),
            "ce_jobrun": os.getenv("CE_JOBRUN", "unknown"),
            "ce_project_id": os.getenv("CE_PROJECT_ID", "unknown"),
            "hostname": os.getenv("HOSTNAME", "unknown"),
            "cloud_region": os.getenv("CLOUD_REGION", "eu-de"),
        }

    def process_event(self, event_data: Dict[str, Any]) -> bool:
        """Process a COS event"""
        try:
            # Log environment info for debugging
            env_info = self._get_environment_info()
            self.logger.info(f"Running in Code Engine Job: {env_info['ce_jobrun']}")

            # Extract event information
            bucket_name = event_data.get("bucket")
            object_key = event_data.get("object")
            event_type = event_data.get("event_type", "unknown")

            self.logger.info(
                f"Processing cloud event: {event_type} for {object_key} "
                f"in bucket {bucket_name}"
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

            # Download file sample for additional validation
            file_sample = self._download_file_sample(object_key)
            sample_info = "Available" if file_sample else "Failed"

            # Log the Excel file creation with detailed info
            file_size = self._format_file_size(metadata["size"])
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            log_message = (
                f"NEW EXCEL FILE DETECTED: {object_key} "
                f"(Size: {file_size}, Type: {metadata['content_type']}, "
                f"Sample: {sample_info}, Modified: {metadata['last_modified']}, "
                f"JobRun: {env_info['ce_jobrun']})"
            )

            self.logger.info(log_message)

            # Log original filename if available in metadata
            if metadata.get("metadata"):
                original_filename = metadata["metadata"].get("original-filename-base64")
                if original_filename:
                    try:
                        import base64

                        decoded_name = base64.b64decode(original_filename).decode(
                            "utf-8"
                        )
                        self.logger.info(f"Original filename: {decoded_name}")
                    except Exception:
                        pass

            # Here you would add your Excel processing logic
            # Example: self._process_excel_file(object_key, metadata)

            return True

        except Exception as e:
            self.logger.error(f"Error processing event: {str(e)}")
            return False

    def run_from_event(self, event_json: str) -> int:
        """Main entry point for Code Engine Job from event JSON"""
        try:
            self.logger.info("=== COS Event Processor (Cloud Version) Started ===")

            # Parse the event JSON
            try:
                event_data = json.loads(event_json)
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid event JSON: {str(e)}")
                return 1

            # Process the event
            success = self.process_event(event_data)

            if success:
                self.logger.info("=== Event processed successfully ===")
                return 0
            else:
                self.logger.error("=== Event processing failed ===")
                return 1

        except Exception as e:
            self.logger.error(f"Unexpected error in run_from_event: {str(e)}")
            return 1

    def run_local_test(self, test_filename: str = "test_file.xlsx") -> int:
        """Test method for cloud environment testing"""
        self.logger.info("=== Running Cloud Test ===")

        # Simulate a COS event
        test_event = {
            "bucket": self.bucket_name,
            "object": test_filename,
            "event_type": "object:write",
        }

        return self.run_from_event(json.dumps(test_event))


def main():
    """Main entry point"""
    try:
        processor = COSEventProcessorCloud()

        # Check if running from Code Engine event or local test
        event_json = os.getenv("CE_EVENT_DATA")

        if event_json:
            # Running from Code Engine event
            return processor.run_from_event(event_json)
        else:
            # Cloud environment testing
            test_file = os.getenv("TEST_FILENAME", "sample_excel_file.xlsx")
            return processor.run_local_test(test_file)

    except KeyboardInterrupt:
        print("\nProcessor stopped by user")
        return 0
    except Exception as e:
        print(f"Processor failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
