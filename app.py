#!/usr/bin/env python3
"""
COS Event Processor for IBM Code Engine

Listens for COS bucket events and logs new Excel file creations.
Designed to run as a Code Engine Job triggered by COS events.
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv

import ibm_boto3
from ibm_botocore.exceptions import ClientError


class COSEventProcessor:
    """Processes COS bucket events and logs Excel file creation"""

    def __init__(self):
        load_dotenv()
        self.logger = self._setup_logger()
        self._validate_environment()
        self._setup_cos_client()

    def _validate_environment(self):
        """Validate required environment variables"""
        required_vars = [
            "COS_ENDPOINT",
            "COS_ACCESS_KEY",
            "COS_SECRET_KEY",
            "BUCKET_NAME",
        ]

        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

    def _setup_cos_client(self):
        """Initialize COS client"""
        try:
            self.cos_client = ibm_boto3.client(
                "s3",
                aws_access_key_id=os.getenv("COS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("COS_SECRET_KEY"),
                endpoint_url=os.getenv("COS_ENDPOINT"),
            )
            self.bucket_name = os.getenv("BUCKET_NAME")
            self.logger.info("Successfully initialized COS client")

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

            # Console handler for debugging
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

    def process_event(self, event_data: Dict[str, Any]) -> bool:
        """Process a COS event"""
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

            # Log the Excel file creation
            file_size = self._format_file_size(metadata["size"])
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            log_message = (
                f"NEW EXCEL FILE DETECTED: {object_key} "
                f"(Size: {file_size}, Type: {metadata['content_type']}, "
                f"Modified: {metadata['last_modified']})"
            )

            self.logger.info(log_message)

            # Additional processing can be added here
            # This is where you would integrate your existing Excel processing logic

            return True

        except Exception as e:
            self.logger.error(f"Error processing event: {str(e)}")
            return False

    def run_from_event(self, event_json: str) -> int:
        """Main entry point for Code Engine Job from event JSON"""
        try:
            self.logger.info("=== COS Event Processor Started ===")

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
        """Test method for local development"""
        self.logger.info("=== Running Local Test ===")

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
        processor = COSEventProcessor()

        # Check if running from Code Engine event or local test
        event_json = os.getenv("CE_EVENT_DATA")

        if event_json:
            # Running from Code Engine event
            return processor.run_from_event(event_json)
        else:
            # Local testing
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
