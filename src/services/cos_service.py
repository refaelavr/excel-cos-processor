"""
Cloud Object Storage service for file operations.
"""

import os
import ibm_boto3
from typing import Optional, List, Dict, Any
from botocore.exceptions import ClientError
from botocore.config import Config
from models.processing_result import FileMetadata
from utils.environment_utils import get_cos_endpoint, is_production
from utils.file_utils import is_excel_file, format_file_size


class COSService:
    """Service for Cloud Object Storage operations."""

    def __init__(self, bucket_name: str, logger):
        self.bucket_name = bucket_name
        self.logger = logger
        self.cos_client = self._initialize_cos_client()
        self._test_connection()

    def _initialize_cos_client(self):
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
            cos_client = ibm_boto3.client(
                "s3",
                ibm_api_key_id=os.getenv("IAM_API_KEY"),
                ibm_service_instance_id=os.getenv("COS_INSTANCE_ID"),
                config=Config(
                    signature_version="oauth", connect_timeout=30, read_timeout=30
                ),
                endpoint_url=endpoint,
            )

            self.logger.info(
                f"Successfully initialized COS client with IAM authentication (Endpoint: {endpoint})"
            )

            return cos_client

        except Exception as e:
            self.logger.error(f"Failed to initialize COS client: {str(e)}")
            raise

    def _test_connection(self) -> None:
        """Test COS connection and bucket access."""
        try:
            self.logger.info("Testing COS connection with 10 second timeout...")

            # Test bucket access
            response = self.cos_client.list_buckets()
            bucket_names = [bucket["Name"] for bucket in response["Buckets"]]

            if self.bucket_name in bucket_names:
                self.logger.info(f"Target bucket '{self.bucket_name}' confirmed")
            else:
                self.logger.warning(f"Target bucket '{self.bucket_name}' not found")
                self.logger.info(f"Available buckets: {bucket_names}")

        except Exception as e:
            self.logger.warning(f"COS connection test failed: {str(e)}")
            self.logger.info("Continuing with processing...")

    def get_file_metadata(self, object_key: str) -> Optional[FileMetadata]:
        """Get file metadata from COS."""
        try:
            response = self.cos_client.head_object(
                Bucket=self.bucket_name, Key=object_key
            )

            return FileMetadata(
                size=response.get("ContentLength", 0),
                last_modified=response.get("LastModified"),
                content_type=response.get("ContentType", "unknown"),
                etag=response.get("ETag", "").strip('"'),
                metadata=response.get("Metadata", {}),
            )

        except ClientError as e:
            self.logger.error(f"Failed to get metadata for {object_key}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(
                f"Unexpected error getting metadata for {object_key}: {str(e)}"
            )
            return None

    def download_file(self, object_key: str, local_path: str) -> bool:
        """Download file from COS to local path."""
        try:
            self.cos_client.download_file(self.bucket_name, object_key, local_path)

            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                self.logger.info(
                    f"Downloaded {object_key} to {local_path} ({format_file_size(file_size)})"
                )
                return True
            else:
                self.logger.error(f"Download failed: {local_path} not found")
                return False

        except Exception as e:
            self.logger.error(f"Error downloading {object_key}: {str(e)}")
            return False

    def upload_file(self, local_path: str, object_key: str) -> bool:
        """Upload file from local path to COS."""
        try:
            self.cos_client.upload_file(local_path, self.bucket_name, object_key)
            self.logger.info(f"Uploaded {local_path} to {object_key}")
            return True
        except Exception as e:
            self.logger.error(f"Error uploading {local_path}: {str(e)}")
            return False

    def copy_file(self, source_key: str, destination_key: str) -> bool:
        """Copy file within COS bucket."""
        try:
            copy_source = {"Bucket": self.bucket_name, "Key": source_key}
            self.cos_client.copy_object(
                CopySource=copy_source, Bucket=self.bucket_name, Key=destination_key
            )
            self.logger.info(f"Copied {source_key} to {destination_key}")
            return True
        except Exception as e:
            self.logger.error(f"Error copying {source_key}: {str(e)}")
            return False

    def delete_file(self, object_key: str) -> bool:
        """Delete file from COS bucket."""
        try:
            self.cos_client.delete_object(Bucket=self.bucket_name, Key=object_key)
            self.logger.info(f"Deleted {object_key}")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting {object_key}: {str(e)}")
            return False

    def list_excel_files(self, prefix: str = "input/") -> List[str]:
        """List Excel files in bucket with given prefix."""
        try:
            response = self.cos_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            excel_files = []
            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    if is_excel_file(key):
                        excel_files.append(key)

            self.logger.info(f"Found {len(excel_files)} Excel files in bucket")
            return excel_files

        except Exception as e:
            self.logger.error(f"Error listing files: {str(e)}")
            return []

    def upload_logs(self, log_file_path: str) -> Optional[str]:
        """Upload log file to COS."""
        if not is_production():
            self.logger.info("Skipping log upload (not in PROD mode)")
            return None

        try:
            # Create log object key with timestamp
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_key = f"logs/excel_processor_{timestamp}.log"

            if self.upload_file(log_file_path, object_key):
                self.logger.info(f"Uploaded run logs to '{object_key}'")
                return object_key
            else:
                self.logger.error("Failed to upload logs to COS")
                return None

        except Exception as e:
            self.logger.error(f"Failed to upload logs to COS: {str(e)}")
            return None
