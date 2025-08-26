"""
File processing service that orchestrates the entire processing workflow.
"""

import os
import tempfile
from typing import Optional
from models.processing_result import ProcessingResult
from utils.file_utils import (
    setup_temp_directory,
    cleanup_temp_directory,
    get_filename_from_path,
    format_file_size,
)
from utils.environment_utils import get_environment, is_production


class FileProcessingService:
    """Service that orchestrates file processing workflow."""

    def __init__(
        self, cos_service, archive_service, excel_service, database_service, logger
    ):
        self.cos_service = cos_service
        self.archive_service = archive_service
        self.excel_service = excel_service
        self.database_service = database_service
        self.logger = logger
        self.temp_dir = None
        self.run_start_time = None

    def process_single_cos_file(self, cos_key: str) -> ProcessingResult:
        """Process a single file from Cloud Object Storage."""
        from datetime import datetime

        start_time = datetime.now()

        try:
            # Validate file
            if not self._is_excel_file(cos_key):
                return ProcessingResult(
                    success=False,
                    file_name=get_filename_from_path(cos_key),
                    cos_key=cos_key,
                    error_message="Not an Excel file",
                )

            # Get file metadata
            metadata = self.cos_service.get_file_metadata(cos_key)
            if not metadata:
                return ProcessingResult(
                    success=False,
                    file_name=get_filename_from_path(cos_key),
                    cos_key=cos_key,
                    error_message="Could not retrieve file metadata",
                )

            file_size = format_file_size(metadata.size)
            self.logger.info(f"Processing COS file: {cos_key} ({file_size})")

            # Create processing status record
            filename = get_filename_from_path(cos_key)
            self._create_processing_record(filename, cos_key, metadata)

            # Setup temp directory and download file
            self.temp_dir = setup_temp_directory()
            local_filename = get_filename_from_path(cos_key)
            local_path = os.path.join(self.temp_dir, "input", local_filename)

            # Download file
            if not self.cos_service.download_file(cos_key, local_path):
                self._update_processing_status(filename, "failed", "Download failed")
                return ProcessingResult(
                    success=False,
                    file_name=filename,
                    cos_key=cos_key,
                    error_message="Download failed",
                )

            # Process the file
            success, processing_error = self._process_local_file(local_path)

            # Archive the file (if archive service is available)
            archive_path = None
            if self.archive_service:
                archive_path = self.archive_service.archive_cos_file(cos_key, success)
            else:
                self.logger.warning(
                    "Archive service not available - skipping archiving"
                )

            # Update processing status
            self._update_processing_status(
                filename,
                "success" if success else "failed",
                processing_error,
                archive_path,
            )

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()

            return ProcessingResult(
                success=success,
                file_name=filename,
                cos_key=cos_key,
                error_message=processing_error,
                archive_path=archive_path,
                processing_time=processing_time,
                metadata={"size": metadata.size},
            )

        except Exception as e:
            error_msg = f"Unexpected error processing {cos_key}: {str(e)}"
            self.logger.error(error_msg)
            return ProcessingResult(
                success=False,
                file_name=get_filename_from_path(cos_key),
                cos_key=cos_key,
                error_message=error_msg,
            )
        finally:
            self._cleanup_resources()

    def process_single_local_file(self, file_path: str) -> ProcessingResult:
        """Process a single local file."""
        from datetime import datetime

        start_time = datetime.now()

        try:
            filename = get_filename_from_path(file_path)
            self.logger.info(f"Processing local file: {filename}")

            # Process the file
            success, processing_error = self._process_local_file(file_path)

            # Archive the file (if archive service is available)
            archive_path = None
            if self.archive_service:
                archive_path = self.archive_service.archive_local_file(
                    file_path, success
                )
            else:
                self.logger.warning(
                    "Archive service not available - skipping archiving"
                )

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()

            return ProcessingResult(
                success=success,
                file_name=filename,
                error_message=processing_error,
                archive_path=archive_path,
                processing_time=processing_time,
            )

        except Exception as e:
            error_msg = f"Unexpected error processing {file_path}: {str(e)}"
            self.logger.error(error_msg)
            return ProcessingResult(
                success=False,
                file_name=get_filename_from_path(file_path),
                error_message=error_msg,
            )

    def _process_local_file(self, file_path: str) -> tuple[bool, Optional[str]]:
        """Process a local file using Excel service."""
        try:
            filename = get_filename_from_path(file_path)
            self.logger.info(f"Processing file: {filename}")

            if self.excel_service:
                # Use the actual Excel service to process the file
                self.logger.info(f"Using Excel service to process: {filename}")

                # Call the actual Excel processing method
                try:
                    self.logger.info(f"Starting Excel processing for: {filename}")

                    # Initialize tables_for_merge dictionary for the Excel service
                    tables_for_merge = {}

                    # Call the actual Excel processing method
                    file_results = self.excel_service._process_single_file(
                        file_path, tables_for_merge
                    )

                    if file_results.get("success", False):
                        tables_count = file_results.get("tables_count", 0)
                        self.logger.info(
                            f"Excel processing completed successfully for: {filename}"
                        )
                        self.logger.info(
                            f"Processed {tables_count} tables from file: {filename}"
                        )

                        # Log details about processed sheets
                        sheets = file_results.get("sheets", {})
                        for sheet_name, sheet_result in sheets.items():
                            if sheet_result.get("success", False):
                                sheet_tables = sheet_result.get("tables_processed", 0)
                                self.logger.info(
                                    f"  Sheet '{sheet_name}': {sheet_tables} tables processed"
                                )
                            else:
                                error = sheet_result.get("error", "Unknown error")
                                self.logger.warning(
                                    f"  Sheet '{sheet_name}': Failed - {error}"
                                )

                        return True, None
                    else:
                        error_msg = f"Excel processing failed for {filename}"
                        self.logger.error(error_msg)
                        return False, error_msg

                except Exception as e:
                    error_msg = f"Excel processing failed: {str(e)}"
                    self.logger.error(error_msg)
                    return False, error_msg
            else:
                # No Excel service available
                self.logger.warning(
                    "Excel service not available - processing not possible"
                )
                return True, None

        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg

    def _is_excel_file(self, filename: str) -> bool:
        """Check if file is an Excel file."""
        from utils.file_utils import is_excel_file

        return is_excel_file(filename)

    def _create_processing_record(self, filename: str, cos_key: str, metadata) -> None:
        """Create processing status record in database."""
        if not self.database_service:
            return

        try:
            from utils.environment_utils import get_environment_info

            env_info = get_environment_info()

            self.database_service.create_file_processing_record(
                file_name=filename,
                cos_key=cos_key,
                job_run_name=env_info.get("job_run_id", "unknown"),
                ce_jobrun=env_info.get("job_run_id", "unknown"),
                ce_job=env_info.get("job_name", "unknown"),
                file_size_bytes=metadata.size,
            )
        except Exception as e:
            self.logger.error(f"Error creating processing record: {str(e)}")

    def _update_processing_status(
        self,
        filename: str,
        status: str,
        error_message: Optional[str] = None,
        archive_path: Optional[str] = None,
    ) -> None:
        """Update processing status in database."""
        if not self.database_service:
            return

        try:
            # Get log file name
            log_file_name = None
            if self.run_start_time:
                timestamp = self.run_start_time.strftime("%Y%m%d_%H%M%S")
                log_file_name = f"excel_processor_{timestamp}.log"

            self.database_service.update_file_processing_status(
                file_name=filename,
                status=status,
                error_message=error_message,
                archive_path=archive_path,
                log_file_name=log_file_name,
            )
        except Exception as e:
            self.logger.error(f"Error updating processing status: {str(e)}")

    def _cleanup_resources(self) -> None:
        """Clean up temporary resources."""
        try:
            if self.temp_dir:
                cleanup_temp_directory(self.temp_dir)
                self.temp_dir = None
        except Exception as e:
            self.logger.warning(f"Error during resource cleanup: {str(e)}")
