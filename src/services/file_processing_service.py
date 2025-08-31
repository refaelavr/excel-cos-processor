"""
File processing service for orchestrating file operations.
"""

import os
from datetime import datetime
from typing import Optional, Tuple
from models.processing_result import ProcessingResult, FileMetadata
from utils.file_utils import (
    get_filename_from_path,
    setup_temp_directory,
    cleanup_temp_directory,
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
        """Process a single file from COS."""
        from datetime import datetime

        start_time = datetime.now()
        self.run_start_time = start_time

        try:
            filename = get_filename_from_path(cos_key)
            self.logger.info(f"Processing COS file: {filename}")

            # Get file metadata
            metadata = self.cos_service.get_file_metadata(cos_key)
            if not metadata:
                error_msg = f"Failed to get metadata for {cos_key}"
                self.logger.error(error_msg)

                # Create failure record
                self._create_processing_record(filename, cos_key, FileMetadata(size=0))
                self._update_processing_status(filename, "failed", error_msg)

                return ProcessingResult(
                    success=False,
                    file_name=filename,
                    cos_key=cos_key,
                    error_message=error_msg,
                )

            # Create initial processing record
            self._create_processing_record(filename, cos_key, metadata)

            # Download the file
            temp_dir = setup_temp_directory()
            local_path = os.path.join(temp_dir, filename)

            if not self.cos_service.download_file(cos_key, local_path):
                error_msg = f"Failed to download {cos_key}"
                self.logger.error(error_msg)
                self._update_processing_status(filename, "failed", error_msg)
                return ProcessingResult(
                    success=False,
                    file_name=filename,
                    cos_key=cos_key,
                    error_message=error_msg,
                )

            # Process the file
            success, processing_error = self._process_local_file(local_path)

            # Archive the file
            archive_path = None
            if self.archive_service:
                archive_path = self.archive_service.archive_cos_file(cos_key, success)

            # Update processing status
            if success:
                self._update_processing_status(filename, "success", None, archive_path)
            else:
                self._update_processing_status(filename, "failed", processing_error)

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

            # Create failure record if we have filename
            try:
                filename = get_filename_from_path(cos_key)
                self._create_processing_record(filename, cos_key, FileMetadata(size=0))
                self._update_processing_status(filename, "failed", error_msg)
            except:
                pass

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
            self.logger.info(f"=== STARTING FILE PROCESSING ===")

            # Force flush logs to ensure they're written
            if hasattr(self.logger, "force_flush_all"):
                self.logger.force_flush_all()

            # Create processing record in database
            try:
                import os

                file_size = os.path.getsize(file_path)

                # Create a simple metadata object
                class FileMetadata:
                    def __init__(self, size):
                        self.size = size

                metadata = FileMetadata(file_size)
                self._create_processing_record(filename, filename, metadata)
            except Exception as e:
                self.logger.warning(f"Could not create processing record: {str(e)}")

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

            # Update processing status in database
            if success:
                self._update_processing_status(filename, "success", None, archive_path)
            else:
                self._update_processing_status(
                    filename, "failed", processing_error, archive_path
                )

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()

            # Force flush logs before finishing
            if hasattr(self.logger, "force_flush_all"):
                self.logger.force_flush_all()
                self.logger.info(f"=== FILE PROCESSING COMPLETED ===")

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

                        # Check for database errors
                        has_database_errors = file_results.get("database_errors", [])

                        if has_database_errors:
                            self.logger.error(
                                f"Excel processing completed with database errors for: {filename}"
                            )
                            for error in has_database_errors:
                                self.logger.error(f"  Database error: {error}")
                            return (
                                False,
                                f"Database errors: {', '.join(has_database_errors)}",
                            )
                        elif tables_count == 0:
                            # No tables were processed - this is a failure
                            self.logger.error(
                                f"Excel processing failed: No tables were processed from file {filename}"
                            )

                            # Collect detailed error information
                            sheets = file_results.get("sheets", {})
                            failed_sheets = []
                            for sheet_name, sheet_result in sheets.items():
                                if not sheet_result.get("success", False):
                                    error = sheet_result.get("error", "Unknown error")
                                    failed_sheets.append(
                                        f"Sheet '{sheet_name}': {error}"
                                    )
                                    self.logger.error(
                                        f"  Sheet '{sheet_name}': Failed - {error}"
                                    )

                            # Create detailed error message
                            if failed_sheets:
                                error_message = f"No tables processed from file {filename}. Failed sheets: {'; '.join(failed_sheets)}"
                            else:
                                error_message = (
                                    f"No tables processed from file {filename}"
                                )

                            return False, error_message
                        else:
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
                                    sheet_tables = sheet_result.get(
                                        "tables_processed", 0
                                    )
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
            # Get log file name using the same logic as logging service
            log_file_name = None
            if self.run_start_time:
                # Use the original filename for log filename (same logic as logging_service.py)
                # First, extract just the filename without path (same as logging_service.py)
                import os

                base_filename = os.path.basename(filename)

                # Use the original filename (just replace spaces with underscores for file system compatibility)
                log_filename_base = base_filename.replace(" ", "_")
                timestamp = self.run_start_time.strftime("%Y%m%d_%H%M%S")
                log_file_name = f"{log_filename_base}_{timestamp}.log"

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
