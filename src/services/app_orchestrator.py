"""
Main application orchestrator that coordinates all services.
"""

import os
import sys
from typing import Optional
from models.processing_result import ProcessingResult
from utils.environment_utils import get_environment, is_production
from services.logging_service import LoggingService
from services.trigger_service import TriggerService
from services.file_processing_service import FileProcessingService


class AppOrchestrator:
    """Main orchestrator that coordinates all services."""

    def __init__(self):
        self.logger = LoggingService("ExcelProcessor")
        self.cos_service = None
        self.archive_service = None
        self.file_processing_service = None
        self.trigger_service = TriggerService(self.logger)
        self.excel_service = None
        self.database_service = None

        # Initialize services
        self._initialize_services()

    def _initialize_services(self) -> None:
        """Initialize all required services."""
        try:
            # Log environment information
            self.logger.log_environment_info()

            # Initialize COS service (only in production)
            if is_production():
                self._initialize_cos_service()
            else:
                # Test mode: Initialize archive service without COS
                self.logger.info("Initializing local archive service for test mode...")
                self._initialize_local_archive_service()

            # Initialize Excel service
            self._initialize_excel_service()

            # Initialize database service
            self._initialize_database_service()

            # Initialize file processing service
            self.file_processing_service = FileProcessingService(
                self.cos_service,
                self.archive_service,
                self.excel_service,
                self.database_service,
                self.logger,
            )

            self.logger.info("All services initialized successfully")

        except Exception as e:
            self.logger.error(f"Error initializing services: {str(e)}")
            raise

    def _initialize_cos_service(self) -> None:
        """Initialize COS service for production mode."""
        try:
            from services.cos_service import COSService
            from services.archive_service import ArchiveService

            bucket_name = os.getenv("COS_BUCKET_NAME", "")
            self.logger.info(f"COS_BUCKET_NAME: {bucket_name}")

            # Debug all COS environment variables
            cos_vars = [
                "IAM_API_KEY",
                "COS_INSTANCE_ID",
                "COS_INTERNAL_ENDPOINT",
                "COS_ENDPOINT",
            ]
            for var in cos_vars:
                value = os.getenv(var, "")
                if value:
                    self.logger.info(
                        f"{var}: {value[:10]}..."
                        if len(value) > 10
                        else f"{var}: {value}"
                    )
                else:
                    self.logger.warning(f"{var}: Not set")

            if bucket_name:
                self.cos_service = COSService(bucket_name, self.logger)
                self.archive_service = ArchiveService(self.cos_service, self.logger)
                self.logger.info("COS and Archive services initialized")
            else:
                self.logger.warning("COS_BUCKET_NAME not configured")
                self.cos_service = None
        except ImportError as e:
            self.logger.warning(f"Could not import COS service: {str(e)}")
            self.logger.info("Continuing without COS service")
            self.cos_service = None
        except Exception as e:
            self.logger.error(f"Error initializing COS service: {str(e)}")
            self.cos_service = None

    def _initialize_local_archive_service(self) -> None:
        """Initialize local archive service for test mode."""
        try:
            self.logger.info("Attempting to initialize local archive service...")
            from src.services.archive_service import ArchiveService

            self.archive_service = ArchiveService(None, self.logger)
            self.logger.info("Local archive service initialized successfully")
        except ImportError as e:
            self.logger.warning(f"Could not import archive service: {str(e)}")
            self.logger.info("Continuing without archive service")
            self.archive_service = None
        except Exception as e:
            self.logger.error(f"Error initializing local archive service: {str(e)}")
            self.archive_service = None

    def _initialize_excel_service(self) -> None:
        """Initialize Excel processing service."""
        try:
            from src.config_manager import get_config
            from src.excel_service import ExcelProcessingService

            config = get_config()
            self.excel_service = ExcelProcessingService(config, self.logger)
            self.logger.info("Excel processing service initialized")

        except ImportError as e:
            self.logger.warning(f"Could not import Excel service: {str(e)}")
            self.logger.info("Continuing without Excel service")
        except Exception as e:
            self.logger.error(f"Error initializing Excel service: {str(e)}")
            self.excel_service = None

    def _initialize_database_service(self) -> None:
        """Initialize database service."""
        try:
            from database_service import DatabaseService
            from config_manager import get_config

            config = get_config()
            if config.processing.enable_database:
                self.database_service = DatabaseService(config.database.to_dict())
                if self.database_service.test_connection():
                    self.logger.info("Database service initialized and connected")
                else:
                    self.logger.warning(
                        "Database connection failed - continuing without DB"
                    )
                    self.database_service = None
            else:
                self.logger.info("Database processing disabled in configuration")

        except ImportError as e:
            self.logger.warning(f"Could not import database service: {str(e)}")
            self.logger.info("Continuing without database service")
            self.database_service = None
        except Exception as e:
            self.logger.error(f"Error initializing database service: {str(e)}")
            self.database_service = None

    def process_single_file(self, filename: str) -> int:
        """Process a single file based on environment."""
        try:
            # Debug logging
            self.logger.info(
                f"Environment check: is_production()={is_production()}, cos_service={self.cos_service is not None}"
            )

            if is_production() and self.cos_service:
                # Production: Process from COS
                self.logger.info(f"=== Production Mode: Processing COS File ===")
                self.logger.info(f"Processing triggered file: {filename}")
                result = self.file_processing_service.process_single_cos_file(filename)
            else:
                # Test mode: Process local file
                self.logger.info(f"=== Test Mode: Processing Local File ===")
                # Extract just the filename without path for local processing
                local_filename = os.path.basename(filename)
                file_path = os.path.join("data", "input", local_filename)
                if not os.path.exists(file_path):
                    self.logger.error(f"File not found: {file_path}")
                    return 1

                result = self.file_processing_service.process_single_local_file(
                    file_path
                )

            if result.success:
                self.logger.info("Processing completed successfully")
                return 0
            else:
                self.logger.error("Processing failed")
                return 1

        except Exception as e:
            self.logger.error(f"Unexpected error in file processing: {str(e)}")
            return 1
        finally:
            self._cleanup()

    def run(self) -> int:
        """Main run method - determines filename and processes it."""
        try:
            # Get filename to process
            filename = self.trigger_service.get_processing_filename()
            if not filename:
                self.logger.error("No filename to process")
                return 1

            # Process the file
            return self.process_single_file(filename)

        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            return 1
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Clean up resources."""
        try:
            # Upload logs if in production
            if is_production() and self.cos_service:
                self._upload_logs()

            # Clean up services
            if self.database_service:
                self.database_service.close()

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    def _upload_logs(self) -> None:
        """Upload logs to COS."""
        try:
            # Find the most recent log file
            from pathlib import Path
            from datetime import datetime

            today = datetime.now().strftime("%d%m%Y")
            log_dir = Path("logs") / today
            log_file = log_dir / "complete_excel_processor.log"

            if log_file.exists():
                self.cos_service.upload_logs(str(log_file))

        except Exception as e:
            self.logger.error(f"Error uploading logs: {str(e)}")
