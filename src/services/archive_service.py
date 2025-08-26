"""
Archive service for file archival operations.
"""

import os
import sys
import shutil
from datetime import datetime
from typing import Optional

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.file_utils import create_archive_filename, get_filename_from_path
from utils.environment_utils import is_production


class ArchiveService:
    """Service for file archival operations."""

    def __init__(self, cos_service, logger):
        self.cos_service = cos_service
        self.logger = logger

    def archive_cos_file(self, cos_key: str, success: bool = True) -> Optional[str]:
        """Archive a file from COS to archive folder."""
        if not self.cos_service:
            self.logger.warning("COS service not available - skipping COS archive")
            return None

        try:
            # Create archive path
            archive_date = datetime.now().strftime("%Y%m%d")
            archived_filename = create_archive_filename(
                get_filename_from_path(cos_key), success
            )

            if success:
                archive_key = f"archive/{archive_date}/success/{archived_filename}"
                self.logger.info(f"Archiving successful processing: {cos_key}")
            else:
                archive_key = f"archive/{archive_date}/failed/{archived_filename}"
                self.logger.warning(f"Archiving failed processing: {cos_key}")

            # Copy file to archive location
            if self.cos_service.copy_file(cos_key, archive_key):
                self.logger.info(
                    f"Successfully archived '{cos_key}' to '{archive_key}'"
                )

                # Delete original file
                if self.cos_service.delete_file(cos_key):
                    self.logger.info(f"Deleted original file: {cos_key}")
                else:
                    self.logger.warning(f"Failed to delete original file: {cos_key}")

                return archive_key
            else:
                self.logger.error(f"Failed to archive {cos_key}")
                return None

        except Exception as e:
            self.logger.error(f"Error archiving {cos_key}: {str(e)}")
            return None

    def archive_local_file(self, file_path: str, success: bool = True) -> Optional[str]:
        """Archive a local file to archive directory."""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File not found for archiving: {file_path}")
                return None

            # Create archive directory
            archive_date = datetime.now().strftime("%Y%m%d")
            archive_folder = "success" if success else "failed"
            archive_dir = os.path.join("data", "archive", archive_date, archive_folder)
            os.makedirs(archive_dir, exist_ok=True)

            # Create archive filename
            archived_filename = create_archive_filename(
                get_filename_from_path(file_path), success
            )
            archive_path = os.path.join(archive_dir, archived_filename)

            # Move file to archive
            shutil.move(file_path, archive_path)

            if success:
                self.logger.info(
                    f"Archived successful file '{file_path}' to '{archive_path}'"
                )
            else:
                self.logger.warning(
                    f"Archived failed file '{file_path}' to '{archive_path}'"
                )

            return archive_path

        except Exception as e:
            self.logger.error(f"Error archiving {file_path}: {str(e)}")
            return None

    def archive_batch_files(self, file_paths: list, success: bool = True) -> list:
        """Archive multiple local files."""
        archived_files = []

        for file_path in file_paths:
            try:
                archive_path = self.archive_local_file(file_path, success)
                if archive_path:
                    archived_files.append(archive_path)
            except Exception as e:
                self.logger.error(f"Error archiving {file_path}: {str(e)}")

        return archived_files

    def cleanup_old_archives(self, days_to_keep: int = 30) -> None:
        """Clean up old archive files (optional maintenance)."""
        if not is_production():
            self.logger.info("Skipping archive cleanup (not in PROD mode)")
            return

        try:
            # This would implement cleanup logic for old archives
            # For now, just log that it's not implemented
            self.logger.info(
                f"Archive cleanup not implemented (would keep {days_to_keep} days)"
            )
        except Exception as e:
            self.logger.error(f"Error during archive cleanup: {str(e)}")
