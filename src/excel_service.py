"""
Excel Processing Service
Handles all Excel file operations and data extraction with intelligent filename pattern matching
"""

import pandas as pd
import os
import re
from typing import Dict, List, Tuple, Optional, Any, Union
from pathlib import Path
from datetime import datetime
import shutil

# Constants
DEFAULT_BATCH_SIZE = 1000
DEFAULT_DATE_THRESHOLD = 0.5  # 50% threshold for date column detection
MAX_SAMPLE_SIZE = 10  # Maximum number of values to sample for type detection

# Import from same directory (src/)
from src.config_manager import ConfigManager, get_config
from src.extractors import (
    extract_key_values,
    extract_custom_tables_col_count,
    extract_no_title_tables_dynamic_headers,
    add_key_values_to_table,
    apply_calculated_columns,
    rename_table_columns,
    merge_tables,
    extract_concatenated_tables,
    extract_multi_concatenated_tables,
)


class ExcelProcessingService:
    """
    Service for processing Excel files with intelligent filename pattern matching.

    Automatically handles files with dates, timestamps, and version numbers in filenames
    by cleaning the filename and matching against configuration keys.
    """

    def __init__(
        self,
        config_manager: Optional[ConfigManager] = None,
        logger: Optional[Any] = None,
    ):
        """Initialize the Excel processing service.

        Args:
            config_manager: Configuration manager instance. If None, uses default config.
            logger: Logger instance for logging operations. If None, uses print statements.
        """
        self.config = config_manager or get_config()
        self.logger = logger
        self.processed_files: List[str] = []
        self.processing_stats: Dict[str, int] = {
            "files_processed": 0,
            "tables_extracted": 0,
            "rows_processed": 0,
            "errors": 0,
        }

        # Set logger for extractors functions
        if logger:
            from src.extractors import set_logger

            set_logger(logger)

    def _clean_filename_from_date_patterns(self, filename: str) -> str:
        """
        Remove date and timestamp patterns from filename to match configuration keys.

        This function handles the common practice of adding dates/timestamps to filenames
        while keeping the core business name that should match the configuration.

        Supported patterns:
        - File extensions: .xlsx, .xls
        - Timestamps: _YYYYMMDD_HHMMSS (e.g., _20240815_143022)
        - Date formats: DD.MM.YYYY, DD/MM/YYYY, DD-MM-YYYY
        - Time formats: HH-MM-SS, HH:MM:SS
        - Standalone numbers: 13.7, 2024, etc.
        - Hebrew month names: ינואר, פברואר, מרץ, אפריל, מאי, יוני, יולי, אוגוסט, ספטמבר, אוקטובר, נובמבר, דצמבר
        - Hebrew month with "חודש" prefix: חודש מאי, חודש יוני, etc.
        - Extra numbers and suffixes: -1, -7, 0, 1, 7, etc.

        Args:
            filename: Original filename that may contain date patterns

        Returns:
            Clean filename suitable for configuration lookup

        Examples:
            "דוח העדרויות נהגים מסכם 13.7.xlsx" → "דוח העדרויות נהגים מסכם"
            "vm_analysis_20240815_143022.xlsx" → "vm_analysis"
            "ניתוח קנסות VM 2024.xlsx" → "ניתוח קנסות VM"
            "סטטוס אי ביצוע בזמן אמת - YIT - נתונים להיום26-08-2025 21-15-00.xlsx" → "סטטוס אי ביצוע בזמן אמת - YIT - נתונים להיום"
            "מהירות מסחרית הסכם משרד התחבורה יוני.xlsx" → "מהירות מסחרית הסכם משרד התחבורה"
            "ניתוח קנסות VM חודש מאי.xlsx" → "ניתוח קנסות VM"
            "ניתוח קנסות VM אקסל04-09-20250.xlsx" → "ניתוח קנסות VM אקסל"
            "ניתוח קנסות VM אקסל03-09-2025-7.xlsx" → "ניתוח קנסות VM אקסל"
            "ניתוח קנסות VM אקסל03-09-2025-1.xlsx" → "ניתוח קנסות VM אקסל"
        """
        clean_name = filename

        # Step 1: Remove file extension
        if clean_name.lower().endswith((".xlsx", ".xls")):
            clean_name = os.path.splitext(clean_name)[0]

        # Step 2: Remove timestamp patterns (_YYYYMMDD_HHMMSS)
        # Common in automated file generation systems
        clean_name = re.sub(r"_\d{8}_\d{6}$", "", clean_name)

        # Step 3: Remove date-time patterns at the end
        # Matches: DD-MM-YYYY HH-MM-SS or DD-MM-YYYY HH:MM:SS (with or without leading space)
        clean_name = re.sub(
            r"\s*\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}-\d{1,2}-\d{1,2}$", "", clean_name
        )
        clean_name = re.sub(
            r"\s*\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{1,2}:\d{1,2}$", "", clean_name
        )

        # Step 4: Remove date patterns at end (without time)
        # Matches: DD.MM.YYYY, DD/MM/YYYY, DD-MM-YYYY with optional leading space
        # Also handles cases with extra digits like: DD-MM-YYYY0, DD-MM-YYYY-1, etc.
        clean_name = re.sub(
            r"\s*\d{1,2}[\.\/\-]\d{1,2}[\.\/\-]\d{2,4}\d*$", "", clean_name
        )
        # Also remove patterns with trailing dashes and numbers
        clean_name = re.sub(
            r"\s*\d{1,2}[\.\/\-]\d{1,2}[\.\/\-]\d{2,4}-\d+$", "", clean_name
        )

        # Step 5: Remove standalone numbers/partial dates at end
        # Matches patterns like: " 13.7", " 2024", " 15", " 13/8"
        clean_name = re.sub(
            r"\s+\d{1,4}[\.\-\/]?\d{0,2}[\.\-\/]?\d{0,4}$", "", clean_name
        )

        # Step 6: Remove Hebrew month names
        # Hebrew month names in various formats
        hebrew_months = [
            r"\s*ינואר\s*",
            r"\s*פברואר\s*",
            r"\s*מרץ\s*",
            r"\s*אפריל\s*",
            r"\s*מאי\s*",
            r"\s*יוני\s*",
            r"\s*יולי\s*",
            r"\s*אוגוסט\s*",
            r"\s*ספטמבר\s*",
            r"\s*אוקטובר\s*",
            r"\s*נובמבר\s*",
            r"\s*דצמבר\s*",
            # Alternative spellings and variations
            r"\s*חודש\s+ינואר\s*",
            r"\s*חודש\s+פברואר\s*",
            r"\s*חודש\s+מרץ\s*",
            r"\s*חודש\s+אפריל\s*",
            r"\s*חודש\s+מאי\s*",
            r"\s*חודש\s+יוני\s*",
            r"\s*חודש\s+יולי\s*",
            r"\s*חודש\s+אוגוסט\s*",
            r"\s*חודש\s+ספטמבר\s*",
            r"\s*חודש\s+אוקטובר\s*",
            r"\s*חודש\s+נובמבר\s*",
            r"\s*חודש\s+דצמבר\s*",
        ]

        for month_pattern in hebrew_months:
            clean_name = re.sub(month_pattern, " ", clean_name)

        # Additional cleanup: Remove standalone "חודש" if it remains
        clean_name = re.sub(r"\s*חודש\s*", " ", clean_name)

        # Step 7: Remove extra numbers and suffixes at the end
        # This handles cases like: "ניתוח קנסות VM אקסל04-09-20250" → "ניתוח קנסות VM אקסל"
        # Or: "ניתוח קנסות VM אקסל03-09-2025-7" → "ניתוח קנסות VM אקסל"
        # Or: "ניתוח קנסות VM אקסל03-09-2025-1" → "ניתוח קנסות VM אקסל"

        # Remove patterns like: -1, -7, -12, etc. (dash followed by numbers)
        clean_name = re.sub(r"-\d+$", "", clean_name)

        # Remove patterns like: 0, 1, 7, 12, etc. (standalone numbers at the end)
        clean_name = re.sub(r"\d+$", "", clean_name)

        # Remove any remaining trailing dashes or underscores
        clean_name = re.sub(r"[-_]+$", "", clean_name)

        # Step 8: Clean up extra whitespace
        clean_name = clean_name.strip()

        return clean_name

    def _log(self, message: str, level: str = "INFO") -> None:
        """Log message using the main logging system if available."""
        if hasattr(self, "logger") and self.logger:
            if level == "INFO":
                self.logger.info(message)
            elif level == "SUCCESS":
                self.logger.info(f"SUCCESS: {message}")
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
        else:
            # Fallback to print if no logger available
            print(f"[{level}] {message}")

    def _get_config_key_from_filename(self, filename: str) -> Optional[str]:
        """
        Resolve configuration key from filename using pattern-based cleaning.

        This method implements a simple but effective filename matching strategy:
        1. Clean the filename from common date/timestamp patterns
        2. Check if the cleaned name exists in configuration
        3. If not found, the file doesn't follow naming conventions

        This approach ensures that files follow established naming conventions
        while being flexible enough to handle date variations.

        Args:
            filename: Original filename from filesystem

        Returns:
            Configuration key if match found, None if naming convention not followed
        """
        # Apply filename cleaning to remove temporal patterns
        clean_filename = self._clean_filename_from_date_patterns(filename)

        self._log(f"    Filename Analysis:", "INFO")
        self._log(f"      Original: '{filename}'", "INFO")
        self._log(f"      Cleaned:  '{clean_filename}'", "INFO")

        # Attempt direct configuration lookup
        if self.config.get_file_config(clean_filename):
            self._log(
                f"      Result: Configuration found for '{clean_filename}'", "SUCCESS"
            )
            return clean_filename

        # Configuration not found - file doesn't follow naming convention
        self._log(
            f"      Result: No configuration exists for '{clean_filename}'", "WARNING"
        )
        self._log(
            f"      Action: File will be skipped (naming convention not followed)",
            "WARNING",
        )
        return None

    def _get_valid_excel_files(self) -> List[str]:
        """
        Discover and validate Excel files in input directory.

        Scans the configured input directory for Excel files and validates each file
        against the configuration using intelligent filename pattern matching.

        Files that don't follow naming conventions are logged and skipped.
        This enforces consistent file naming practices across the organization.

        Returns:
            List of file paths that have valid configuration mappings
        """
        abs_paths = self.config.processing.get_absolute_paths()
        input_dir = abs_paths["input_dir"]

        # Ensure input directory exists
        if not os.path.exists(input_dir):
            os.makedirs(input_dir)
            self._log(f"Created input directory: {input_dir}", "INFO")
            return []

        # Scan for Excel files
        excel_files = []
        for filename in os.listdir(input_dir):
            if filename.lower().endswith((".xlsx", ".xls")):
                excel_files.append(os.path.join(input_dir, filename))

        if not excel_files:
            self._log("File Discovery: No Excel files found in input directory", "INFO")
            return []

        self._log(f"File Discovery: Found {len(excel_files)} Excel files", "INFO")
        for file_path in excel_files:
            self._log(f"  - {os.path.basename(file_path)}", "INFO")

        # Validate files against configuration
        valid_files = []
        invalid_count = 0

        self._log(f"\nFile Validation: Checking configuration mappings", "INFO")

        for file_path in excel_files:
            file_name = os.path.basename(file_path)

            # Attempt configuration resolution
            config_key = self._get_config_key_from_filename(file_name)

            if config_key:
                valid_files.append(file_path)
                self._log(f"  Valid: '{file_name}' → '{config_key}'", "SUCCESS")
            else:
                invalid_count += 1
                self._log(
                    f"  Invalid: '{file_name}' (no matching configuration)", "WARNING"
                )

        # Log validation summary
        self._log(f"\nValidation Summary:", "INFO")
        self._log(f"  Total Files Scanned: {len(excel_files)}", "INFO")
        self._log(f"  Valid Files: {len(valid_files)}", "SUCCESS")

        if invalid_count > 0:
            self._log(f"  Invalid Files: {invalid_count}", "WARNING")
            self._log(
                f"  Note: Invalid files don't follow naming conventions and will be ignored",
                "WARNING",
            )
        else:
            self._log(f"  All files follow naming conventions", "SUCCESS")

        return valid_files

    def process_all_files(self) -> Dict[str, Any]:
        """Main entry point - process all Excel files"""
        try:
            self._log("=== Starting Excel File Processing ===", "INFO")

            # Get valid Excel files using intelligent discovery
            excel_files = self._get_valid_excel_files()
            if not excel_files:
                self._log("No valid Excel files found to process", "WARNING")
                return self._build_result(success=True, message="No files to process")

            # Process each file
            results = {}
            tables_for_merge = {}

            for file_path in excel_files:
                try:
                    file_result = self._process_single_file(file_path, tables_for_merge)
                    results[file_path] = file_result
                    self.processing_stats["files_processed"] += 1

                    # Only archive if processing was successful AND no database errors
                    if file_result["success"] and not file_result.get(
                        "database_errors", []
                    ):
                        self._archive_processed_file(file_path)
                    elif file_result.get("database_errors", []):
                        self._log(
                            f"Not archiving file due to database errors: {os.path.basename(file_path)}",
                            "WARNING",
                        )

                except Exception as e:
                    self._log(f"Error processing file {file_path}: {str(e)}", "ERROR")
                    self.processing_stats["errors"] += 1
                    results[file_path] = {"success": False, "error": str(e)}

            # Process merge operations
            merge_results = self._process_merge_operations(tables_for_merge)

            # Check if there were any database errors
            has_database_errors = any(
                file_result.get("database_errors", [])
                for file_result in results.values()
            )

            # Determine overall success based on errors
            overall_success = (
                self.processing_stats["errors"] == 0 and not has_database_errors
            )

            if overall_success:
                self._log(
                    f"Processing completed successfully: {self.processing_stats}",
                    "SUCCESS",
                )
                message = "Processing completed successfully"
            else:
                self._log(
                    f"Processing completed with errors: {self.processing_stats}",
                    "ERROR",
                )
                message = f"Processing completed with {self.processing_stats['errors']} errors"

            return self._build_result(
                success=overall_success,
                message=message,
                file_results=results,
                merge_results=merge_results,
                stats=self.processing_stats,
            )

        except Exception as e:
            self._log(f"Fatal error in Excel processing: {str(e)}", "ERROR")
            self.processing_stats["errors"] += 1
            return self._build_result(success=False, error=str(e))

    def _process_single_file(
        self, file_path: str, tables_for_merge: Dict
    ) -> Dict[str, Any]:
        """Process a single Excel file using intelligent filename mapping"""
        file_name = os.path.basename(file_path)

        self._log(f"\nProcessing File: {file_name}", "INFO")

        # Get configuration key using intelligent pattern matching
        config_key = self._get_config_key_from_filename(file_name)
        if not config_key:
            raise ValueError(f"No configuration found for file: {file_name}")

        # Get file configuration using the resolved key
        file_config = self.config.get_file_config(config_key)
        if not file_config:
            raise ValueError(f"Configuration missing for resolved key: {config_key}")

        self._log(f"  Using Configuration: '{config_key}'", "INFO")

        try:
            # Load Excel file
            xl = pd.ExcelFile(file_path)
            file_results = {"success": True, "sheets": {}, "tables_count": 0}

            # Store file-level key_values (shared across all sheets in file)
            file_level_key_values = {}

            # Initialize file-level database errors collection
            file_database_errors = []

            # Process each configured sheet
            for sheet_name, sheet_config in file_config.items():
                try:
                    sheet_result = self._process_sheet(
                        xl,
                        sheet_name,
                        sheet_config,
                        config_key,
                        tables_for_merge,
                        file_level_key_values,
                    )
                    file_results["sheets"][sheet_name] = sheet_result
                    file_results["tables_count"] += sheet_result.get(
                        "tables_processed", 0
                    )

                    # Collect database errors from this sheet
                    sheet_database_errors = sheet_result.get("database_errors", [])
                    if sheet_database_errors:
                        file_database_errors.extend(sheet_database_errors)

                except Exception as e:
                    self._log(f"Error processing sheet {sheet_name}: {str(e)}", "ERROR")
                    file_results["sheets"][sheet_name] = {
                        "success": False,
                        "error": str(e),
                    }
                    self.processing_stats["errors"] += 1

            # Add file-level database errors to results
            file_results["database_errors"] = file_database_errors

            return file_results

        except Exception as e:
            raise Exception(f"Failed to process Excel file {file_name}: {str(e)}")

    def _process_sheet(
        self,
        xl: pd.ExcelFile,
        sheet_name: str,
        sheet_config: Dict,
        file_name: str,
        tables_for_merge: Dict,
        file_level_key_values: Dict,  # Shared key_values for entire file
    ) -> Dict[str, Any]:
        """Process a single sheet within an Excel file"""

        if sheet_name not in xl.sheet_names:
            raise ValueError(f"Sheet '{sheet_name}' not found in file")

        self._log(f"  Processing sheet: {sheet_name}", "INFO")

        # Load sheet data
        df = xl.parse(sheet_name, header=None)

        # Extract key values from this sheet
        key_values_def = sheet_config.get("key_values", [])
        key_values = extract_key_values(df, key_values_def)

        # Update file-level key_values with values from this sheet
        # This allows other sheets to use key_values from previous sheets
        if key_values:
            file_level_key_values.update(key_values)
            self._log(
                f"    Updated file-level key_values: {list(key_values.keys())}", "INFO"
            )

        # Determine data date for flattening operations
        # Use file-level key_values instead of just sheet key_values
        report_date_str = file_level_key_values.get("report_date")
        if report_date_str:
            try:
                # Convert string date to datetime object for flattening
                data_date_for_flattening = datetime.strptime(
                    report_date_str, "%d/%m/%Y"
                )
            except Exception as e:
                self._log(
                    f"    Failed to parse report_date '{report_date_str}': {str(e)}",
                    "WARNING",
                )
                data_date_for_flattening = datetime.now()
        else:
            data_date_for_flattening = datetime.now()

        self._log("    Key Values extracted:", "INFO")
        for k, v in key_values.items():
            self._log(f"      {k}: {v}", "INFO")

        # Show file-level key_values if different from sheet key_values
        if file_level_key_values != key_values:
            self._log("    File-level key_values available:", "INFO")
            for k, v in file_level_key_values.items():
                self._log(f"      {k}: {v}", "INFO")

        sheet_result = {
            "success": True,
            "tables_processed": 0,
            "key_values": key_values,
            "file_level_key_values": file_level_key_values,
        }

        # Initialize database errors collection for this sheet
        sheet_database_errors = []

        # Process regular tables with titles
        tables_processed = self._process_regular_tables(
            df, sheet_config, key_values, key_values_def, file_level_key_values
        )
        sheet_result["tables_processed"] += tables_processed

        # Process no-title tables
        no_title_processed = self._process_no_title_tables(
            xl,
            df,
            sheet_config,
            key_values,
            key_values_def,
            file_name,
            sheet_name,
            data_date_for_flattening,
            tables_for_merge,
            file_level_key_values,
        )
        sheet_result["tables_processed"] += no_title_processed

        # Collect database errors from this sheet's processing
        if hasattr(self, "database_errors") and self.database_errors:
            sheet_database_errors.extend(self.database_errors)
            # Clear the errors for next sheet
            self.database_errors = []

        sheet_result["database_errors"] = sheet_database_errors

        return sheet_result

    def _process_regular_tables(
        self,
        df: pd.DataFrame,
        sheet_config: Dict,
        key_values: Dict,
        key_values_def: List,
        file_level_key_values: Dict,  # File-level key_values
    ) -> int:
        """Process regular tables with titles"""
        tables_def = sheet_config.get("tables", [])
        if not tables_def:
            return 0

        tables = extract_custom_tables_col_count(df, tables_def)
        tables_processed = 0

        for table_def in tables_def:
            title = table_def["title"]
            table = tables.get(title)

            if table is not None:
                self._log(f"    Table '{title}': Found ({len(table)} rows)", "INFO")

                # Process table
                processed_table = self._process_table_data(
                    table, table_def, key_values, key_values_def, file_level_key_values
                )

                # Export table (to database or CSV based on configuration)
                export_to_db = table_def.get("export_to_db", False)
                if export_to_db and self.config.processing.enable_database:
                    # Export to database
                    db_success, db_error = self._export_table_to_database(
                        processed_table, title, table_def
                    )
                    if not db_success:
                        # Store database error for later reporting
                        if not hasattr(self, "database_errors"):
                            self.database_errors = []
                        self.database_errors.append(f"{title}: {db_error}")
                elif not export_to_db:
                    # Export to CSV
                    db_success, db_error = self._export_table_to_database(
                        processed_table, title, table_def
                    )
                    if not db_success:
                        # Store export error for later reporting
                        if not hasattr(self, "database_errors"):
                            self.database_errors = []
                        self.database_errors.append(f"{title}: {db_error}")

                self.processing_stats["tables_extracted"] += 1
                self.processing_stats["rows_processed"] += len(processed_table)
                tables_processed += 1
            else:
                self._log(f"    Table '{title}': Not found", "WARNING")

        return tables_processed

    def _process_no_title_tables(
        self,
        xl: pd.ExcelFile,
        df: pd.DataFrame,
        sheet_config: Dict,
        key_values: Dict,
        key_values_def: List,
        file_name: str,
        sheet_name: str,
        data_date_for_flattening: datetime,
        tables_for_merge: Dict,
        file_level_key_values: Dict,  # File-level key_values
    ) -> int:
        """Process no-title tables"""
        no_title_tables_def = sheet_config.get("no_title_tables", [])
        if not no_title_tables_def:
            return 0

        tables_processed = 0

        for no_title_table in no_title_tables_def:
            title = no_title_table["title"]
            table_type = no_title_table.get(
                "type", "standard"
            )  # Default to standard type

            # Handle concatenate_tables type
            if table_type == "concatenate_tables":
                table = self._process_concatenate_tables(
                    df,
                    no_title_table,
                    key_values,
                    key_values_def,
                    file_level_key_values,
                )
            # Handle multi_concatenate_tables type
            elif table_type == "multi_concatenate_tables":
                table = self._process_multi_concatenate_tables(
                    xl,
                    no_title_table,
                    key_values,
                    key_values_def,
                    file_level_key_values,
                )
            else:
                # Standard no-title table processing
                start_row = no_title_table["start_row"]
                table = extract_no_title_tables_dynamic_headers(
                    df,
                    start_row,
                    custom_headers=no_title_table.get("headers"),
                    fill_na=no_title_table.get("fill_na", False),
                    flat_table=no_title_table.get("flat_table", False),
                    flat_by=no_title_table.get("flat_by", "day"),
                    data_date=data_date_for_flattening,
                    columns_to_exclude=no_title_table.get("columns_to_exclude"),
                )

            if table is None:
                self._log(f"    No-title table '{title}': Not found", "WARNING")
                continue

            self._log(
                f"    No-title table '{title}': Found ({len(table)} rows)", "INFO"
            )

            # Check if this table needs merging
            if no_title_table.get("merge_with"):
                merge_key = f"{file_name}_{title}"
                tables_for_merge[merge_key] = {
                    "table": table,
                    "file_name": file_name,
                    "sheet_name": sheet_name,
                    "title": title,
                    "merge_with": no_title_table["merge_with"],
                    "merge_on": no_title_table.get("merge_on"),
                    "config": no_title_table,
                }
                self._log(
                    f"      Stored for merging with '{no_title_table['merge_with']}'",
                    "INFO",
                )
            else:
                # Process table immediately
                processed_table = self._process_table_data(
                    table,
                    no_title_table,
                    key_values,
                    key_values_def,
                    file_level_key_values,
                )

                # Export table (to database or CSV based on configuration)
                export_to_db = no_title_table.get("export_to_db", False)
                if export_to_db and self.config.processing.enable_database:
                    # Export to database
                    db_success, db_error = self._export_table_to_database(
                        processed_table, title, no_title_table
                    )
                    if not db_success:
                        # Store database error for later reporting
                        if not hasattr(self, "database_errors"):
                            self.database_errors = []
                        self.database_errors.append(f"{title}: {db_error}")
                elif not export_to_db:
                    # Export to CSV
                    db_success, db_error = self._export_table_to_database(
                        processed_table, title, no_title_table
                    )
                    if not db_success:
                        # Store export error for later reporting
                        if not hasattr(self, "database_errors"):
                            self.database_errors = []
                        self.database_errors.append(f"{title}: {db_error}")

            self.processing_stats["tables_extracted"] += 1
            self.processing_stats["rows_processed"] += len(table)
            tables_processed += 1

        return tables_processed

    def _process_concatenate_tables(
        self,
        df: pd.DataFrame,
        table_config: Dict,
        key_values: Dict,
        key_values_def: List,
        file_level_key_values: Dict,
    ) -> pd.DataFrame:
        """Process concatenate_tables type - extract and concatenate two tables"""
        try:
            concatenate_config = table_config.get("concatenate_config", {})
            if not concatenate_config:
                self._log("      ERROR: No concatenate_config found", "ERROR")
                return None

            self._log("      Processing concatenate_tables type", "INFO")

            # Extract concatenated table
            table = extract_concatenated_tables(
                df, concatenate_config, custom_headers=table_config.get("headers")
            )

            if table is None:
                self._log("      ERROR: Failed to extract concatenated tables", "ERROR")
                return None

            self._log(
                f"      Successfully extracted concatenated table: {len(table)} rows",
                "SUCCESS",
            )
            return table

        except Exception as e:
            self._log(f"      ERROR in _process_concatenate_tables: {str(e)}", "ERROR")
            return None

    def _process_multi_concatenate_tables(
        self,
        xl: pd.ExcelFile,
        table_config: Dict,
        key_values: Dict,
        key_values_def: List,
        file_level_key_values: Dict,
    ) -> pd.DataFrame:
        """Process multi_concatenate_tables type - extract and concatenate multiple tables from multiple sheets cumulatively"""
        try:
            multi_concatenate_config = table_config.get("multi_concatenate_config", {})
            if not multi_concatenate_config:
                self._log("      ERROR: No multi_concatenate_config found", "ERROR")
                return None

            self._log(
                "      Processing multi_concatenate_tables type cumulatively", "INFO"
            )

            # Collect all sheets data needed for processing
            sheets_config = multi_concatenate_config.get("sheets", [])
            all_sheets_data = {}

            for sheet_config in sheets_config:
                sheet_name = sheet_config.get("sheet_name")
                if sheet_name in xl.sheet_names:
                    df = xl.parse(sheet_name, header=None)
                    all_sheets_data[sheet_name] = df
                    self._log(f"      Loaded sheet data: {sheet_name}", "INFO")
                else:
                    self._log(
                        f"      WARNING: Sheet '{sheet_name}' not found in file",
                        "WARNING",
                    )

            if not all_sheets_data:
                self._log("      ERROR: No sheets data loaded", "ERROR")
                return None

            # Extract multi-concatenated table using all sheets data
            table = extract_multi_concatenated_tables(
                all_sheets_data,
                multi_concatenate_config,
                custom_headers=table_config.get("headers"),
                key_values=key_values,
            )

            if table is None:
                self._log(
                    "      ERROR: Failed to extract multi-concatenated tables", "ERROR"
                )
                return None

            self._log(
                f"      Successfully extracted multi-concatenated table: {len(table)} rows",
                "SUCCESS",
            )
            return table

        except Exception as e:
            self._log(
                f"      ERROR in _process_multi_concatenate_tables: {str(e)}", "ERROR"
            )
            return None

    def _process_table_data(
        self,
        table: pd.DataFrame,
        table_config: Dict,
        key_values: Dict,
        key_values_def: List,
        file_level_key_values: Dict = None,  # Optional file-level key_values
    ) -> pd.DataFrame:
        """Apply all processing steps to a table"""

        # Check if table is empty to prevent IndexError
        if table.empty:
            self._log("      WARNING: Table is empty, skipping processing", "WARNING")
            return table

        processed_table = table.copy()

        # Add key values if requested
        if table_config.get("add_keys", False):
            processed_table = add_key_values_to_table(
                processed_table, key_values, key_values_def
            )
            self._log("      Added key values", "INFO")

        # Special handling for concatenate_tables and multi_concatenate_tables types
        if table_config.get("type") in [
            "concatenate_tables",
            "multi_concatenate_tables",
        ]:
            self._log(
                f"      Special processing for {table_config.get('type')} type", "INFO"
            )

            # For concatenate_tables and multi_concatenate_tables, rename columns FIRST, then apply calculated columns
            # This is because the formulas reference the renamed column names
            custom_headers = table_config.get("headers")
            if custom_headers:
                self._log(
                    "      Renaming columns according to headers config (BEFORE calculated columns)",
                    "INFO",
                )
                processed_table = rename_table_columns(processed_table, custom_headers)

            # Apply calculated columns AFTER renaming
            calculated_columns = table_config.get("calculated_columns")
            if calculated_columns:
                self._log(
                    f"      Applying {len(calculated_columns)} calculated columns (AFTER renaming)",
                    "INFO",
                )
                processed_table = apply_calculated_columns(
                    processed_table, calculated_columns, key_values
                )
        else:
            # Normal processing for all other table types
            # Apply calculated columns first, then rename columns
            calculated_columns = table_config.get("calculated_columns")
            if calculated_columns:
                self._log(
                    f"      Applying {len(calculated_columns)} calculated columns",
                    "INFO",
                )
                processed_table = apply_calculated_columns(
                    processed_table, calculated_columns, key_values
                )

            # Rename columns according to headers config
            custom_headers = table_config.get("headers")
            if custom_headers:
                self._log("      Renaming columns according to headers config", "INFO")
                processed_table = rename_table_columns(processed_table, custom_headers)

        # Add data date using file-level key_values (for shared report_date)
        if table_config.get("add_data_date", False):
            # Try to get report_date from file-level key_values first, then sheet key_values
            available_key_values = file_level_key_values or key_values
            data_date = available_key_values.get("report_date")

            if data_date:
                try:
                    date_obj = datetime.strptime(data_date, "%d/%m/%Y")
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                    processed_table["date"] = [formatted_date] * len(processed_table)
                    self._log(
                        f"      Added date column: {formatted_date} (from file-level key_values)",
                        "INFO",
                    )
                except Exception as e:
                    self._log(f"      Failed to add date column: {str(e)}", "WARNING")
            else:
                self._log(
                    "      Cannot add date column: no report_date found in key_values",
                    "WARNING",
                )

        return processed_table

    def _export_table_to_database(
        self, table: pd.DataFrame, title: str, table_config: Dict
    ) -> tuple[bool, str]:
        """
        Export table to database using DatabaseService

        Returns:
            tuple: (success: bool, error_message: str)
        """
        primary_keys = table_config.get("primary_keys")
        if not primary_keys:
            error_msg = (
                f"WARNING: '{title}' marked for DB export but no primary_keys defined!"
            )
            self._log(f"      {error_msg}", "WARNING")
            return False, error_msg

        try:
            from database_service import DatabaseService

            db_service = DatabaseService(self.config.database.to_dict())

            success, error_msg = db_service.export_table(
                table,
                title,
                primary_keys,
                skip_empty_updates=table_config.get("skip_empty_updates", False),
                explicit_table_name=table_config.get("table_name"),
                export_to_db=table_config.get("export_to_db", True),
            )

            if success:
                self._log(
                    f"      Successfully exported '{title}' to database", "SUCCESS"
                )
                return True, ""
            else:
                self._log(
                    f"      Failed to export '{title}' to database: {error_msg}",
                    "ERROR",
                )
                self.processing_stats["errors"] += 1
                return False, error_msg

        except Exception as e:
            error_msg = f"Database export error for '{title}': {str(e)}"
            self._log(f"      {error_msg}", "ERROR")
            self.processing_stats["errors"] += 1
            return False, error_msg

    def _process_merge_operations(self, tables_for_merge: Dict) -> Dict[str, Any]:
        """Process all table merge operations"""
        if not tables_for_merge:
            self._log("No tables require merging", "INFO")
            return {"merges_processed": 0}

        self._log("\n=== Processing Table Merges ===", "INFO")

        processed_merges = set()
        merge_results = {"merges_processed": 0, "errors": 0}

        for merge_key, table_info in tables_for_merge.items():
            file_name = table_info["file_name"]
            title = table_info["title"]
            merge_with_file = table_info["merge_with"]
            merge_on = table_info["merge_on"]

            # Create unique merge identifier
            merge_pair = tuple(sorted([file_name, merge_with_file]))
            merge_id = f"{merge_pair}_{title}"

            if merge_id in processed_merges:
                continue  # Already processed

            self._log(f"\nProcessing merge: {title}", "INFO")
            self._log(f"  File 1: {file_name}", "INFO")
            self._log(f"  File 2: {merge_with_file}", "INFO")
            self._log(f"  Merge on: {merge_on}", "INFO")

            # Find partner table
            partner_info = self._find_merge_partner(
                tables_for_merge, merge_with_file, title, merge_key
            )

            if partner_info is None:
                self._log("  ERROR: Partner table not found", "ERROR")
                merge_results["errors"] += 1
                continue

            # Perform merge
            try:
                merged_table = merge_tables(
                    table_info["table"],
                    partner_info["table"],
                    merge_on,
                    file_name,
                    merge_with_file,
                )

                if merged_table is not None:
                    # Apply calculated columns from both sources
                    calc_cols_1 = table_info["config"].get("calculated_columns", [])
                    calc_cols_2 = partner_info["config"].get("calculated_columns", [])
                    all_calc_cols = calc_cols_1 + calc_cols_2

                    if all_calc_cols:
                        self._log(
                            f"  Applying {len(all_calc_cols)} calculated columns to merged table",
                            "INFO",
                        )
                        merged_table = apply_calculated_columns(
                            merged_table, all_calc_cols
                        )

                    # Export merged table to database if configured
                    if self._should_export_merged_table(table_info, partner_info):
                        self._export_merged_table_to_database(
                            merged_table, table_info, partner_info
                        )

                    processed_merges.add(merge_id)
                    merge_results["merges_processed"] += 1
                    self._log(f"  Merge completed successfully", "SUCCESS")
                else:
                    self._log(f"  ERROR: Merge failed", "ERROR")
                    merge_results["errors"] += 1

            except Exception as e:
                self._log(f"  ERROR: Merge operation failed: {str(e)}", "ERROR")
                merge_results["errors"] += 1

        return merge_results

    def _find_merge_partner(
        self, tables_for_merge: Dict, merge_with_file: str, title: str, exclude_key: str
    ) -> Optional[Dict]:
        """Find the partner table for merging"""
        expected_partner_key = f"{merge_with_file}_{title}"

        # Try exact match first
        if expected_partner_key in tables_for_merge:
            return tables_for_merge[expected_partner_key]

        # Search manually
        for key, info in tables_for_merge.items():
            if (
                key != exclude_key
                and info["file_name"] == merge_with_file
                and info["title"] == title
            ):
                return info

        return None

    def _should_export_merged_table(
        self, table_info_1: Dict, table_info_2: Dict
    ) -> bool:
        """Check if merged table should be exported to database"""
        export_1 = table_info_1["config"].get("export_to_db", False)
        export_2 = table_info_2["config"].get("export_to_db", False)
        return (export_1 or export_2) and self.config.processing.enable_database

    def _export_merged_table_to_database(
        self, merged_table: pd.DataFrame, table_info_1: Dict, table_info_2: Dict
    ):
        """Export merged table to database"""
        try:
            # Get primary keys from either config
            primary_keys_1 = table_info_1["config"].get("primary_keys")
            primary_keys_2 = table_info_2["config"].get("primary_keys")
            primary_keys = primary_keys_1 or primary_keys_2

            if not primary_keys:
                self._log(
                    "  WARNING: Merged table marked for DB export but no primary_keys found!",
                    "WARNING",
                )
                return

            # Create descriptive table name
            safe_file1 = table_info_1["file_name"].split()[-1]
            safe_file2 = table_info_2["file_name"].split()[-1]
            merged_title = f"MERGED_{table_info_1['title']}_{safe_file1}_{safe_file2}"

            from database_service import DatabaseService

            db_service = DatabaseService(self.config.database.to_dict())

            success = db_service.export_table(merged_table, merged_title, primary_keys)

            if success:
                self._log(
                    f"  Successfully exported merged table to database", "SUCCESS"
                )
            else:
                self._log(f"  Failed to export merged table to database", "ERROR")

        except Exception as e:
            self._log(f"  Error exporting merged table: {str(e)}", "ERROR")

    def _archive_processed_file(self, file_path: str):
        """Move processed file to archive directory"""
        try:
            abs_paths = self.config.processing.get_absolute_paths()
            today_str = datetime.now().strftime("%d%m%Y")
            archive_dir = os.path.join(abs_paths["archive_dir"], today_str)
            os.makedirs(archive_dir, exist_ok=True)

            original_name = os.path.basename(file_path)
            name, ext = os.path.splitext(original_name)
            new_filename = f"{name}_{today_str}{ext}"
            dest_path = os.path.join(archive_dir, new_filename)

            shutil.move(file_path, dest_path)
            self._log(f"Archived file to: {dest_path}", "SUCCESS")
            self.processed_files.append(dest_path)

        except Exception as e:
            self._log(f"Error archiving file {file_path}: {str(e)}", "ERROR")

    def _build_result(
        self, success: bool, message: str = "", **kwargs
    ) -> Dict[str, Any]:
        """Build standardized result dictionary"""
        result = {
            "success": success,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "processed_files": self.processed_files,
            "stats": self.processing_stats,
        }
        result.update(kwargs)
        return result

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return self.processing_stats.copy()

    def reset_stats(self):
        """Reset processing statistics"""
        self.processing_stats = {
            "files_processed": 0,
            "tables_extracted": 0,
            "rows_processed": 0,
            "errors": 0,
        }
        self.processed_files = []
