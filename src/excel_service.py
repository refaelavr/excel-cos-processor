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

# Import from same directory (src/)
from logger import print_success, print_error, print_warning, print_normal
from config_manager import ConfigManager, get_config
from extractors import (
    extract_key_values,
    extract_custom_tables_col_count,
    extract_no_title_tables_dynamic_headers,
    add_key_values_to_table,
    apply_calculated_columns,
    rename_table_columns,
    merge_tables,
)


class ExcelProcessingService:
    """
    Service for processing Excel files with intelligent filename pattern matching.

    Automatically handles files with dates, timestamps, and version numbers in filenames
    by cleaning the filename and matching against configuration keys.
    """

    def __init__(self, config_manager: Optional[ConfigManager] = None):
        self.config = config_manager or get_config()
        self.processed_files = []
        self.processing_stats = {
            "files_processed": 0,
            "tables_extracted": 0,
            "rows_processed": 0,
            "errors": 0,
        }

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

        Args:
            filename: Original filename that may contain date patterns

        Returns:
            Clean filename suitable for configuration lookup

        Examples:
            "דוח העדרויות נהגים מסכם 13.7.xlsx" → "דוח העדרויות נהגים מסכם"
            "vm_analysis_20240815_143022.xlsx" → "vm_analysis"
            "ניתוח קנסות VM 2024.xlsx" → "ניתוח קנסות VM"
            "סטטוס אי ביצוע בזמן אמת - YIT - נתונים להיום26-08-2025 21-15-00.xlsx" → "סטטוס אי ביצוע בזמן אמת - YIT - נתונים להיום"
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
        clean_name = re.sub(
            r"\s*\d{1,2}[\.\/\-]\d{1,2}[\.\/\-]\d{2,4}$", "", clean_name
        )

        # Step 5: Remove standalone numbers/partial dates at end
        # Matches patterns like: " 13.7", " 2024", " 15", " 13/8"
        clean_name = re.sub(
            r"\s+\d{1,4}[\.\-\/]?\d{0,2}[\.\-\/]?\d{0,4}$", "", clean_name
        )

        # Step 6: Clean up extra whitespace
        clean_name = clean_name.strip()

        return clean_name

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

        print_normal(f"    Filename Analysis:")
        print_normal(f"      Original: '{filename}'")
        print_normal(f"      Cleaned:  '{clean_filename}'")

        # Attempt direct configuration lookup
        if self.config.get_file_config(clean_filename):
            print_success(f"      Result: Configuration found for '{clean_filename}'")
            return clean_filename

        # Configuration not found - file doesn't follow naming convention
        print_warning(f"      Result: No configuration exists for '{clean_filename}'")
        print_warning(
            f"      Action: File will be skipped (naming convention not followed)"
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
            print_normal(f"Created input directory: {input_dir}")
            return []

        # Scan for Excel files
        excel_files = []
        for filename in os.listdir(input_dir):
            if filename.lower().endswith((".xlsx", ".xls")):
                excel_files.append(os.path.join(input_dir, filename))

        if not excel_files:
            print_normal("File Discovery: No Excel files found in input directory")
            return []

        print_normal(f"File Discovery: Found {len(excel_files)} Excel files")
        for file_path in excel_files:
            print_normal(f"  - {os.path.basename(file_path)}")

        # Validate files against configuration
        valid_files = []
        invalid_count = 0

        print_normal(f"\nFile Validation: Checking configuration mappings")

        for file_path in excel_files:
            file_name = os.path.basename(file_path)

            # Attempt configuration resolution
            config_key = self._get_config_key_from_filename(file_name)

            if config_key:
                valid_files.append(file_path)
                print_success(f"  Valid: '{file_name}' → '{config_key}'")
            else:
                invalid_count += 1
                print_warning(f"  Invalid: '{file_name}' (no matching configuration)")

        # Log validation summary
        print_normal(f"\nValidation Summary:")
        print_normal(f"  Total Files Scanned: {len(excel_files)}")
        print_success(f"  Valid Files: {len(valid_files)}")

        if invalid_count > 0:
            print_warning(f"  Invalid Files: {invalid_count}")
            print_warning(
                f"  Note: Invalid files don't follow naming conventions and will be ignored"
            )
        else:
            print_success(f"  All files follow naming conventions")

        return valid_files

    def process_all_files(self) -> Dict[str, Any]:
        """Main entry point - process all Excel files"""
        try:
            print_normal("=== Starting Excel File Processing ===")

            # Get valid Excel files using intelligent discovery
            excel_files = self._get_valid_excel_files()
            if not excel_files:
                print_warning("No valid Excel files found to process")
                return self._build_result(success=True, message="No files to process")

            # Process each file
            results = {}
            tables_for_merge = {}

            for file_path in excel_files:
                try:
                    file_result = self._process_single_file(file_path, tables_for_merge)
                    results[file_path] = file_result
                    self.processing_stats["files_processed"] += 1

                    if file_result["success"]:
                        self._archive_processed_file(file_path)

                except Exception as e:
                    print_error(f"Error processing file {file_path}: {str(e)}")
                    self.processing_stats["errors"] += 1
                    results[file_path] = {"success": False, "error": str(e)}

            # Process merge operations
            merge_results = self._process_merge_operations(tables_for_merge)

            print_success(f"Processing completed: {self.processing_stats}")

            return self._build_result(
                success=True,
                message="Processing completed successfully",
                file_results=results,
                merge_results=merge_results,
                stats=self.processing_stats,
            )

        except Exception as e:
            print_error(f"Fatal error in Excel processing: {str(e)}")
            self.processing_stats["errors"] += 1
            return self._build_result(success=False, error=str(e))

    def _process_single_file(
        self, file_path: str, tables_for_merge: Dict
    ) -> Dict[str, Any]:
        """Process a single Excel file using intelligent filename mapping"""
        file_name = os.path.basename(file_path)

        print_normal(f"\nProcessing File: {file_name}")

        # Get configuration key using intelligent pattern matching
        config_key = self._get_config_key_from_filename(file_name)
        if not config_key:
            raise ValueError(f"No configuration found for file: {file_name}")

        # Get file configuration using the resolved key
        file_config = self.config.get_file_config(config_key)
        if not file_config:
            raise ValueError(f"Configuration missing for resolved key: {config_key}")

        print_normal(f"  Using Configuration: '{config_key}'")

        try:
            # Load Excel file
            xl = pd.ExcelFile(file_path)
            file_results = {"success": True, "sheets": {}, "tables_count": 0}

            # Store file-level key_values (shared across all sheets in file)
            file_level_key_values = {}

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

                except Exception as e:
                    print_error(f"Error processing sheet {sheet_name}: {str(e)}")
                    file_results["sheets"][sheet_name] = {
                        "success": False,
                        "error": str(e),
                    }
                    self.processing_stats["errors"] += 1

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

        print_normal(f"  Processing sheet: {sheet_name}")

        # Load sheet data
        df = xl.parse(sheet_name, header=None)

        # Extract key values from this sheet
        key_values_def = sheet_config.get("key_values", [])
        key_values = extract_key_values(df, key_values_def)

        # Update file-level key_values with values from this sheet
        # This allows other sheets to use key_values from previous sheets
        if key_values:
            file_level_key_values.update(key_values)
            print_normal(
                f"    Updated file-level key_values: {list(key_values.keys())}"
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
                print_warning(
                    f"    Failed to parse report_date '{report_date_str}': {str(e)}"
                )
                data_date_for_flattening = datetime.now()
        else:
            data_date_for_flattening = datetime.now()

        print_normal("    Key Values extracted:")
        for k, v in key_values.items():
            print_normal(f"      {k}: {v}")

        # Show file-level key_values if different from sheet key_values
        if file_level_key_values != key_values:
            print_normal("    File-level key_values available:")
            for k, v in file_level_key_values.items():
                print_normal(f"      {k}: {v}")

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
                print_normal(f"    Table '{title}': Found ({len(table)} rows)")

                # Process table
                processed_table = self._process_table_data(
                    table, table_def, key_values, key_values_def, file_level_key_values
                )

                # Export to database if configured
                if (
                    table_def.get("export_to_db", False)
                    and self.config.processing.enable_database
                ):
                    db_success, db_error = self._export_table_to_database(
                        processed_table, title, table_def
                    )
                    if not db_success:
                        # Store database error for later reporting
                        if not hasattr(self, "database_errors"):
                            self.database_errors = []
                        self.database_errors.append(f"{title}: {db_error}")

                self.processing_stats["tables_extracted"] += 1
                self.processing_stats["rows_processed"] += len(processed_table)
                tables_processed += 1
            else:
                print_warning(f"    Table '{title}': Not found")

        return tables_processed

    def _process_no_title_tables(
        self,
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
            start_row = no_title_table["start_row"]

            # Extract table with all parameters
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
                print_warning(f"    No-title table '{title}': Not found")
                continue

            print_normal(f"    No-title table '{title}': Found ({len(table)} rows)")

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
                print_normal(
                    f"      Stored for merging with '{no_title_table['merge_with']}'"
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

                # Export to database if configured
                if (
                    no_title_table.get("export_to_db", False)
                    and self.config.processing.enable_database
                ):
                    db_success, db_error = self._export_table_to_database(
                        processed_table, title, no_title_table
                    )
                    if not db_success:
                        # Store database error for later reporting
                        if not hasattr(self, "database_errors"):
                            self.database_errors = []
                        self.database_errors.append(f"{title}: {db_error}")

            self.processing_stats["tables_extracted"] += 1
            self.processing_stats["rows_processed"] += len(table)
            tables_processed += 1

        return tables_processed

    def _process_table_data(
        self,
        table: pd.DataFrame,
        table_config: Dict,
        key_values: Dict,
        key_values_def: List,
        file_level_key_values: Dict = None,  # Optional file-level key_values
    ) -> pd.DataFrame:
        """Apply all processing steps to a table"""
        processed_table = table.copy()

        # Add key values if requested
        if table_config.get("add_keys", False):
            processed_table = add_key_values_to_table(
                processed_table, key_values, key_values_def
            )
            print_normal("      Added key values")

        # Apply calculated columns
        calculated_columns = table_config.get("calculated_columns")
        if calculated_columns:
            print_normal(f"      Applying {len(calculated_columns)} calculated columns")
            processed_table = apply_calculated_columns(
                processed_table, calculated_columns
            )

        # Rename columns according to headers config
        custom_headers = table_config.get("headers")
        if custom_headers:
            print_normal("      Renaming columns according to headers config")
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
                    print_normal(
                        f"      Added date column: {formatted_date} (from file-level key_values)"
                    )
                except Exception as e:
                    print_warning(f"      Failed to add date column: {str(e)}")
            else:
                print_warning(
                    "      Cannot add date column: no report_date found in key_values"
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
            print_warning(f"      {error_msg}")
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
            )

            if success:
                print_success(f"      Successfully exported '{title}' to database")
                return True, ""
            else:
                print_error(
                    f"      Failed to export '{title}' to database: {error_msg}"
                )
                self.processing_stats["errors"] += 1
                return False, error_msg

        except Exception as e:
            error_msg = f"Database export error for '{title}': {str(e)}"
            print_error(f"      {error_msg}")
            self.processing_stats["errors"] += 1
            return False, error_msg

    def _process_merge_operations(self, tables_for_merge: Dict) -> Dict[str, Any]:
        """Process all table merge operations"""
        if not tables_for_merge:
            print_normal("No tables require merging")
            return {"merges_processed": 0}

        print_normal("\n=== Processing Table Merges ===")

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

            print_normal(f"\nProcessing merge: {title}")
            print_normal(f"  File 1: {file_name}")
            print_normal(f"  File 2: {merge_with_file}")
            print_normal(f"  Merge on: {merge_on}")

            # Find partner table
            partner_info = self._find_merge_partner(
                tables_for_merge, merge_with_file, title, merge_key
            )

            if partner_info is None:
                print_error("  ERROR: Partner table not found")
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
                        print_normal(
                            f"  Applying {len(all_calc_cols)} calculated columns to merged table"
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
                    print_success(f"  Merge completed successfully")
                else:
                    print_error(f"  ERROR: Merge failed")
                    merge_results["errors"] += 1

            except Exception as e:
                print_error(f"  ERROR: Merge operation failed: {str(e)}")
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
                print_warning(
                    "  WARNING: Merged table marked for DB export but no primary_keys found!"
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
                print_success(f"  Successfully exported merged table to database")
            else:
                print_error(f"  Failed to export merged table to database")

        except Exception as e:
            print_error(f"  Error exporting merged table: {str(e)}")

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
            print_success(f"Archived file to: {dest_path}")
            self.processed_files.append(dest_path)

        except Exception as e:
            print_error(f"Error archiving file {file_path}: {str(e)}")

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


def test_excel_service():
    """Test Excel service functionality"""
    try:
        print("\n" + "=" * 50)
        print("EXCEL SERVICE TEST")
        print("=" * 50)

        # Test service initialization
        excel_service = ExcelProcessingService()
        print("Excel service initialized")

        # Test file discovery
        excel_files = excel_service._get_valid_excel_files()
        print(f"Found {len(excel_files)} valid Excel files")

        if excel_files:
            print("Files ready for processing:")
            for file_path in excel_files:
                print(f"  - {os.path.basename(file_path)}")
        else:
            print("No Excel files found in input directory")
            print("  Put some Excel files in data/input/ to test processing")

        return True

    except Exception as e:
        print(f"Excel service test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_excel_service()
