import os
import pandas as pd
from typing import Optional

# Global logger for extractors
_logger: Optional[object] = None


def set_logger(logger) -> None:
    """Set the logger for extractors functions.

    Args:
        logger: Logger instance to use for logging operations
    """
    global _logger
    _logger = logger


def _log_message(msg: str, level: str = "INFO") -> None:
    """Internal logging function that uses the global logger if available.

    Args:
        msg: Message to log
        level: Log level (INFO, SUCCESS, WARNING, ERROR)
    """
    if _logger:
        if level == "SUCCESS":
            _logger.info(f"SUCCESS: {msg}")
        elif level == "ERROR":
            _logger.error(msg)
        elif level == "WARNING":
            _logger.warning(msg)
        else:
            _logger.info(msg)
    else:
        # Fallback to console output
        prefix = f"{level}: " if level != "INFO" else ""
        print(f"{prefix}{msg}")


def print_success(msg: str) -> None:
    """Log a success message."""
    _log_message(msg, "SUCCESS")


def print_error(msg: str) -> None:
    """Log an error message."""
    _log_message(msg, "ERROR")


def print_warning(msg: str) -> None:
    """Log a warning message."""
    _log_message(msg, "WARNING")


def print_normal(msg: str) -> None:
    """Log a normal info message."""
    _log_message(msg, "INFO")


def convert_hebrew_month_abbreviation(month_abbrev):
    """
    Convert abbreviated Hebrew month to full Hebrew month name.

    Args:
        month_abbrev: Abbreviated Hebrew month (e.g., 'ינו', 'פבר', 'מרץ')

    Returns:
        Full Hebrew month name (e.g., 'ינואר', 'פברואר', 'מרץ')
    """
    if not month_abbrev or pd.isna(month_abbrev):
        return month_abbrev

    # Hebrew month mappings (abbreviated to full)
    hebrew_month_mapping = {
        "ינו": "ינואר",
        "פבר": "פברואר",
        "מרץ": "מרץ",
        "אפר": "אפריל",
        "מאי": "מאי",
        "יונ": "יוני",
        "יול": "יולי",
        "אוג": "אוגוסט",
        "ספט": "ספטמבר",
        "אוק": "אוקטובר",
        "נוב": "נובמבר",
        "דצמ": "דצמבר",
    }

    month_str = str(month_abbrev).strip()
    full_month = hebrew_month_mapping.get(month_str, month_str)

    if full_month != month_str:
        print_normal(f"      Converted Hebrew month '{month_str}' -> '{full_month}'")

    return full_month


def parse_hebrew_month_date(hebrew_date_str):
    """
    Parse Hebrew month format (e.g., 'יונ-2025') to YYYY-MM-01 format.

    Args:
        hebrew_date_str: String in format like 'יונ-2025', 'דצמ-2024', etc.

    Returns:
        String in format 'YYYY-MM-01' or None if parsing fails
    """
    if not hebrew_date_str or pd.isna(hebrew_date_str):
        return None

    try:
        hebrew_date_str = str(hebrew_date_str).strip()

        # Hebrew month mappings (abbreviated forms commonly used)
        hebrew_months = {
            "ינו": "01",  # ינואר
            "פבר": "02",  # פברואר
            "מרץ": "03",  # מרץ
            "אפר": "04",  # אפריל
            "מאי": "05",  # מאי
            "יונ": "06",  # יוני
            "יול": "07",  # יולי
            "אוג": "08",  # אוגוסט
            "ספט": "09",  # ספטמבר
            "אוק": "10",  # אוקטובר
            "נוב": "11",  # נובמבר
            "דצמ": "12",  # דצמבר
            # Additional variations
            "ינואר": "01",
            "פברואר": "02",
            "מרץ": "03",
            "אפריל": "04",
            "מאי": "05",
            "יוני": "06",
            "יולי": "07",
            "אוגוסט": "08",
            "ספטמבר": "09",
            "אוקטובר": "10",
            "נובמבר": "11",
            "דצמבר": "12",
        }

        # Split by dash or other separators
        if "-" in hebrew_date_str:
            parts = hebrew_date_str.split("-")
        elif " " in hebrew_date_str:
            parts = hebrew_date_str.split(" ")
        else:
            print_warning(f"Could not parse Hebrew date format: {hebrew_date_str}")
            return None

        if len(parts) != 2:
            print_warning(f"Unexpected Hebrew date format: {hebrew_date_str}")
            return None

        month_part = parts[1].strip()
        year_part = parts[0].strip()

        # Look up month
        month_num = hebrew_months.get(month_part)
        if not month_num:
            print_warning(f"Unknown Hebrew month: {month_part}")
            return None

        # Validate and format year
        if not year_part.isdigit() or len(year_part) != 4:
            print_warning(f"Invalid year format: {year_part}")
            return None

        result = f"{year_part}-{month_num}-01"
        print_normal(f"      Converted Hebrew date '{hebrew_date_str}' to '{result}'")
        return result

    except Exception as e:
        print_warning(f"Error parsing Hebrew date '{hebrew_date_str}': {str(e)}")
        return None


def extract_key_values(df, key_defs):
    """
    Extract key-value pairs from DataFrame based on config.
    key_defs: dict with keys and their location ({'row': x, 'col': y})
    Supports date formatting via 'format' parameter
    Supports Hebrew month parsing for format '%Y-%m-01'
    Supports adding days to dates via 'add_days' parameter
    Returns a dict of {key: value}
    """
    results = {}
    for key in key_defs:
        title = key["title"]
        row = key.get("row")
        col = key.get("col")
        date_format = key.get("format")  # Get date format if specified
        add_days = key.get("add_days", 0)  # Get number of days to add (default: 0)

        if row is not None and col is not None:
            try:
                value = df.iloc[row, col]

                # Apply date formatting if specified
                if date_format and value is not None:
                    try:
                        # Handle Hebrew month format specifically for %Y-%m-01
                        if date_format == "%Y-%m-01":
                            # First try to parse as Hebrew month format
                            hebrew_result = parse_hebrew_month_date(value)
                            if hebrew_result:
                                print_normal(
                                    f"      Formatted Hebrew month '{value}' -> '{hebrew_result}'"
                                )
                                value = hebrew_result
                            else:
                                # If Hebrew parsing fails, try to parse as regular date and extract first day of month
                                try:
                                    # Handle various date formats including datetime
                                    if isinstance(value, str) and value.strip():
                                        # Try common date formats
                                        input_formats = [
                                            "%d/%m/%Y %H:%M:%S",  # 13/07/2025 23:01:19
                                            "%d/%m/%Y",  # 13/07/2025
                                            "%d.%m.%Y %H:%M:%S",  # 13.07.2025 23:01:19
                                            "%d.%m.%Y",  # 13.07.2025
                                            "%Y-%m-%d %H:%M:%S",  # 2025-07-13 23:01:19
                                            "%Y-%m-%d",  # 2025-07-13
                                        ]
                                        parsed_date = None

                                        for input_format in input_formats:
                                            try:
                                                parsed_date = datetime.strptime(
                                                    value.strip(), input_format
                                                )
                                                break
                                            except ValueError:
                                                continue

                                        if parsed_date:
                                            # Extract first day of month in YYYY-MM-01 format
                                            first_day_of_month = parsed_date.replace(
                                                day=1
                                            ).strftime("%Y-%m-%d")
                                            print_normal(
                                                f"      Converted date '{value}' to first day of month: '{first_day_of_month}'"
                                            )
                                            value = first_day_of_month
                                        else:
                                            print_warning(
                                                f"      Warning: Could not parse date '{value}' in any known format"
                                            )

                                    elif hasattr(
                                        value, "strftime"
                                    ):  # pandas datetime or datetime object
                                        # Extract first day of month
                                        if hasattr(value, "replace"):
                                            first_day_of_month = value.replace(
                                                day=1
                                            ).strftime("%Y-%m-%d")
                                        else:
                                            # For pandas datetime, convert to datetime first
                                            dt_obj = pd.to_datetime(value)
                                            first_day_of_month = dt_obj.replace(
                                                day=1
                                            ).strftime("%Y-%m-%d")

                                        print_normal(
                                            f"      Converted datetime '{value}' to first day of month: '{first_day_of_month}'"
                                        )
                                        value = first_day_of_month

                                    elif pd.notna(value):
                                        # Try to convert to datetime first
                                        try:
                                            parsed_date = pd.to_datetime(
                                                value, dayfirst=True
                                            )
                                            first_day_of_month = parsed_date.replace(
                                                day=1
                                            ).strftime("%Y-%m-%d")
                                            print_normal(
                                                f"      Converted value '{value}' to first day of month: '{first_day_of_month}'"
                                            )
                                            value = first_day_of_month
                                        except (ValueError, TypeError):
                                            print_warning(
                                                f"      Warning: Could not convert value '{value}' to first day of month"
                                            )

                                except Exception as e:
                                    print_warning(
                                        f"      Warning: Error processing date '{value}' for first day of month: {e}"
                                    )
                        else:
                            # Handle other date formats (existing logic)
                            import pandas as pd
                            from datetime import datetime

                            # Handle different input formats
                            if isinstance(value, str) and value.strip():
                                # Try common date formats for string inputs
                                input_formats = [
                                    "%d.%m.%y",
                                    "%d/%m/%Y",
                                    "%d-%m-%Y",
                                    "%Y-%m-%d",
                                    "%d.%m.%Y",
                                ]
                                parsed_date = None

                                for input_format in input_formats:
                                    try:
                                        parsed_date = datetime.strptime(
                                            value.strip(), input_format
                                        )
                                        break
                                    except ValueError:
                                        continue

                                if parsed_date:
                                    # Add days if specified
                                    if add_days != 0:
                                        from datetime import timedelta

                                        parsed_date = parsed_date + timedelta(
                                            days=add_days
                                        )
                                        print_normal(
                                            f"      Added {add_days} days to date '{value}' -> '{parsed_date}'"
                                        )
                                    formatted_value = parsed_date.strftime(date_format)
                                    print_normal(
                                        f"      Formatted date '{value}' -> '{formatted_value}' using format '{date_format}'"
                                    )
                                    value = formatted_value
                                else:
                                    print_warning(
                                        f"      Warning: Could not parse date '{value}' with any known format"
                                    )

                            elif hasattr(
                                value, "strftime"
                            ):  # pandas datetime or datetime object
                                # Add days if specified
                                if add_days != 0:
                                    from datetime import timedelta

                                    value = value + timedelta(days=add_days)
                                    print_normal(
                                        f"      Added {add_days} days to datetime '{value}'"
                                    )
                                formatted_value = value.strftime(date_format)
                                print_normal(
                                    f"      Formatted date '{value}' -> '{formatted_value}' using format '{date_format}'"
                                )
                                value = formatted_value
                            elif pd.notna(value):
                                # Try to convert to datetime first
                                try:
                                    parsed_date = pd.to_datetime(value, dayfirst=True)
                                    # Add days if specified
                                    if add_days != 0:
                                        from datetime import timedelta

                                        parsed_date = parsed_date + timedelta(
                                            days=add_days
                                        )
                                        print_normal(
                                            f"      Added {add_days} days to pandas datetime '{value}' -> '{parsed_date}'"
                                        )
                                    formatted_value = parsed_date.strftime(date_format)
                                    print_normal(
                                        f"      Formatted date '{value}' -> '{formatted_value}' using format '{date_format}'"
                                    )
                                    value = formatted_value
                                except (ValueError, TypeError):
                                    print_warning(
                                        f"      Warning: Could not format value '{value}' as date with format '{date_format}'"
                                    )

                    except Exception as e:
                        print_warning(
                            f"      Warning: Date formatting error for '{value}' with format '{date_format}': {e}"
                        )
                        # Keep original value if formatting fails

            except Exception as e:
                print_error(
                    f"      Error extracting key '{title}' from row {row}, col {col}: {e}"
                )
                value = None
        else:
            value = None
        results[title] = value
    return results


def extract_custom_tables_col_count(df, table_defs, header_offset=1, min_header_cols=2):
    """
    Extract tables by using col_count from config.
    If col_count is set, extract exactly this many columns from the header position.
    Supports dynamic header renaming via 'headers' config array.
    Supports fill_na option to fill empty columns with zeros instead of dropping them.
    """
    tables = {}
    for table_def in table_defs:
        title = table_def["title"]
        col_count = table_def.get("col_count")
        start_from_end = table_def.get("start_from_end", False)
        custom_headers = table_def.get("headers")
        fill_na = table_def.get("fill_na", False)
        table = None

        for i in range(len(df)):
            row = df.iloc[i].astype(str)
            if any(str(cell).strip() == title for cell in row):
                header_idx = i + header_offset

                if col_count:
                    start_col = list(row).index(title)
                    if start_from_end:
                        # Find total columns in header row
                        header_row = df.iloc[header_idx]
                        total_cols = len(
                            [
                                val
                                for val in header_row
                                if str(val).strip() != ""
                                and not pd.isna(val)
                                and str(val) != "NaT"
                            ]
                        )

                        # Take last col_count columns
                        end_col = start_col + total_cols
                        start_col_adjusted = end_col - col_count
                        table_cols = list(range(start_col_adjusted, end_col))
                        print_normal(
                            f"      Taking {col_count} columns from END: columns {start_col_adjusted}-{end_col-1}"
                        )
                    else:
                        table_cols = list(range(start_col, start_col + col_count))
                        print_normal(
                            f"      Taking {col_count} columns from START: columns {start_col}-{start_col + col_count - 1}"
                        )

                else:
                    # fallback: try to autodetect as before
                    header_row = df.iloc[header_idx]
                    table_cols = [
                        idx
                        for idx, val in enumerate(header_row)
                        if str(val).strip() != ""
                    ]
                data = []
                for j in range(header_idx + 1, len(df)):
                    row_data = df.iloc[j, table_cols]
                    if row_data.count() < min_header_cols:
                        break
                    data.append(row_data)
                if data:
                    header_row = df.iloc[header_idx, table_cols]
                    table = pd.DataFrame(data)

                    # Note: Custom headers will be applied later in the processing flow
                    if custom_headers and len(custom_headers) > 0:
                        num_cols = len(table.columns)
                        print_normal(
                            f"      Table has {num_cols} columns, custom_headers has {len(custom_headers)} headers"
                        )
                        print_normal(
                            f"      Custom headers will be applied later in processing flow"
                        )

                    # Use original headers for now
                    table.columns = header_row

                    # Handle fill_na for existing empty columns
                    if fill_na:
                        # Fill NaN values with 0
                        numeric_cols = table.select_dtypes(include=["number"]).columns
                        table[numeric_cols] = table[numeric_cols].fillna(0)

                        # For non-numeric columns, fill with empty string or 0 based on content
                        for col in table.columns:
                            if col not in numeric_cols:
                                # Try to convert to numeric, if possible fill with 0, otherwise empty string
                                try:
                                    table[col] = pd.to_numeric(
                                        table[col], errors="coerce"
                                    ).fillna(0)
                                except (ValueError, TypeError):
                                    table[col] = table[col].fillna("")

                        print_normal(
                            "      Filled NaN values with zeros (fill_na=True)"
                        )
                        table = table.reset_index(drop=True)
                    else:
                        # Original behavior: drop empty columns
                        table = table.dropna(axis=1, how="all")
                        print_normal(
                            "      Dropped completely empty columns (fill_na=False)"
                        )
                        table = table.reset_index(drop=True)

                break
        tables[title] = table
    return tables


def add_key_values_to_table(table, key_values, key_values_def):
    """
    Adds each key in key_values as a new column to table.

    DEFAULT: Values are added only to the LAST ROW (legacy behavior)

    Supports different placement strategies via "placement" config:
    - "last_row": Add value only to last row (DEFAULT)
    - "all_rows": Add value to all rows
    - "first_row": Add value only to first row

    Added validation for table existence and proper column creation
    """
    if table is None or len(table) == 0:
        return table

    n_rows = len(table)

    for k, v in key_values.items():
        # Find the key definition to check if it should be added to table
        item = next((d for d in key_values_def if d.get("title") == k), None)
        if item and item.get("add_to_table") is True:

            # Get placement strategy from config - DEFAULT is "last_row"
            placement = item.get("placement", "last_row")

            # Create column based on placement strategy
            if placement == "all_rows":
                # Add value to all rows
                col = [v] * n_rows
                print_normal(f"      Added '{k}' to all {n_rows} rows (value: {v})")

            elif placement == "first_row":
                # Add value only to first row, empty for others
                col = [v] + [""] * (n_rows - 1) if n_rows > 0 else []
                print_normal(f"      Added '{k}' to first row only (value: {v})")

            else:  # placement == "last_row" OR any other value defaults to last_row
                # DEFAULT: empty for all rows except last
                col = [""] * n_rows
                if n_rows > 0:
                    col[-1] = v  # Last row gets the value
                print_normal(
                    f"      Added '{k}' to last row only (value: {v}) [DEFAULT]"
                )

            table[k] = col
    return table


def extract_no_title_tables_dynamic_headers(
    df,
    start_row,
    min_values=2,
    custom_headers=None,
    fill_na=False,
    flat_table=False,  # whether to flatten the table
    flat_by="day",  # flattening type
    data_date=None,  # date for flattening
    columns_to_exclude=None,
):
    """
    Extracts a table without header, starting at start_row.
    Finds the first non-empty cell, starts reading headers from there,
    stops at the next empty cell (after started).
    Then extracts the data for those columns only.

    Supports flattening BEFORE applying custom headers
    Supports custom_headers parameter for dynamic column renaming
    Supports fill_na parameter to fill empty columns with zeros
    Better error handling and date parsing
    """
    if start_row >= len(df):
        return None

    header_row = df.iloc[start_row]
    valid_cols = []
    found_first = False

    for idx, val in enumerate(header_row):
        # Check if we have a next row for validation
        next_row_val = None
        if start_row + 1 < len(df):
            next_row_val = df.iloc[start_row + 1, idx]

        if not found_first:
            if str(val).strip() != "" and not pd.isna(val):
                found_first = True
                valid_cols.append(idx)
            elif (
                next_row_val is not None
                and str(next_row_val).strip() != ""
                and not pd.isna(next_row_val)
            ):
                found_first = True
                valid_cols.append(idx)
        else:
            if str(val).strip() == "" or pd.isna(val):
                break
            valid_cols.append(idx)

    if not valid_cols:
        return None

    # Get original headers
    original_headers = [str(header_row[idx]).strip() for idx in valid_cols]

    # Collect data rows after headers, only from the detected columns
    data = []
    for i in range(start_row + 1, len(df)):
        row = df.iloc[i, valid_cols]
        if row.count() < min_values:
            break
        data.append(row)

    if not data:
        return None

    table = pd.DataFrame(data)
    table.columns = original_headers  # Set original headers first

    print_normal(f"      Original table shape: {table.shape}")
    print_normal(f"      Original columns: {list(table.columns)}")

    if columns_to_exclude is not None:
        for col in columns_to_exclude:
            table = table.drop(col, axis=1)
            original_headers.remove(col)

    # Handle flattening BEFORE applying custom headers
    if flat_table and data_date is not None:
        print_normal(f"      Flattening table by '{flat_by}'...")
        table = flatten_no_title_table(table, flat_by, data_date)
        if table is not None:
            print_normal(f"      After flattening shape: {table.shape}")
            print_normal(f"      After flattening columns: {list(table.columns)}")
        else:
            print_error("      ERROR: Flattening returned None")
            return None

    # Handle fill_na for existing NaN values
    if fill_na:
        print_normal("      Filling NaN values")
        table = table.fillna(0)
        print_normal("      Filled NaN values with zeros")

    # NOW apply custom headers (AFTER flattening)
    if custom_headers and len(custom_headers) > 0:
        num_original_cols = len(table.columns)
        print_normal(f"      Custom headers provided: {custom_headers}")
        print_normal(f"      fill_na = {fill_na}")
        print_normal(
            f"      Need to add {max(0, len(custom_headers) - num_original_cols)} extra columns"
        )

        # First, add any missing columns if we have more headers than data
        if len(custom_headers) > num_original_cols:
            additional_headers = custom_headers[num_original_cols:]
            print_normal(f"      Adding columns: {additional_headers}")

            for extra_header in additional_headers:
                if fill_na:
                    table[extra_header] = [0] * len(table)
                    print_normal(
                        f"      Added column '{extra_header}' with {len(table)} zeros"
                    )
                else:
                    table[extra_header] = [""] * len(table)
                    print_normal(
                        f"      Added column '{extra_header}' with {len(table)} empty strings"
                    )

        # Now apply all custom headers
        table.columns = custom_headers[: len(table.columns)]
        print_normal(f"      Applied headers: {list(table.columns)}")

    print_normal(f"      Final table shape: {table.shape}")
    print_normal(f"      Final columns: {list(table.columns)}")

    # Date sorting with better error handling (only for non-flattened tables)
    if not flat_table and len(table.columns) > 0:
        date_col = table.columns[0]
        series = table[date_col]
        if not pd.api.types.is_numeric_dtype(series):
            try:
                parsed_dates = pd.to_datetime(
                    table[date_col], dayfirst=True, errors="coerce"
                )
                if (
                    parsed_dates.notna().sum() > len(table) * 0.5
                ):  # If more than 50% are valid dates
                    table[date_col] = parsed_dates
                    table = table.sort_values(by=date_col, ascending=True).reset_index(
                        drop=True
                    )
            except (ValueError, TypeError):
                pass  # If date parsing fails, continue without sorting

    table = table.reset_index(drop=True)
    return table


def flatten_no_title_table(df, flat_by, data_date):
    """
    Flattens a 'monthly data' table into a single row, with each column as <column>_<area>.
    Assumes first column is area label, rest are metrics.
    Returns: pandas DataFrame with single row containing flattened data.
    """
    if df is None or len(df) == 0:
        return None

    area_col = df.columns[0]
    flat_data = {}

    for _, row in df.iterrows():
        area = str(row[area_col])
        for col in df.columns[1:]:
            flat_col = f"{str(col).strip()}_{area}"
            flat_data[flat_col] = row[col]

    # Add date information based on flat_by parameter
    if flat_by == "month":
        flat_by_str = data_date.strftime("%Y-%m")
    else:
        flat_by_str = data_date.strftime("%d/%m/%Y")

    flat_data = {flat_by: flat_by_str, **flat_data}

    return pd.DataFrame([flat_data])


def merge_tables(table1, table2, merge_on, table1_source, table2_source):
    """
    Merge two tables on a specified column.

    Args:
        table1: First DataFrame to merge
        table2: Second DataFrame to merge
        merge_on: Column name to merge on
        table1_source: Source identifier for table1 (for column suffixes)
        table2_source: Source identifier for table2 (for column suffixes)

    Returns:
        Merged DataFrame or None if merge fails

    Uses pandas outer join to combine tables, keeping all records
    from both sources and adding suffixes to distinguish column origins
    """
    if table1 is None or table2 is None:
        print_error("Cannot merge: one or both tables are None")
        return None

    if len(table1) == 0 or len(table2) == 0:
        print_error("Cannot merge: one or both tables are empty")
        return None

    if merge_on not in table1.columns:
        print_error(f"Merge column '{merge_on}' not found in first table")
        print_normal(f"Available columns in table1: {list(table1.columns)}")
        return None

    if merge_on not in table2.columns:
        print_error(f"Merge column '{merge_on}' not found in second table")
        print_normal(f"Available columns in table2: {list(table2.columns)}")
        return None

    try:
        # Create clean source names for suffixes (extract last word from filename)
        suffix1 = table1_source.split()[-1] if table1_source else "today"
        suffix2 = table2_source.split()[-1] if table2_source else "tomorrow"

        # Perform outer join to keep all records from both tables
        merged = pd.merge(
            table1,
            table2,
            on=merge_on,
            how="outer",
            suffixes=(f"_{suffix1}", f"_{suffix2}"),
        )

        print_success(
            f"Merge successful: {len(table1)} + {len(table2)} rows -> {len(merged)} rows"
        )
        return merged

    except Exception as e:
        print_error(f"Error during merge: {str(e)}")
        return None


def apply_calculated_columns(table, calculated_columns_config, key_values=None):
    """
    Apply calculated columns to a table based on configuration.

    Args:
        table: pandas DataFrame to add calculated columns to
        calculated_columns_config: List of calculation definitions from config
        key_values: Dictionary of key values extracted from sheets (optional)

    Returns:
        DataFrame with additional calculated columns

    Supported calculation types:
    - cumulative_average: Running average up to current row
    - cumulative_sum: Running sum up to current row
    - cumulative_count: Count of non-null values up to current row
    - cumulative_max: Maximum value up to current row
    - cumulative_min: Minimum value up to current row
    - rolling_average: Rolling average over N periods
    - rolling_sum: Rolling sum over N periods
    - percent_of_total: Percentage of total for entire column
    - percent_change: Percentage change from previous row
    - custom_formula: Apply custom pandas expression
    - current_date: Add current date/time
    """
    if table is None or len(table) == 0 or not calculated_columns_config:
        return table

    result_table = table.copy()

    for calc_config in calculated_columns_config:
        try:
            col_name = calc_config["name"]
            calc_type = calc_config["type"]
            source_col = calc_config.get("source_column")

            print_normal(f"      Calculating column '{col_name}' ({calc_type})")

            # Handle current_date type
            if calc_type == "current_date":
                from datetime import datetime

                # Get format from config (default: YYYY-MM-DD)
                date_format = calc_config.get("format", "%Y-%m-%d")
                placement = calc_config.get(
                    "placement", "all_rows"
                )  # Default: all rows get date

                current_date = datetime.now().strftime(date_format)
                n_rows = len(result_table)

                if placement == "all_rows":
                    result_table[col_name] = [current_date] * n_rows
                    print_normal(
                        f"         Added current date '{current_date}' to all {n_rows} rows"
                    )
                elif placement == "first_row":
                    col = [current_date] + [""] * (n_rows - 1) if n_rows > 0 else []
                    result_table[col_name] = col
                    print_normal(
                        f"         Added current date '{current_date}' to first row only"
                    )
                elif placement == "last_row":
                    col = [""] * n_rows
                    if n_rows > 0:
                        col[-1] = current_date
                    result_table[col_name] = col
                    print_normal(
                        f"         Added current date '{current_date}' to last row only"
                    )
                else:
                    # Default to all rows
                    result_table[col_name] = [current_date] * n_rows
                    print_normal(
                        f"         Added current date '{current_date}' to all rows (default)"
                    )

                continue  # Skip to next calculation

            # Ensure we have the source column for other calculation types
            if source_col and source_col not in result_table.columns:
                print_warning(
                    f"         WARNING: Source column '{source_col}' not found. Skipping."
                )
                continue

            # Sort by date if specified for time-based calculations
            if (
                calc_config.get("date_column")
                and calc_config["date_column"] in result_table.columns
            ):
                date_col = calc_config["date_column"]
                result_table = result_table.sort_values(by=date_col).reset_index(
                    drop=True
                )
                print_normal(
                    f"         Sorted by {date_col} for time-based calculation"
                )

            # Apply the calculation based on type
            if calc_type == "cumulative_average":
                result_table[col_name] = result_table[source_col].expanding().mean()

            elif calc_type == "cumulative_sum":
                # Convert to numeric, replacing non-numeric values with 0
                numeric_series = pd.to_numeric(
                    result_table[source_col], errors="coerce"
                ).fillna(0)
                result_table[col_name] = numeric_series.cumsum()

            elif calc_type == "cumulative_count":
                if calc_config.get("condition"):
                    # Count with condition (e.g., "> 1.0")
                    condition = calc_config["condition"]
                    condition_expr = f"result_table[source_col] {condition}"
                    mask = eval(condition_expr)
                    result_table[col_name] = mask.cumsum()
                else:
                    # Count non-null values
                    result_table[col_name] = result_table[source_col].notna().cumsum()

            elif calc_type == "cumulative_max":
                result_table[col_name] = result_table[source_col].expanding().max()

            elif calc_type == "cumulative_min":
                result_table[col_name] = result_table[source_col].expanding().min()

            elif calc_type == "rolling_average":
                window = calc_config.get("window", 7)  # Default 7-period window
                result_table[col_name] = (
                    result_table[source_col].rolling(window=window).mean()
                )

            elif calc_type == "rolling_sum":
                window = calc_config.get("window", 7)
                result_table[col_name] = (
                    result_table[source_col].rolling(window=window).sum()
                )

            elif calc_type == "percent_of_total":
                total = result_table[source_col].sum()
                result_table[col_name] = (result_table[source_col] / total) * 100

            elif calc_type == "percent_change":
                result_table[col_name] = result_table[source_col].pct_change() * 100

            elif calc_type == "custom_formula":
                formula = calc_config.get("formula")
                if formula:
                    print_normal(f"         Original formula: {formula}")
                    print_normal(
                        f"         Available columns: {list(result_table.columns)}"
                    )

                    # Check if formula references a key_value first
                    if key_values and formula in key_values:
                        # Formula is a key_value reference
                        key_value = key_values[formula]
                        print_normal(
                            f"         Using key_value '{formula}': {key_value}"
                        )
                        result_table[col_name] = [key_value] * len(result_table)
                        print_success(
                            f"         Successfully calculated column '{col_name}' from key_value"
                        )
                        continue

                    # Use regex with word boundaries to prevent partial replacements
                    import re

                    safe_formula = formula

                    # Sort columns by length (longest first) to handle overlapping names
                    sorted_columns = sorted(result_table.columns, key=len, reverse=True)

                    for col in sorted_columns:
                        col_str = str(col)
                        # Use word boundaries (\b) to match only complete words
                        pattern = r"\b" + re.escape(col_str) + r"\b"
                        replacement = f"result_table['{col_str}']"
                        safe_formula = re.sub(pattern, replacement, safe_formula)

                    print_normal(f"         Processed formula: {safe_formula}")

                    try:
                        result_table[col_name] = eval(safe_formula)
                        print_success(
                            f"         Successfully calculated column '{col_name}'"
                        )
                    except Exception as e:
                        print_error(f"         ERROR in processed formula: {str(e)}")
                        continue
                else:
                    print_error(
                        "         ERROR: No formula provided for custom calculation"
                    )
                    continue

            elif calc_type == "hebrew_month_conversion":
                # Convert abbreviated Hebrew months to full Hebrew month names
                if source_col and source_col in result_table.columns:
                    result_table[col_name] = result_table[source_col].apply(
                        convert_hebrew_month_abbreviation
                    )
                    print_success(
                        f"         Successfully converted Hebrew months in column '{col_name}'"
                    )
                else:
                    print_error(
                        f"         ERROR: Source column '{source_col}' not found for Hebrew month conversion"
                    )
                    continue

            else:
                print_error(f"         ERROR: Unknown calculation type '{calc_type}'")
                continue

            print_success(f"         Successfully added column '{col_name}'")

        except Exception as e:
            print_error(
                f"         ERROR calculating column '{calc_config.get('name', 'unknown')}': {str(e)}"
            )
            continue

    return result_table


def export_to_csv(table, sheet_name, out_path, title, file):
    """
    Export table to CSV with safe filename generation.

    Better error handling and validation
    """
    if table is None or len(table) == 0:
        print_warning(f"Cannot export {title}: table is empty or None")
        return

    # Create safe filename components
    safe_table = "".join(
        [c if c.isalnum() or c in (" ", "_", "-") else "_" for c in title]
    )
    safe_sheet = "".join(
        [c if c.isalnum() or c in (" ", "_", "-") else "_" for c in sheet_name]
    )
    safe_file = "".join(
        [
            c if c.isalnum() or c in (" ", "_", "-") else "_"
            for c in os.path.splitext(os.path.basename(str(file)))[0]
        ]
    )

    # Create output path
    output_file = os.path.join(out_path, f"{safe_file}__{safe_sheet}__{safe_table}.csv")

    try:
        table.to_csv(output_file, index=False, encoding="utf-8-sig")
        print_success(f"Saved table to {output_file}")
        print_normal(f"  Columns: {list(table.columns)}")
        print_normal(f"  Rows: {len(table)}")
    except Exception as e:
        print_error(f"Error saving table {title}: {str(e)}")


def find_table_by_text_search(df, search_text, exclude_year=True, start_row=0):
    """
    Find a table by searching for specific text in Excel cells.

    Args:
        df: pandas DataFrame (Excel sheet)
        search_text: Text to search for (e.g., "פילוח סוגי ולידציות")
        exclude_year: If True, ignore year numbers in the search (e.g., "2025")
        start_row: Row to start searching from (0-based)

    Returns:
        int: Row number where the text was found, or -1 if not found
    """
    if not search_text or pd.isna(search_text):
        return -1

    print_normal(f"      Searching for text pattern: '{search_text}'")

    # Clean search text - remove year if exclude_year is True
    clean_search_text = search_text
    if exclude_year:
        import re

        # Remove 4-digit years from search text
        clean_search_text = re.sub(r"\d{4}", "", search_text).strip()
        print_normal(f"      Cleaned search text (year removed): '{clean_search_text}'")

    # Search through the DataFrame
    for row_idx in range(start_row, len(df)):
        row = df.iloc[row_idx]
        for col_idx, cell_value in enumerate(row):
            if pd.notna(cell_value):
                cell_str = str(cell_value).strip()

                # Clean the cell value if exclude_year is True
                if exclude_year:
                    cell_clean = re.sub(r"\d{4}", "", cell_str).strip()
                else:
                    cell_clean = cell_str

                # Check if the cleaned cell contains the cleaned search text
                if clean_search_text in cell_clean:
                    print_success(
                        f"      Found text at row {row_idx}, col {col_idx}: '{cell_str}'"
                    )
                    return row_idx

    print_warning(f"      Text pattern '{search_text}' not found")
    return -1


def extract_concatenated_tables(df, concatenate_config, custom_headers=None):
    """
    Extract two tables and concatenate them vertically.

    Args:
        df: pandas DataFrame (Excel sheet)
        concatenate_config: Configuration for table concatenation
        custom_headers: Optional custom headers to apply

    Returns:
        pandas DataFrame: Concatenated table or None if extraction fails
    """
    try:
        first_config = concatenate_config.get("first_table", {})
        second_config = concatenate_config.get("second_table", {})

        if not first_config or not second_config:
            print_error("      Missing first_table or second_table configuration")
            return None

        # Extract first table
        first_table = None
        if "start_row" in first_config:
            start_row = first_config["start_row"]
            select_columns = first_config.get("select_columns", [])

            print_normal(f"      Extracting first table from row {start_row}")
            first_table = extract_no_title_tables_dynamic_headers(df, start_row)

            if first_table is not None and select_columns:
                # Select only specified columns
                available_cols = [
                    col for col in select_columns if col in first_table.columns
                ]
                if available_cols:
                    first_table = first_table[available_cols]
                    print_normal(
                        f"      Selected columns from first table: {available_cols}"
                    )
                else:
                    print_warning(
                        f"      None of the specified columns {select_columns} found in first table"
                    )
                    print_normal(
                        f"      Available columns: {list(first_table.columns)}"
                    )

            # Apply column renaming if specified
            rename_columns = first_config.get("rename_columns", {})
            if rename_columns and first_table is not None:
                first_table = first_table.rename(columns=rename_columns)
                print_normal(f"      Renamed columns in first table: {rename_columns}")

        # Extract second table
        second_table = None
        if "search_title" in second_config:
            search_text = second_config["search_title"]
            exclude_year = second_config.get("exclude_year", True)

            # Find the table by searching for text
            found_row = find_table_by_text_search(df, search_text, exclude_year)
            if found_row >= 0:
                # Apply header offset if specified
                header_offset = second_config.get("header_offset", 0)
                start_row = found_row + header_offset
                print_normal(
                    f"      Found text at row {found_row}, extracting table from row {start_row} (offset: {header_offset})"
                )
                second_table = extract_no_title_tables_dynamic_headers(df, start_row)

                if second_table is not None:
                    select_columns = second_config.get("select_columns", "all")
                    if select_columns != "all" and isinstance(select_columns, list):
                        # Select only specified columns
                        available_cols = [
                            col for col in select_columns if col in second_table.columns
                        ]
                        if available_cols:
                            second_table = second_table[available_cols]
                            print_normal(
                                f"      Selected columns from second table: {available_cols}"
                            )

                    # Apply column renaming if specified
                    rename_columns = second_config.get("rename_columns", {})
                    if rename_columns:
                        second_table = second_table.rename(columns=rename_columns)
                        print_normal(
                            f"      Renamed columns in second table: {rename_columns}"
                        )

        # Check if both tables were extracted successfully
        if first_table is None:
            print_error("      Failed to extract first table")
            return None

        if second_table is None:
            print_error("      Failed to extract second table")
            return None

        print_normal(f"      First table shape: {first_table.shape}")
        print_normal(f"      Second table shape: {second_table.shape}")

        # Concatenate tables vertically
        try:
            # For vertical concatenation, we need to ensure both tables have the same column structure
            # The first table should have its column as the LAST column
            # The second table should have all its columns first, then the first table's column

            # Get the column from the first table (should be only one column)
            first_table_col = list(first_table.columns)[0]

            # Get all columns from the second table
            second_table_cols = list(second_table.columns)

            # Create the final column order: second table columns first, then first table column
            final_columns = second_table_cols + [first_table_col]

            # Create the concatenated table by adding the first table column to the second table
            concatenated_table = second_table.copy()

            # Add the first table column to the second table with empty values
            concatenated_table[first_table_col] = ""

            # Now merge the first table data with the second table data
            # We need to match the rows properly - the first table has cumulative change percentages
            # that should be added to the corresponding validation data rows

            # Get the first table data as a list
            first_table_values = first_table[first_table_col].tolist()

            # Skip the first row if configured to do so
            skip_first_row = first_config.get("skip_first_row", False)
            if skip_first_row and len(first_table_values) > 0:
                first_table_values = first_table_values[1:]
                print_normal(
                    f"      Skipped first row from first table (skip_first_row=True)"
                )

            print_normal(
                f"      Using {len(first_table_values)} values from first table"
            )

            # Add the first table values to the corresponding rows in the second table
            # If the first table has more rows than the second table, we'll add them as separate rows
            for i, value in enumerate(first_table_values):
                if i < len(concatenated_table):
                    # Update existing row with the cumulative change value
                    concatenated_table.iloc[
                        i, concatenated_table.columns.get_loc(first_table_col)
                    ] = value
                else:
                    # Add new row if first table has more rows than second table
                    new_row = {
                        col: "" for col in second_table_cols
                    }  # Empty values for second table columns
                    new_row[first_table_col] = value  # Value from first table

                    # Add the new row to the concatenated table
                    concatenated_table = pd.concat(
                        [concatenated_table, pd.DataFrame([new_row])], ignore_index=True
                    )

            # Ensure column order is correct
            concatenated_table = concatenated_table[final_columns]

            print_success(
                f"      Successfully concatenated tables: {len(concatenated_table)} total rows"
            )
            print_normal(
                f"      Final columns before custom headers: {list(concatenated_table.columns)}"
            )

            # Note: Custom headers will be applied later in the processing flow
            if custom_headers and len(custom_headers) > 0:
                print_normal(
                    f"      Custom headers will be applied later in processing flow"
                )

            return concatenated_table

        except Exception as e:
            print_error(f"      Error concatenating tables: {str(e)}")
            return None

    except Exception as e:
        print_error(f"      Error in extract_concatenated_tables: {str(e)}")
        return None


def rename_table_columns(table, new_headers):
    """
    Rename table columns according to headers config

    This should be called AFTER all data processing (key_values, calculated_columns)
    and BEFORE database export.

    Args:
        table: pandas DataFrame with original column names
        new_headers: list of new column names from config

    Returns:
        DataFrame with renamed columns
    """
    if table is None or len(table) == 0:
        print_warning("Cannot rename columns: table is empty or None")
        return table

    if not new_headers or len(new_headers) == 0:
        print_normal("      No headers provided for renaming, keeping original")
        return table

    original_columns = list(table.columns)
    num_original_cols = len(original_columns)
    num_new_headers = len(new_headers)

    print_normal(
        f"      Renaming columns: {num_original_cols} original -> {num_new_headers} new headers"
    )
    print_normal(f"      Original columns: {original_columns}")
    print_normal(f"      New headers: {new_headers}")

    try:
        if num_new_headers == num_original_cols:
            # Perfect match - rename all columns
            table.columns = new_headers
            print_success(f"      Successfully renamed all {num_original_cols} columns")

        elif num_new_headers < num_original_cols:
            # More columns than headers - rename only the first N columns
            new_column_names = new_headers + original_columns[num_new_headers:]
            table.columns = new_column_names
            print_normal(
                f"      Renamed first {num_new_headers} columns, kept {num_original_cols - num_new_headers} original names"
            )

        else:
            # More headers than columns - add missing columns and use all headers
            additional_headers = new_headers[num_original_cols:]
            print_normal(
                f"      Adding {len(additional_headers)} missing columns: {additional_headers}"
            )

            # Add missing columns with empty values (only if they don't already exist)
            for extra_header in additional_headers:
                if extra_header not in table.columns:
                    table[extra_header] = [""] * len(table)
                    print_normal(
                        f"      Added column '{extra_header}' with {len(table)} empty strings"
                    )
                else:
                    print_normal(
                        f"      Column '{extra_header}' already exists, skipping"
                    )

            # Now apply all headers - ensure we have the right number of columns
            if len(table.columns) == len(new_headers):
                table.columns = new_headers
                print_normal(f"      Applied all {num_new_headers} headers")
            else:
                print_error(
                    f"      Column count mismatch: table has {len(table.columns)} columns, but {len(new_headers)} headers provided"
                )
                # Use only the first N headers that match the column count
                table.columns = new_headers[: len(table.columns)]
                print_normal(f"      Applied first {len(table.columns)} headers")

        print_normal(f"      Final columns: {list(table.columns)}")
        return table

    except Exception as e:
        print_error(f"      Error renaming columns: {str(e)}")
        print_normal("      Keeping original column names")
        return table


def extract_multi_concatenated_tables(
    all_sheets_data, multi_concatenate_config, custom_headers=None, key_values=None
):
    """
    Extract and concatenate multiple tables from multiple sheets cumulatively.

    This function processes all sheets and tables in sequence, joining each new table
    to the already combined result using the month column as the primary key.

    Flow: sheet1 -> export table1 -> export table2 -> join with table1 ->
          export table3 -> join with table1+2 -> export table4 -> join with table1+2+3 ->
          sheet2 -> export table1 -> join with table1+2+3+4 from sheet1 ->
          export table2 -> join with table1+2+3+4 from sheet1 and table1 from sheet2

    Args:
        all_sheets_data: Dictionary mapping sheet names to their DataFrames
        multi_concatenate_config: Configuration for multi-concatenate processing
        custom_headers: Custom headers to apply
        key_values: Dictionary of key values extracted from sheets (e.g., year from cell G2)

    Returns:
        Combined DataFrame with all tables joined horizontally
    """
    try:
        sheets_config = multi_concatenate_config.get("sheets", [])

        if not sheets_config:
            print_error(
                "      No sheets configuration found in multi_concatenate_config"
            )
            return None

        combined_table = None
        table_counter = 0

        print_normal(f"      Processing {len(sheets_config)} sheet(s) cumulatively")

        # Process each sheet in order
        for sheet_config in sheets_config:
            sheet_name = sheet_config.get("sheet_name")
            tables_config = sheet_config.get("tables", [])

            print_normal(f"      Processing sheet: {sheet_name}")
            print_normal(f"      Found {len(tables_config)} table(s) in sheet")

            # Get the DataFrame for this sheet
            if sheet_name not in all_sheets_data:
                print_error(f"      Sheet '{sheet_name}' not found in provided data")
                continue

            df = all_sheets_data[sheet_name]

            # Process each table in the sheet
            for table_config in tables_config:
                table_name = table_config.get("table_name", "unknown")
                table_counter += 1
                print_normal(f"        Processing table {table_counter}: {table_name}")

                # Extract table based on configuration
                table = None

                if "start_row" in table_config:
                    # First table in sheet - use start_row
                    start_row = table_config["start_row"]
                    print_normal(f"        Extracting table from start_row {start_row}")
                    table = extract_no_title_tables_dynamic_headers(df, start_row)

                elif "search_title" in table_config:
                    # Subsequent tables - search for title
                    search_text = table_config["search_title"]
                    exclude_year = table_config.get("exclude_year", True)

                    # Find the table by searching for text
                    found_row = find_table_by_text_search(df, search_text, exclude_year)
                    if found_row >= 0:
                        # Apply header offset if specified
                        header_offset = table_config.get("header_offset", 0)
                        start_row = found_row + header_offset
                        print_normal(
                            f"        Found text at row {found_row}, extracting table from row {start_row} (offset: {header_offset})"
                        )
                        table = extract_no_title_tables_dynamic_headers(df, start_row)
                    else:
                        print_warning(
                            f"        Could not find text pattern: {search_text}"
                        )
                        continue

                if table is None:
                    print_warning(f"        Failed to extract table: {table_name}")
                    continue

                print_normal(f"        Extracted table shape: {table.shape}")
                print_normal(f"        Extracted table columns: {list(table.columns)}")

                # Apply column selection if specified
                select_columns = table_config.get("select_columns", "all")
                if select_columns != "all" and isinstance(select_columns, list):
                    available_cols = [
                        col for col in select_columns if col in table.columns
                    ]
                    if available_cols:
                        table = table[available_cols]
                        print_normal(f"        Selected columns: {available_cols}")
                    else:
                        print_warning(
                            f"        None of the specified columns {select_columns} found"
                        )
                        print_normal(
                            f"        Available columns: {list(table.columns)}"
                        )

                # Apply column renaming if specified
                rename_columns = table_config.get("rename_columns", {})
                if rename_columns:
                    table = table.rename(columns=rename_columns)
                    print_normal(f"        Renamed columns: {rename_columns}")

                # Join with the combined table
                if combined_table is None:
                    # First table - just use it as the starting point
                    combined_table = table.copy()
                    print_normal(
                        f"        Starting with table {table_counter}. Shape: {combined_table.shape}"
                    )
                else:
                    # Join with existing combined table
                    print_normal(
                        f"        Joining table {table_counter} with existing combined table..."
                    )
                    print_normal(f"        Table {table_counter} shape: {table.shape}")
                    print_normal(
                        f"        Combined table shape: {combined_table.shape}"
                    )

                    # Check for duplicate columns (excluding the join key)
                    join_key = "חודש"
                    if join_key not in table.columns:
                        print_warning(
                            f"        Join key '{join_key}' not found in table {table_counter}, skipping join"
                        )
                        continue

                    if join_key not in combined_table.columns:
                        print_warning(
                            f"        Join key '{join_key}' not found in combined table, skipping join"
                        )
                        continue

                    # Find duplicate columns (excluding the join key)
                    duplicate_cols = []
                    for col in table.columns:
                        if col != join_key and col in combined_table.columns:
                            duplicate_cols.append(col)

                    # Rename duplicate columns in the new table
                    if duplicate_cols:
                        print_normal(
                            f"        Found duplicate columns: {duplicate_cols}"
                        )
                        rename_dict = {
                            col: f"{col}_table{table_counter}" for col in duplicate_cols
                        }
                        table = table.rename(columns=rename_dict)
                        print_normal(
                            f"        Renamed duplicate columns: {rename_dict}"
                        )

                    # Perform horizontal join
                    try:
                        combined_table = pd.merge(
                            combined_table,
                            table,
                            on=join_key,
                            how="outer",
                            suffixes=("", f"_table{table_counter}"),
                        )
                        print_normal(
                            f"        Successfully joined table {table_counter}. New shape: {combined_table.shape}"
                        )
                    except Exception as e:
                        print_error(
                            f"        Failed to join table {table_counter}: {str(e)}"
                        )
                        continue

        if combined_table is None:
            print_error("      No tables were successfully extracted")
            return None

        print_normal(f"      Final combined table shape: {combined_table.shape}")
        print_normal(
            f"      Final combined table columns: {list(combined_table.columns)}"
        )

        # Note: Custom headers will be applied later in the processing flow
        if custom_headers:
            print_normal(
                "      Custom headers will be applied later in processing flow"
            )

        return combined_table

    except Exception as e:
        print_error(f"      Error in extract_multi_concatenated_tables: {str(e)}")
        return None
