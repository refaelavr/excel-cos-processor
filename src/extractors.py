import os
import pandas as pd


# Global logger for extractors
_logger = None


def set_logger(logger):
    """Set the logger for extractors functions."""
    global _logger
    _logger = logger


def print_success(msg):
    if _logger:
        _logger.info(f"SUCCESS: {msg}")
    else:
        print(f"SUCCESS: {msg}")


def print_error(msg):
    if _logger:
        _logger.error(msg)
    else:
        print(f"ERROR: {msg}")


def print_warning(msg):
    if _logger:
        _logger.warning(msg)
    else:
        print(f"WARNING: {msg}")


def print_normal(msg):
    if _logger:
        _logger.info(msg)
    else:
        print(msg)


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
            print(f"WARNING: Could not parse Hebrew date format: {hebrew_date_str}")
            return None

        if len(parts) != 2:
            print(f"WARNING: Unexpected Hebrew date format: {hebrew_date_str}")
            return None

        month_part = parts[1].strip()
        year_part = parts[0].strip()

        # Look up month
        month_num = hebrew_months.get(month_part)
        if not month_num:
            print(f"WARNING: Unknown Hebrew month: {month_part}")
            return None

        # Validate and format year
        if not year_part.isdigit() or len(year_part) != 4:
            print(f"WARNING: Invalid year format: {year_part}")
            return None

        result = f"{year_part}-{month_num}-01"
        print(f"      Converted Hebrew date '{hebrew_date_str}' to '{result}'")
        return result

    except Exception as e:
        print(f"WARNING: Error parsing Hebrew date '{hebrew_date_str}': {str(e)}")
        return None


def extract_key_values(df, key_defs):
    """
    Extract key-value pairs from DataFrame based on config.
    key_defs: dict with keys and their location ({'row': x, 'col': y})
    Supports date formatting via 'format' parameter
    Supports Hebrew month parsing for format '%Y-%m-01'
    Returns a dict of {key: value}
    """
    results = {}
    for key in key_defs:
        title = key["title"]
        row = key.get("row")
        col = key.get("col")
        date_format = key.get("format")  # Get date format if specified

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
                                        except:
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
                                formatted_value = value.strftime(date_format)
                                print_normal(
                                    f"      Formatted date '{value}' -> '{formatted_value}' using format '{date_format}'"
                                )
                                value = formatted_value
                            elif pd.notna(value):
                                # Try to convert to datetime first
                                try:
                                    parsed_date = pd.to_datetime(value, dayfirst=True)
                                    formatted_value = parsed_date.strftime(date_format)
                                    print_normal(
                                        f"      Formatted date '{value}' -> '{formatted_value}' using format '{date_format}'"
                                    )
                                    value = formatted_value
                                except:
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

                    # Apply custom headers if provided
                    if custom_headers and len(custom_headers) > 0:
                        # Use custom headers, but only for the number of columns we have
                        num_cols = len(table.columns)
                        print_normal(
                            f"      Table has {num_cols} columns, custom_headers has {len(custom_headers)} headers"
                        )
                        print_normal(f"      Custom headers: {custom_headers}")
                        print_normal(f"      fill_na = {fill_na}")

                        if len(custom_headers) >= num_cols:
                            table.columns = custom_headers[:num_cols]
                            print_normal(
                                f"      Applied custom headers: {custom_headers[:num_cols]}"
                            )
                        else:
                            # If not enough custom headers, use original for remaining columns
                            new_headers = custom_headers + list(
                                header_row[len(custom_headers) : num_cols]
                            )
                            table.columns = new_headers
                            print_normal(
                                f"      Applied partial custom headers: {new_headers}"
                            )

                        # If we have more custom headers than actual columns, add empty columns
                        if len(custom_headers) > num_cols:
                            print_normal(
                                f"      Adding {len(custom_headers) - num_cols} extra columns"
                            )
                            additional_headers = custom_headers[num_cols:]
                            print_normal(
                                f"      Additional headers to add: {additional_headers}"
                            )

                            for extra_header in additional_headers:
                                if fill_na:
                                    table[extra_header] = [0] * len(
                                        table
                                    )  # Fill entire column with zeros
                                    print_normal(
                                        f"      Added empty column '{extra_header}' filled with {len(table)} zeros"
                                    )
                                else:
                                    table[extra_header] = [""] * len(
                                        table
                                    )  # Fill entire column with empty strings
                                    print_normal(
                                        f"      Added empty column '{extra_header}' filled with {len(table)} empty strings"
                                    )

                            print_normal(
                                f"      Table now has {len(table.columns)} columns: {list(table.columns)}"
                            )
                    else:
                        # Use original headers
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
                                except:
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
            except:
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


def apply_calculated_columns(table, calculated_columns_config):
    """
    Apply calculated columns to a table based on configuration.

    Args:
        table: pandas DataFrame to add calculated columns to
        calculated_columns_config: List of calculation definitions from config

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
                result_table[col_name] = result_table[source_col].cumsum()

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
            # More headers than columns - use only the first N headers
            table.columns = new_headers[:num_original_cols]
            print_normal(
                f"      Used first {num_original_cols} headers, ignored {num_new_headers - num_original_cols} extra headers"
            )

        print_normal(f"      Final columns: {list(table.columns)}")
        return table

    except Exception as e:
        print_error(f"      Error renaming columns: {str(e)}")
        print_normal("      Keeping original column names")
        return table
