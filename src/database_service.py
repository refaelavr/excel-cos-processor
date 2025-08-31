"""
Database Service using psycopg2
Handles database operations with batch processing for large datasets
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Union, Any

# Import from same directory (src/)
from logger import print_success, print_error, print_warning, print_normal
from config_manager import get_database_config


class DatabaseService:
    """
    Database service using psycopg2 for batch processing
    """

    def __init__(self, db_config: Dict):
        self.db_config = db_config

    def get_connection(self):
        """Get direct psycopg2 connection"""
        try:
            conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                sslmode=self.db_config["sslmode"],
                sslrootcert=self.db_config["sslrootcert"],
            )
            return conn
        except Exception as e:
            print_error(f"Database connection error: {str(e)}")
            return None

    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            conn = self.get_connection()
            if conn is None:
                return False

            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print_success("Database connection successful")
            print_normal(f"PostgreSQL version: {version}")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print_error(f"Database connection test failed: {str(e)}")
            return False

    def _convert_single_value(self, value):
        """Convert a single value to Python native type"""
        if pd.isna(value):
            return None
        elif isinstance(value, (np.int64, np.int32, np.int16, np.int8)):
            return int(value)
        elif isinstance(value, (np.float64, np.float32)):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d")
        elif hasattr(value, "item"):  # numpy scalar
            return value.item()
        else:
            return value

    def _convert_numpy_types(self, df):
        """Convert numpy types to Python native types"""
        df_clean = df.copy()

        # Convert each column
        for col in df_clean.columns:
            # Apply conversion to each value in the column
            df_clean[col] = df_clean[col].apply(self._convert_single_value)

        return df_clean

    def _convert_date_columns(self, df):
        """Convert date columns to PostgreSQL compatible format"""
        df_processed = df.copy()

        for col in df_processed.columns:
            if df_processed[col].dtype == "object":
                # Sample some values to check if they're dates
                sample_values = df_processed[col].dropna().head(10)
                date_like_count = 0

                for val in sample_values:
                    str_val = str(val).strip()
                    if len(str_val) == 10 and str_val.count("/") == 2:
                        try:
                            pd.to_datetime(str_val, dayfirst=True, errors="raise")
                            date_like_count += 1
                        except:
                            pass

                # If most values look like dates, convert the column
                if date_like_count > len(sample_values) * 0.5:
                    try:
                        parsed_dates = pd.to_datetime(
                            df_processed[col], dayfirst=True, errors="coerce"
                        )
                        df_processed[col] = parsed_dates.dt.strftime("%Y-%m-%d")
                        df_processed[col] = df_processed[col].replace("NaT", None)
                        print_normal(
                            f"Converted date column '{col}' to PostgreSQL format"
                        )
                    except Exception as e:
                        print_warning(f"Date conversion failed for '{col}': {str(e)}")

        return df_processed

    def _get_postgres_type(self, pandas_dtype, sample_values=None):
        """Map pandas dtype to PostgreSQL data type"""
        dtype_str = str(pandas_dtype).lower()

        if "int" in dtype_str:
            return "INTEGER"
        elif "float" in dtype_str:
            return "DECIMAL(10,2)"
        elif "bool" in dtype_str:
            return "BOOLEAN"
        elif "datetime" in dtype_str:
            return "DATE"
        else:
            # Check if looks like dates for object types
            if sample_values:
                date_like_count = sum(
                    1
                    for val in sample_values[:10]
                    if val and len(str(val)) == 10 and str(val).count("/") == 2
                )
                if date_like_count > len(sample_values[:10]) * 0.5:
                    return "DATE"

                # Determine VARCHAR length
                max_len = max(
                    (len(str(val)) for val in sample_values if pd.notna(val)),
                    default=100,
                )
                # Use a more generous buffer and higher minimum
                varchar_len = min(max(max_len + 100, 200), 1000)
                return f"VARCHAR({varchar_len})"

            return "VARCHAR(500)"

    def _sanitize_table_name(self, table_name):
        """Convert table name to valid PostgreSQL table name"""
        import re

        sanitized = re.sub(r"[^\w\s-]", "_", table_name)
        sanitized = re.sub(r"[\s-]+", "_", sanitized)
        sanitized = re.sub(r"_+", "_", sanitized)
        sanitized = sanitized.strip("_").lower()
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == "_"):
            sanitized = "table_" + sanitized
        return sanitized if sanitized else "unnamed_table"

    def _create_table_from_dataframe(self, conn, table_name, df):
        """Create table based on DataFrame schema"""
        try:
            cursor = conn.cursor()

            # Generate column definitions
            column_defs = []
            for col in df.columns:
                col_name = col.lower().replace(" ", "_").replace("-", "_")
                sample_values = df[col].dropna().head(100).tolist()
                pg_type = self._get_postgres_type(df[col].dtype, sample_values)
                column_defs.append(f'"{col_name}" {pg_type}')

            # Create table SQL
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {', '.join(column_defs)}
            )
            """

            cursor.execute(create_sql)
            conn.commit()
            cursor.close()
            print_normal(f"Created/verified table '{table_name}'")
            return True

        except Exception as e:
            print_error(f"Error creating table '{table_name}': {str(e)}")
            conn.rollback()
            return False

    def bulk_upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        skip_empty_updates: bool = False,
        batch_size: int = 1000,
    ) -> tuple[bool, str]:
        """
        Bulk upsert using psycopg2 with batching for large datasets

        Returns:
            tuple: (success: bool, error_message: str)
        """
        if df is None or len(df) == 0:
            print_warning(f"No data to upsert for table {table_name}")
            return True, ""

        try:
            print_normal(f"Starting bulk upsert: {len(df)} rows to '{table_name}'")

            conn = self.get_connection()
            if conn is None:
                return False, "Failed to get database connection"

            # Prepare data
            df_clean = self._convert_date_columns(df.copy())
            df_clean = self._convert_numpy_types(df_clean)

            # Normalize column names
            df_clean.columns = [
                col.lower().replace(" ", "_").replace("-", "_")
                for col in df_clean.columns
            ]

            # Create table if not exists
            if not self._create_table_from_dataframe(conn, table_name, df_clean):
                conn.close()
                return False, f"Failed to create table '{table_name}'"

            if skip_empty_updates:
                success, error_msg = self._bulk_upsert_merge_mode(
                    conn, df_clean, table_name, primary_keys, batch_size
                )
            else:
                success, error_msg = self._bulk_upsert_standard_mode(
                    conn, df_clean, table_name, primary_keys, batch_size
                )

            conn.close()
            return success, error_msg

        except Exception as e:
            error_msg = f"Bulk upsert failed for table {table_name}: {str(e)}"
            if hasattr(e, "pgcode"):
                error_msg += f" (PostgreSQL code: {e.pgcode})"
            if hasattr(e, "pgerror"):
                error_msg += f" (PostgreSQL error: {e.pgerror})"
            print_error(error_msg)
            return False, error_msg

    def _bulk_upsert_standard_mode(
        self, conn, df, table_name, primary_keys, batch_size
    ):
        """Standard mode using execute_values for performance"""
        try:
            cursor = conn.cursor()

            # Normalize primary key names
            pk_normalized = [
                pk.lower().replace(" ", "_").replace("-", "_") for pk in primary_keys
            ]

            # Get column mapping
            cursor.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name,),
            )

            db_columns = {row[0]: row[1] for row in cursor.fetchall()}

            # Map DataFrame columns to database columns
            df_cols_mapped = {}
            for df_col in df.columns:
                db_col_name = df_col.lower().replace(" ", "_").replace("-", "_")
                if db_col_name in db_columns:
                    df_cols_mapped[df_col] = db_col_name

            if not df_cols_mapped:
                error_msg = f"No matching columns found for table '{table_name}'"
                print_error(error_msg)
                return False, error_msg

            # Validate primary keys
            valid_pk_cols = []
            for pk_col in pk_normalized:
                if pk_col in df_cols_mapped.values():
                    valid_pk_cols.append(pk_col)

            if not valid_pk_cols:
                error_msg = "No valid primary key columns found"
                print_error(error_msg)
                return False, error_msg

            print_normal(f"Using UPSERT with primary keys: {valid_pk_cols}")

            # Prepare data for insertion
            insert_columns = list(df_cols_mapped.values())
            column_str = ", ".join(f'"{col}"' for col in insert_columns)

            # Process in batches for large datasets
            total_rows = len(df)
            rows_processed = 0

            for i in range(0, total_rows, batch_size):
                chunk = df.iloc[i : i + batch_size]

                # Convert chunk rows to tuples
                values = []
                for _, row in chunk.iterrows():
                    row_values = []
                    for df_col, db_col in df_cols_mapped.items():
                        value = row[df_col]
                        if pd.isna(value):
                            row_values.append(None)
                        elif isinstance(value, str) and value.strip() == "":
                            row_values.append(None)
                        else:
                            row_values.append(value)
                    values.append(tuple(row_values))

                # Create UPSERT query
                placeholders = f"({', '.join(['%s'] * len(insert_columns))})"
                conflict_columns = ", ".join(f'"{col}"' for col in valid_pk_cols)

                # UPDATE SET clause for non-PK columns
                update_columns = [
                    col for col in insert_columns if col not in valid_pk_cols
                ]

                if update_columns:
                    update_set = ", ".join(
                        f'"{col}" = EXCLUDED."{col}"' for col in update_columns
                    )
                    insert_query = f"""
                    INSERT INTO "{table_name}" ({column_str}) 
                    VALUES %s
                    ON CONFLICT ({conflict_columns}) 
                    DO UPDATE SET {update_set}
                    """
                else:
                    insert_query = f"""
                    INSERT INTO "{table_name}" ({column_str}) 
                    VALUES %s
                    ON CONFLICT ({conflict_columns}) 
                    DO NOTHING
                    """

                # Use execute_values for performance
                execute_values(
                    cursor,
                    insert_query,
                    values,
                    template=placeholders,
                    page_size=batch_size,
                )

                rows_processed += len(chunk)
                print_normal(f"Processed {rows_processed}/{total_rows} rows")

            conn.commit()
            cursor.close()
            print_success(
                f"Standard mode completed: {rows_processed} rows in {table_name}"
            )
            return True, ""

        except Exception as e:
            error_details = (
                f"Standard bulk upsert failed for table '{table_name}': {str(e)}"
            )
            if hasattr(e, "pgcode"):
                error_details += f" (PostgreSQL code: {e.pgcode})"
            if hasattr(e, "pgerror"):
                error_details += f" (PostgreSQL error: {e.pgerror})"
            print_error(error_details)

            # If it's a VARCHAR length error, provide more detailed information
            if "value too long for type character varying" in str(e):
                print_error(
                    f"Column length issue detected. You may need to alter the table columns to VARCHAR(500)"
                )
                print_error(
                    f"Example: ALTER TABLE {table_name} ALTER COLUMN column_name TYPE VARCHAR(500);"
                )
                print_error(
                    f"Or drop and recreate the table to use new column definitions."
                )

            if conn:
                conn.rollback()
            return False, error_details

    def _bulk_upsert_merge_mode(self, conn, df, table_name, primary_keys, batch_size):
        """Merge mode with batching"""
        try:
            cursor = conn.cursor()

            # Normalize primary key names
            pk_normalized = [
                pk.lower().replace(" ", "_").replace("-", "_") for pk in primary_keys
            ]

            # Get column mapping
            cursor.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name,),
            )

            db_columns = {row[0]: row[1] for row in cursor.fetchall()}

            # Map DataFrame columns to database columns
            df_cols_mapped = {}
            for df_col in df.columns:
                db_col_name = df_col.lower().replace(" ", "_").replace("-", "_")
                if db_col_name in db_columns:
                    df_cols_mapped[df_col] = db_col_name

            all_db_columns = list(df_cols_mapped.values())
            rows_affected = 0
            total_rows = len(df)

            print_normal(f"Starting merge mode for {total_rows} rows")

            # Process in smaller batches for merge mode
            for i in range(0, total_rows, min(batch_size, 100)):
                batch_df = df.iloc[i : i + min(batch_size, 100)]

                for idx, (_, row) in enumerate(batch_df.iterrows()):
                    # Convert row to dictionary with native Python types
                    row_dict = {}
                    for df_col, db_col in df_cols_mapped.items():
                        value = row[df_col]
                        row_dict[db_col] = self._convert_single_value(value)

                    # Build primary key values
                    pk_values = [row_dict[pk] for pk in pk_normalized if pk in row_dict]

                    if len(pk_values) != len(pk_normalized):
                        continue

                    # Check if record exists
                    pk_condition = " AND ".join(f'"{pk}" = %s' for pk in pk_normalized)

                    # Get existing row data
                    select_columns = ", ".join(f'"{col}"' for col in all_db_columns)
                    select_query = f'SELECT {select_columns} FROM "{table_name}" WHERE {pk_condition}'

                    cursor.execute(select_query, pk_values)
                    existing_row = cursor.fetchone()

                    if existing_row:
                        # UPDATE: Merge existing with new values
                        existing_values = dict(zip(all_db_columns, existing_row))

                        # Build merged values
                        merged_values = {}
                        for db_col in all_db_columns:
                            if db_col in pk_normalized:
                                merged_values[db_col] = row_dict[db_col]
                            else:
                                new_val = row_dict.get(db_col)
                                existing_val = existing_values.get(db_col)

                                # Use new value if not empty, otherwise keep existing
                                if new_val is not None and (
                                    not isinstance(new_val, str)
                                    or new_val.strip() != ""
                                ):
                                    merged_values[db_col] = new_val
                                else:
                                    merged_values[db_col] = existing_val

                        # Update record
                        update_columns = [
                            col for col in all_db_columns if col not in pk_normalized
                        ]
                        if update_columns:
                            update_values = [
                                merged_values[col] for col in update_columns
                            ] + pk_values
                            update_set = ", ".join(
                                f'"{col}" = %s' for col in update_columns
                            )
                            update_query = f'UPDATE "{table_name}" SET {update_set} WHERE {pk_condition}'

                            cursor.execute(update_query, update_values)
                            rows_affected += cursor.rowcount

                    else:
                        # INSERT: New record with only non-empty values
                        insert_data = {}

                        # Always include primary keys
                        for pk in pk_normalized:
                            if pk in row_dict:
                                insert_data[pk] = row_dict[pk]

                        # Add non-empty non-PK values
                        for db_col in all_db_columns:
                            if db_col not in pk_normalized:
                                val = row_dict.get(db_col)
                                if val is not None and (
                                    not isinstance(val, str) or val.strip() != ""
                                ):
                                    insert_data[db_col] = val

                        if insert_data:
                            cols = list(insert_data.keys())
                            values = list(insert_data.values())
                            placeholders = ", ".join(["%s"] * len(cols))
                            cols_clause = ", ".join(f'"{col}"' for col in cols)

                            insert_query = f'INSERT INTO "{table_name}" ({cols_clause}) VALUES ({placeholders})'
                            cursor.execute(insert_query, values)
                            rows_affected += cursor.rowcount

                processed = i + len(batch_df)
                if processed % 50 == 0:  # Print progress every 50 rows
                    print_normal(f"Merge mode processed {processed}/{total_rows} rows")

            conn.commit()
            cursor.close()
            print_success(
                f"Merge mode completed: {rows_affected} rows affected in {table_name}"
            )
            return True, ""

        except Exception as e:
            error_details = (
                f"Merge mode bulk upsert failed for table '{table_name}': {str(e)}"
            )
            if hasattr(e, "pgcode"):
                error_details += f" (PostgreSQL code: {e.pgcode})"
            if hasattr(e, "pgerror"):
                error_details += f" (PostgreSQL error: {e.pgerror})"
            print_error(error_details)
            if conn:
                conn.rollback()
            return False, error_details

    def export_table(
        self,
        df: pd.DataFrame,
        table_title: str,
        primary_keys: List[str],
        skip_empty_updates: bool = False,
        explicit_table_name: Optional[str] = None,
    ) -> tuple[bool, str]:
        """
        Main export function

        Returns:
            tuple: (success: bool, error_message: str)
        """
        if df is None or len(df) == 0:
            error_msg = f"Cannot export {table_title}: table is empty or None"
            print_warning(error_msg)
            return False, error_msg

        if not primary_keys:
            error_msg = f"Cannot export {table_title}: primary_keys is required"
            print_error(error_msg)
            return False, error_msg

        # Use explicit table name or sanitize the title
        db_table_name = (
            explicit_table_name
            if explicit_table_name
            else self._sanitize_table_name(table_title)
        )

        logic_type = "MERGE MODE" if skip_empty_updates else "STANDARD MODE"
        print_normal(
            f"Exporting '{table_title}' to '{db_table_name}' using {logic_type}"
        )

        try:
            success, error_msg = self.bulk_upsert(
                df, db_table_name, primary_keys, skip_empty_updates
            )
            if success:
                return True, ""
            else:
                # bulk_upsert failed and returned detailed error message
                return False, error_msg
        except Exception as e:
            error_msg = f"Export failed for table '{table_title}': {str(e)}"
            if hasattr(e, "pgcode"):
                error_msg += f" (PostgreSQL code: {e.pgcode})"
            if hasattr(e, "pgerror"):
                error_msg += f" (PostgreSQL error: {e.pgerror})"
            return False, error_msg

    def create_file_processing_record(
        self,
        file_name: str,
        cos_key: str,
        job_run_name: str,
        ce_jobrun: str,
        ce_job: str,
        file_size_bytes: Optional[int] = None,
    ) -> bool:
        """
        Create a new processing record when file processing starts

        Returns:
            bool: True if record was created successfully
        """
        try:
            conn = self.get_connection()
            if not conn:
                return False

            cursor = conn.cursor()

            query = """
                INSERT INTO file_processing_status 
                (file_name, cos_key, status, job_run_name, ce_jobrun, ce_job, file_size_bytes, processing_start_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            cursor.execute(
                query,
                (
                    file_name,
                    cos_key,
                    "processing",
                    job_run_name,
                    ce_jobrun,
                    ce_job,
                    file_size_bytes,
                    datetime.now(),
                ),
            )

            record_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()

            print_success(
                f"Created processing record (ID: {record_id}) for file: {file_name}"
            )
            return True

        except Exception as e:
            print_error(f"Error creating processing record for {file_name}: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def update_file_processing_status(
        self,
        file_name: str,
        status: str,
        error_message: Optional[str] = None,
        archive_path: Optional[str] = None,
        log_file_name: Optional[str] = None,
    ) -> bool:
        """
        Update the processing status of a file

        Args:
            file_name: The filename to update
            status: New status (success, failed, archived)
            error_message: Error message if status is failed
            archive_path: Path where file was archived
            log_file_name: Name of the log file

        Returns:
            bool: True if update was successful
        """
        try:
            conn = self.get_connection()
            if not conn:
                return False

            cursor = conn.cursor()

            query = """
                UPDATE file_processing_status 
                SET status = %s, 
                    error_message = %s,
                    archive_path = %s,
                    log_file_name = %s,
                    processing_end_time = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM file_processing_status 
                    WHERE file_name = %s 
                    AND status = 'processing'
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            """

            cursor.execute(
                query,
                (
                    status,
                    error_message,
                    archive_path,
                    log_file_name,
                    datetime.now(),
                    file_name,
                ),
            )

            rows_affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()

            if rows_affected > 0:
                print_success(f"Updated status to '{status}' for file: {file_name}")
                return True
            else:
                print_warning(
                    f"No processing record found to update for file: {file_name}"
                )
                return False

        except Exception as e:
            print_error(f"Error updating processing status for {file_name}: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def get_file_processing_status(self, file_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the current processing status of a file

        Returns:
            Dict with status information or None if not found
        """
        try:
            conn = self.get_connection()
            if not conn:
                return None

            cursor = conn.cursor()

            query = """
                SELECT id, file_name, cos_key, status, error_message, 
                       job_run_name, processing_start_time, processing_end_time,
                       archive_path, log_file_name, created_at, updated_at
                FROM file_processing_status 
                WHERE file_name = %s 
                ORDER BY created_at DESC 
                LIMIT 1
            """

            cursor.execute(query, (file_name,))
            row = cursor.fetchone()

            cursor.close()
            conn.close()

            if row:
                return {
                    "id": row[0],
                    "file_name": row[1],
                    "cos_key": row[2],
                    "status": row[3],
                    "error_message": row[4],
                    "job_run_name": row[5],
                    "processing_start_time": row[6],
                    "processing_end_time": row[7],
                    "archive_path": row[8],
                    "log_file_name": row[9],
                    "created_at": row[10],
                    "updated_at": row[11],
                }
            return None

        except Exception as e:
            print_error(f"Error getting processing status for {file_name}: {str(e)}")
            if conn:
                conn.close()
            return None

    def close(self):
        """Close any open database connections and cleanup resources"""
        # DatabaseService doesn't maintain persistent connections,
        # but this method can be used for any future cleanup needs
        pass

    def __del__(self):
        """Destructor to ensure cleanup when object is garbage collected"""
        self.close()


# Backward compatibility function
def export_to_database(
    table: pd.DataFrame,
    title: str,
    primary_keys: List[str],
    skip_empty_updates: bool = False,
    table_name: Optional[str] = None,
) -> bool:
    """
    Backward compatibility wrapper
    """
    db_config = get_database_config()
    db_service = DatabaseService(db_config)
    return db_service.export_table(
        table, title, primary_keys, skip_empty_updates, table_name
    )


def test_database_service():
    """Test database service functionality"""
    try:
        print("\n" + "=" * 50)
        print("DATABASE SERVICE TEST")
        print("=" * 50)

        # Test configuration loading
        db_config = get_database_config()
        print(f"Database config loaded: {db_config['host']}")

        # Test service initialization
        db_service = DatabaseService(db_config)
        print("Database service initialized")

        # Test connection
        success = db_service.test_connection()
        if success:
            print("Database connection test passed")
        else:
            print("Database connection test failed")

        return success

    except Exception as e:
        print(f"Database service test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_database_service()
