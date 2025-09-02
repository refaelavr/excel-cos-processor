"""
Excel COS Processor - File Configuration Documentation
====================================================

This file contains the configuration for processing different types of Excel files.
The configuration is structured as a JSON-like Python dictionary that defines how
each file type should be processed, what data to extract, and how to transform it.

STRUCTURE OVERVIEW:
------------------
FILE_CONFIG = {
    "Excel File Name": {                    # Exact Excel file name (including extension)
        "Sheet Name": {                     # Exact sheet name in the Excel file
            "key_values": [...],            # Key-value pairs to extract from specific cells
            "tables": [...],                # Tables with headers to extract
            "no_title_tables": [...]        # Tables without headers to extract
        }
    }
}

HOW TO ADD A NEW FILE:
---------------------
1. Add a new entry to FILE_CONFIG with the exact Excel file name as the key
2. For each sheet you want to process, add a sheet configuration
3. Define what data to extract using key_values, tables, and/or no_title_tables
4. Configure database export settings and calculated columns

CONFIGURATION KEYS EXPLANATION:
------------------------------

FILE LEVEL:
- "Excel File Name": The exact name of the Excel file (including .xlsx extension)
  This is used to match incoming files for processing.

SHEET LEVEL:
- "Sheet Name": The exact name of the worksheet in the Excel file
  Each sheet can have its own processing configuration.

KEY_VALUES SECTION:
- "key_values": List of key-value pairs to extract from specific cells
  Each key_value object contains:
  - "title": The name/key for this value (used in database)
  - "row": Row number (0-based index) where the value is located
  - "col": Column number (0-based index) where the value is located
  - "add_to_table": Boolean - whether to include this value in table data
  - "placement": Where to place the value ("last_row", "all_rows", or omitted)
  - "format": Optional date format string (e.g., "%d/%m/%Y")

TABLES SECTION:
- "tables": List of tables that have headers to extract
  Each table object contains:
  - "title": The title/name of the table (used for identification)
  - "add_keys": Boolean - whether to add key_values to this table
  - "export_to_db": Boolean - whether to export this table to database
  - "primary_keys": List of column names that form the primary key
  - "skip_empty_updates": Boolean - skip updates if no data changes
  - "table_name": Database table name where data will be stored
  - "headers": List of column names in the order they appear
  - "calculated_columns": List of calculated/derived columns
  - "col_count": Optional - number of columns if multiple tables are adjacent

NO_TITLE_TABLES SECTION:
- "no_title_tables": List of tables without headers to extract
  Each no_title_table object contains:
  - "title": Internal name for the table
  - "start_row": Row number (0-based) where table data starts
  - "export_to_db": Boolean - whether to export to database
  - "table_name": Database table name
  - "primary_keys": List of primary key column names
  - "add_keys": Boolean - whether to add key_values
  - "columns_to_exclude": List of column names to exclude from processing
  - "headers": List of column names for the table
  - "calculated_columns": List of calculated columns

CALCULATED_COLUMNS SECTION:
- "calculated_columns": List of columns to calculate/derive
  Each calculated_column object contains:
  - "name": Name of the calculated column
  - "type": Type of calculation (see CALCULATED_COLUMN_TYPES below)
  - "formula": Custom formula (for custom_formula type)
  - "description": Description of what this column represents
  - "format": Optional format string (for date types)
  - "placement": Where to place the value ("all_rows" or omitted)

CALCULATED_COLUMN_TYPES:
- "cumulative_average": Running average up to current row
- "cumulative_sum": Running sum up to current row
- "cumulative_count": Count of non-null values up to current row
- "cumulative_max": Maximum value up to current row
- "cumulative_min": Minimum value up to current row
- "rolling_average": Rolling average over N periods
- "rolling_sum": Rolling sum over N periods
- "percent_of_total": Percentage of total for entire column
- "percent_change": Percentage change from previous row
- "custom_formula": Apply custom pandas expression
- "current_date": Apply current date

DATABASE INTEGRATION:
- The system automatically creates database tables based on the configuration
- Primary keys are used for upsert operations (update if exists, insert if not)
- Data types are automatically inferred from Excel data
- Calculated columns are computed and added to the database table

EXAMPLE CONFIGURATION:
---------------------
{
    "My Report.xlsx": {
        "Summary Sheet": {
            "key_values": [
                {
                    "title": "report_date",
                    "row": 1,
                    "col": 2,
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%Y-%m-%d"
                }
            ],
            "tables": [
                {
                    "title": "Monthly Summary",
                    "add_keys": True,
                    "export_to_db": True,
                    "primary_keys": ["report_date", "category"],
                    "table_name": "monthly_summary",
                    "headers": ["category", "value", "percentage"],
                    "calculated_columns": [
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%Y-%m-%d",
                            "placement": "all_rows"
                        }
                    ]
                }
            ]
        }
    }
}
"""

CALCULATED_COLUMN_TYPES = {
    # Cumulative calculations (growing over time)
    "cumulative_average": "Calculate running average up to current row",
    "cumulative_sum": "Calculate running sum up to current row",
    "cumulative_count": "Count non-null values up to current row",
    "cumulative_max": "Maximum value up to current row",
    "cumulative_min": "Minimum value up to current row",
    # Rolling window calculations
    "rolling_average": "Calculate rolling average over N periods",
    "rolling_sum": "Calculate rolling sum over N periods",
    # Percentage calculations
    "percent_of_total": "Calculate percentage of total for entire column",
    "percent_change": "Calculate percentage change from previous row",
    # Custom formulas
    "custom_formula": "Apply custom pandas expression",
    "current_date": "Apply current date",
}

FILE_CONFIG = {
    "ניתוח קנסות VM אקסל": {  # File name
        'בקרת קנסות וק"מ': {  # Sheet name
            "key_values": [
                {
                    "title": "total_rides_according_to_license_as_to_date",
                    "row": 19,
                    "col": 1,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_km_by_license_as_of_date",
                    "row": 19,
                    "col": 4,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_rides_made_by_license_as_of_date",
                    "row": 19,
                    "col": 5,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_kilometers_made_by_license_as_of_date",
                    "row": 19,
                    "col": 10,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_kilometers_of_rides_unlicensed_as_of_date",
                    "row": 24,
                    "col": 10,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_license_rides_non_performance",
                    "row": 24,
                    "col": 1,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_license_kilometers_non_performance",
                    "row": 24,
                    "col": 2,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "current_date",
                    "row": 1,
                    "col": 19,
                    "add_to_table": False,
                    "format": "%d/%m/%Y",
                },  # 0-based index
                {
                    "title": "report_date",
                    "row": 3,
                    "col": 2,
                    "add_to_table": False,
                    "format": "%d/%m/%Y",
                },  # 0-based index
                {
                    "title": "non_performance_rate_as_of_date",
                    "row": 8,
                    "col": 0,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "Non_performance_orderer_include_delays_rate_as_of_date",
                    "row": 8,
                    "col": 4,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "non_performance_of_kilometers_rate_as_of_date",
                    "row": 8,
                    "col": 8,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "non_performance_e_check_before_exceptions",
                    "row": 8,
                    "col": 12,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "service_level_rate_non_performance",
                    "row": 8,
                    "col": 16,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "total_unlicensed_rides_reinforcements_to_date",
                    "row": 24,
                    "col": 5,
                    "add_to_table": True,
                    "placement": "last_row",
                },
                {
                    "title": "אחוז אי ביצוע נסיעות לתקופה - יום האתמול",
                    "row": 12,
                    "col": 0,
                    "add_to_table": False,
                },
                {
                    "title": "אי ביצוע לפי סדרן - יום האתמול",
                    "row": 12,
                    "col": 4,
                    "add_to_table": False,
                },
                {
                    "title": 'אחוז אי ביצוע ק"מ - יום האתמול',
                    "row": 12,
                    "col": 7,
                    "add_to_table": False,
                },
            ],
            "tables": [  # Add col_count value if 2 tables are next to each other
                {
                    "title": 'טבלה מסכמת ק"מ',
                    "add_keys": True,
                    "export_to_db": True,
                    "primary_keys": ["date"],
                    "skip_empty_updates": True,
                    "table_name": "fines_and_mileage_control_vm",
                    "headers": [
                        "date",
                        "day",
                        "rides_according_to_licensing",
                        "rides_made_according_to_license",
                        "non_performance",
                        "non_performance_rate",
                        "non_performance_orderer",
                        "non_performance_orderer_rate",
                        "reinforcements",
                        "total_km",
                        "total_rides_according_to_license_as_to_date",
                        "total_km_by_license_as_of_date",
                        "total_rides_made_by_license_as_of_date",
                        "total_kilometers_made_by_license_as_of_date",
                        "total_kilometers_of_rides_unlicensed_as_of_date",
                        "total_license_rides_non_performance",
                        "total_license_kilometers_non_performance",
                        "non_performance_rate_as_of_date",
                        "non_performance_orderer_include_delays_rate_as_of_date",
                        "non_performance_of_kilometers_rate_as_of_date",
                        "non_performance_e_check_before_exceptions",
                        "service_level_rate_non_performance",
                        "total_unlicensed_rides_reinforcements_to_date",
                    ],
                    "calculated_columns": [
                        {
                            "name": "total_km_as_of_date",
                            "type": "custom_formula",
                            "formula": "(total_kilometers_made_by_license_as_of_date + total_kilometers_of_rides_unlicensed_as_of_date)",
                            "description": "Total kilometers made by license as of date",
                        },
                    ],
                },
                # {"title": 'נסיעות עפ"י רישוי', "add_keys": False, "col_count": 4},
                # {
                #     "title": 'נסיעות שבוצעו עפ"י רישוי',
                #     "add_keys": False,
                #     "col_count": 6,
                # },
                # {
                #     "title": "נסיעות ברישוי ללא זיהוי ביצוע",
                #     "add_keys": False,
                #     "col_count": 4,
                # },
                # {
                #     "title": "נסיעות שבוצעו ואינן מוכרות ברישוי",
                #     "add_keys": False,
                #     "col_count": 6,
                # },
            ],
            "no_title_tables": [],
        },
        "מעקב ביצוע דן חריגות": {
            "key_values": [],
            "tables": [
                {
                    "title": "טבלה מסכמת חריגות",
                    "add_keys": False,
                    "export_to_db": True,
                    "table_name": "performance_monitoring_and_exceptions",
                    "primary_keys": ["date"],
                    "skip_empty_updates": False,
                    "headers": [
                        # Order should match Excel columns
                        "date",
                        "day",
                        "late_11_20",
                        "early_2_10",
                        "non_perform_subsequent_trip",
                        "late_20_59",
                        "non_perform_early_10",
                        "non_perform",
                        "non_perform_arrival_only",
                        "total_non_perform",
                    ],
                },
                # {"title": "טבלה מסכמת חריגות לפי אשכול", "add_keys": False},
            ],
            "no_title_tables": [],
        },
        # "ציון מפעיל": {
        #     "key_values": [],
        #     "tables": [
        #         {"title": "ציון מפעיל", "add_keys": False},
        #         {"title": "ציון מפעיל לפי אשכול", "add_keys": False},
        #         {"title": "פירוט חריגות  ברמת אשכול", "add_keys": False},
        #     ],
        #     "no_title_tables": [],
        # },
        "מעקב ביצוע דן קנסות": {
            "key_values": [],
            "tables": [
                {
                    "title": "טבלה מסכמת קנסות",
                    "add_keys": False,
                    "add_data_date": True,
                    "col_count": 4,
                    "start_from_end": True,
                    "export_to_db": True,
                    "table_name": "summarizing_fines",
                    "primary_keys": ["date"],
                    "skip_empty_updates": False,
                    "headers": [
                        "non_performance_fines",
                        "late_fines",
                        "early_fines",
                        "total_fines",
                    ],
                },
            ],
            "no_title_tables": [
                {
                    "title": "summarizing_fines_per_area",
                    "flat_by": "date",
                    "export_to_db": True,
                    "table_name": "summarizing_fines_per_area",
                    "primary_keys": ["date"],
                    "start_row": 14,
                    "flat_table": True,
                    "headers": [
                        "date",
                        "fines_in_intercity",  # קנסות באזור_בינעירוני
                        "rides_amount_in_intercity",  # כמות נסיעות באזור_בינעירוני
                        "km_in_intercity",  # ביצוע ק"מ באזור_בינעירוני
                        "fines_in_bat_yam",  # קנסות באזור_בת-ים
                        "rides_amount_in_bat_yam",  # כמות נסיעות באזור_בת-ים
                        "km_in_bat_yam",  # ביצוע ק"מ באזור_בת-ים
                        "fines_in_south",  # קנסות באזור_דרומי
                        "rides_amount_in_south",  # כמות נסיעות באזור_דרומי
                        "km_in_south",  # ביצוע ק"מ באזור_דרומי
                        "fines_in_center",  # קנסות באזור_מרכז
                        "rides_amount_in_center",  # כמות נסיעות באזור_מרכז
                        "km_in_center",  # ביצוע ק"מ באזור_מרכז
                        "fines_in_petah_tikva",  # קנסות באזור_פתח-תקווה
                        "rides_amount_in_petah_tikva",  # כמות נסיעות באזור_פתח-תקווה
                        "km_in_petah_tikva",  # ביצוע ק"מ באזור_פתח-תקווה
                        "fines_in_housing",  # קנסות באזור_שיכון
                        "rides_amount_in_housing",  # כמות נסיעות באזור_שיכון
                        "km_in_housing",  # ביצוע ק"מ באזור_שיכון
                    ],
                }
            ],
        },
    },
    "סטטוס אי ביצוע בזמן אמת - YIT - נתונים להיום": {
        "Sheet1": {  # Sheet name
            "key_values": [
                {
                    "title": "date",
                    "row": 1,
                    "col": 6,
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%d/%m/%Y",
                }
            ],
            "tables": [],  # Add col_count value if 2 tables are next to each other
            "no_title_tables": [
                {
                    "title": "Real-Time Task Status – YIT",
                    "export_to_db": True,
                    "add_keys": True,
                    "start_row": 6,
                    "primary_keys": ["date", "zone"],
                    # "flat_table": True,
                    # "flat_by": "day",
                    # "merge_with": "סטטוס אי ביצוע בזמן אמת - YIT - משימות פתוחות למחר",
                    # "merge_on": "אזור",
                    "headers": [
                        "zone",
                        "task_count",
                        "open_task_count",
                        "canceled_task_count",
                        "failure_count",
                        "failure_percentage",
                        "maintenance_failure_count",
                        "maintenance_failure_percent",
                    ],
                    "calculated_columns": [
                        {
                            "name": "open_task_percentage",
                            "type": "custom_formula",
                            "formula": "(open_task_count / task_count)",
                            "description": "Percentage of open tasks",
                        },
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%d/%m/%Y",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
    },
    "סטטוס אי ביצוע בזמן אמת - YIT - משימות פתוחות למחר": {
        "Sheet1": {  # Sheet name
            "key_values": [
                {
                    "title": "date",
                    "row": 1,
                    "col": 4,
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%d/%m/%Y",
                }
            ],
            "tables": [],  # Add col_count value if 2 tables are next to each other
            "no_title_tables": [
                {
                    "title": "Real-Time Task Status – YIT",
                    "start_row": 6,
                    # "flat_table": True,
                    # "merge_with": "סטטוס אי ביצוע בזמן אמת - YIT - נתונים להיום",
                    # "merge_on": "אזור",
                    "headers": [
                        "zone",
                        "task_count",
                        "open_task_count",
                        "open_task_percentage",
                        "canceled_task_count",
                        "failure_count",
                        "failure_percentage",
                        "maintenance_failure_count",
                        "maintenance_failure_percent",
                    ],
                    "add_keys": True,
                    "export_to_db": True,
                    "fill_na": True,
                    "primary_keys": ["date", "zone"],
                    "calculated_columns": [
                        {
                            "name": "open_task_percentage",
                            "type": "custom_formula",
                            "formula": "(open_task_count / task_count)",
                            "description": "Percentage of open tasks",
                        },
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%d/%m/%Y",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
    },
    "מהירות מסחרית בשעות השיא יומי": {
        "Sheet1": {  # Sheet name
            "key_values": [
                {
                    "title": 'סה"כ ק"מ לתקופה',
                    "row": 5,
                    "col": 0,
                    "add_to_table": True,
                },  # 0-based index
                {
                    "title": 'סה"כ שעות נסיעה לתקופה',
                    "row": 5,
                    "col": 3,
                    "add_to_table": True,
                },  # 0-based index
                {
                    "title": "מהירות מסחרית לתקופה",
                    "row": 5,
                    "col": 6,
                    "add_to_table": True,
                },  # 0-based index
            ],
            "tables": [],  # Add col_count value if 2 tables are next to each other
            "no_title_tables": [
                {
                    "title": "commercial_speed_during_rush_hour",
                    "add_keys": True,
                    "start_row": 9,
                    "flat_table": False,
                    # "headers": ["zone"],
                }
            ],
        },
    },
    "מהירות מסחרית הסכם משרד התחבורה": {
        'מהירות מסחרית הסכם משה"ת': {  # Sheet name
            "key_values": [],
            "tables": [],  # Add col_count value if 2 tables are next to each other
            "no_title_tables": [
                {
                    "title": "commercial_speed_ministry_of_transportation_agreement",
                    "start_row": 13,
                    "headers": ["date", "commercial_speed"],
                    "export_to_db": True,
                    "primary_keys": ["date"],
                    "calculated_columns": [
                        {
                            "name": "average_up_to_date",
                            "type": "cumulative_average",
                            "source_column": "commercial_speed",
                            "description": "Running average of commercial speed up to current date",
                        },
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%d/%m/%Y",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
        "מהירות מסחרית ברמת קו": {
            "key_values": [],
            "tables": [],
            "no_title_tables": [
                {
                    "title": "commercial_speed_by_line",
                    "start_row": 5,  # Row 6 in Excel (0-based),
                    "export_to_db": True,
                    "table_name": "commercial_speed_by_line",
                    "primary_keys": [
                        "date",
                        "line",
                        "direction",
                        "slot",
                        "alternative",
                    ],
                    "add_keys": False,
                    "headers": [
                        "date",
                        "day",
                        "line",
                        "direction",
                        "alternative",
                        "slot",
                        "speed",
                    ],
                }
            ],
        },
    },
    "תחקור שעות נטו ברוטו": {
        "סיכומי": {  # Summary sheet - goes to net_and_gross_hours_summary table
            "key_values": [
                {
                    "title": "month",
                    "row": 1,  # Row 2 in Excel (0-based)
                    "col": 9,  # Column J (0-based)
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%Y-%m-01",  # Convert Hebrew month to first day of month
                }
            ],
            "tables": [],
            "no_title_tables": [
                {
                    "title": "net_and_gross_hours_summary",
                    "start_row": 15,  # Row 16 in Excel (0-based)
                    "export_to_db": True,
                    "table_name": "net_and_gross_hours_summary",
                    "primary_keys": ["month", "id_num"],
                    "add_keys": True,
                    "columns_to_exclude": ["שם נהג", "מספר אישי"],  # Drop columns
                    "headers": [
                        "id_num",
                        "zone",
                        "billable_hours",
                        "avg_billable_daily_hours",
                        "driving_hours_net",
                        "average_daily",
                        "working_days",
                        "gross_net_hours_ratio",
                    ],
                    "calculated_columns": [
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%Y-%m-%d",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
        "ימי חול בלבד": {  # Weekdays only
            "key_values": [
                {
                    "title": "month",
                    "row": 1,
                    "col": 9,
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%Y-%m-01",
                }
            ],
            "tables": [],
            "no_title_tables": [
                {
                    "title": "net_and_gross_hours_weekdays",
                    "start_row": 15,
                    "export_to_db": True,
                    "table_name": "net_and_gross_hours",
                    "primary_keys": ["month", "id_num", "day_type"],
                    "add_keys": True,
                    "columns_to_exclude": ["שם נהג", "מספר אישי"],  # Drop columns
                    "headers": [
                        "id_num",
                        "zone",
                        "billable_hours",
                        "avg_billable_daily_hours",
                        "driving_hours_net",
                        "average_daily",
                        "working_days",
                        "gross_net_hours_ratio",
                    ],
                    "calculated_columns": [
                        {
                            "name": "day_type",
                            "type": "custom_formula",
                            "formula": "'חול'",
                            "description": "Day type identifier for weekdays",
                        },
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%Y-%m-%d",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
        "ימי שישי בלבד": {  # Fridays only
            "key_values": [
                {
                    "title": "month",
                    "row": 1,
                    "col": 9,
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%Y-%m-01",
                }
            ],
            "tables": [],
            "no_title_tables": [
                {
                    "title": "net_and_gross_hours_fridays",
                    "start_row": 15,
                    "export_to_db": True,
                    "table_name": "net_and_gross_hours",
                    "primary_keys": ["month", "id_num", "day_type"],
                    "add_keys": True,
                    "columns_to_exclude": ["שם נהג", "מספר אישי"],  # Drop columns
                    "headers": [
                        "id_num",
                        "zone",
                        "billable_hours",
                        "avg_billable_daily_hours",
                        "driving_hours_net",
                        "average_daily",
                        "working_days",
                        "gross_net_hours_ratio",
                    ],
                    "calculated_columns": [
                        {
                            "name": "day_type",
                            "type": "custom_formula",
                            "formula": "'שישי'",
                            "description": "Day type identifier for Fridays",
                        },
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%Y-%m-%d",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
        "ימי שבת בלבד": {  # Saturdays only
            "key_values": [
                {
                    "title": "month",
                    "row": 1,
                    "col": 9,
                    "add_to_table": True,
                    "placement": "all_rows",
                    "format": "%Y-%m-01",
                }
            ],
            "tables": [],
            "no_title_tables": [
                {
                    "title": "net_and_gross_hours_saturdays",
                    "start_row": 15,
                    "export_to_db": True,
                    "table_name": "net_and_gross_hours",
                    "primary_keys": ["month", "id_num", "day_type"],
                    "add_keys": True,
                    "columns_to_exclude": ["שם נהג", "מספר אישי"],  # Drop columns
                    "headers": [
                        "id_num",
                        "zone",
                        "billable_hours",
                        "avg_billable_daily_hours",
                        "driving_hours_net",
                        "average_daily",
                        "working_days",
                        "gross_net_hours_ratio",
                    ],
                    "calculated_columns": [
                        {
                            "name": "day_type",
                            "type": "custom_formula",
                            "formula": "'שבת'",
                            "description": "Day type identifier for Saturdays",
                        },
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%Y-%m-%d",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
    },
    "דוח העדרויות נהגים מסכם": {
        "טבלת פירוט סטטוס נהגים": {  # Main table
            "key_values": [],
            "tables": [],
            "no_title_tables": [
                {
                    "title": "daily_driver_absence",
                    "start_row": 6,  # Row 7 in Excel (0-based)
                    "export_to_db": True,
                    "table_name": "daily_driver_absence",
                    "primary_keys": ["date", "id_num"],
                    "add_keys": True,
                    "columns_to_exclude": [
                        "מספר אישי",
                        "זמן הרצת המודל",
                        "שם פרטי",
                        "שם משפחה",
                        "ספירה",
                    ],  # Drop columns
                    "headers": [
                        "date",
                        "id_num",
                        "sector_type",
                        "sector",
                        "zone_id",
                        "zone",
                        "driver_type",
                        "status_type",
                        "status",
                        "absence_type",
                        "working_today",
                        "relative_remark",
                        "assigned",
                        "missing_and_unassigned",
                        "limited_in_working_hours",
                        "significant",
                    ],
                    "calculated_columns": [
                        {
                            "name": "last_updated",
                            "type": "current_date",
                            "format": "%Y-%m-%d",
                            "placement": "all_rows",
                        },
                    ],
                }
            ],
        },
    },
}
