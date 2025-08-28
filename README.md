# Excel COS Processor

A professional Excel file processor for IBM Cloud Object Storage that processes Excel files and exports data to PostgreSQL databases.

## Overview

The Excel COS Processor is designed to handle Excel file processing in IBM Cloud environments. It supports single file processing with automatic trigger detection, comprehensive error handling, and detailed logging.

## Features

- **Single File Processing**: Processes one file at a time for better control and logging
- **IBM Cloud Integration**: Seamless integration with IBM Cloud Object Storage and Code Engine
- **Database Export**: Exports processed data to PostgreSQL with configurable table structures
- **Error Handling**: Comprehensive error handling with detailed logging
- **File Archival**: Automatic archival of processed files to success/failed folders
- **Status Tracking**: Database tracking of file processing status and results

## Architecture

The application follows a modular service-oriented architecture:

```text
excel_cos_processor/
├── src/
│   ├── services/           # Business logic services
│   │   ├── app_orchestrator.py      # Main orchestrator
│   │   ├── cos_service.py           # Cloud Object Storage operations
│   │   ├── file_processing_service.py # File processing workflow
│   │   ├── archive_service.py       # File archival operations
│   │   ├── trigger_service.py       # Trigger event handling
│   │   └── logging_service.py       # Centralized logging
│   ├── models/             # Data models
│   │   └── processing_result.py     # Processing result data structures
│   ├── utils/              # Utility functions
│   │   ├── file_utils.py           # File operations
│   │   └── environment_utils.py    # Environment handling
│   ├── excel_service.py    # Excel file processing
│   ├── database_service.py # Database operations
│   ├── extractors.py       # Data extraction logic
│   ├── config_manager.py   # Configuration management
│   └── logger.py           # Logging utilities
├── config/                 # Configuration files
│   ├── file_config.py      # File type configurations
│   └── db_config.py        # Database configurations
├── data/                   # Data directories
├── app_cloud.py            # Main entry point
├── requirements.txt        # Python dependencies
└── Dockerfile             # Docker configuration
```

## Services

### AppOrchestrator

Coordinates all services and manages the application workflow.

### COSService

Handles Cloud Object Storage operations including file download, upload, and management.

### FileProcessingService

Orchestrates the file processing workflow from download to database export.

### ArchiveService

Manages file archival operations, moving processed files to appropriate folders.

### TriggerService

Handles IBM Cloud Code Engine trigger events and extracts file information.

### LoggingService

Provides centralized logging with IBM Cloud console visibility.

### ExcelService

Processes Excel files, extracts data, and applies transformations.

### DatabaseService

Manages database connections and operations including table creation and data upserts.

## Usage

### Production Mode

```bash
python app_cloud.py
```

The application automatically detects IBM Cloud Code Engine triggers and processes the triggered file.

### Test Mode

```bash
python app_cloud.py filename.xlsx
```

Processes a specific file from the local `data/input/` directory.

## Environment Variables

### Required for Production

- `ENVIRONMENT=prod`
- `COS_BUCKET_NAME`: IBM Cloud Object Storage bucket name
- `COS_API_KEY`: IBM Cloud API key
- `COS_INSTANCE_ID`: COS instance ID
- `COS_INTERNAL_ENDPOINT`: COS internal endpoint
- `DB_HOST`: PostgreSQL host
- `DB_PORT`: PostgreSQL port
- `DB_NAME`: Database name
- `DB_USER`: Database user
- `DB_PASSWORD`: Database password

### Optional

- `ENVIRONMENT=test`: For local testing mode

## Configuration

The application uses configuration files in the `config/` directory:

### file_config.py

The main configuration file that defines how different Excel files should be processed. This file contains:

- **File Type Definitions**: Exact Excel file names and their processing rules
- **Sheet Configurations**: How each worksheet should be processed
- **Data Extraction Rules**: What cells, tables, and data to extract
- **Database Mappings**: How extracted data maps to database tables
- **Calculated Columns**: Formulas and derived data fields
- **Transformation Rules**: Data cleaning and formatting instructions

#### Configuration Structure

The configuration follows this JSON-like structure:

```python
FILE_CONFIG = {
    "Excel File Name.xlsx": {              # Exact file name
        "Sheet Name": {                    # Exact sheet name
            "key_values": [...],           # Key-value pairs from specific cells
            "tables": [...],               # Tables with headers
            "no_title_tables": [...]       # Tables without headers
        }
    }
}
```

#### Key Configuration Sections

**Key Values (`key_values`)**

- Extract specific values from individual cells
- Define row/column coordinates (0-based indexing)
- Specify data formatting and placement rules
- Add metadata to table records

**Tables (`tables`)**

- Extract structured data from tables with headers
- Define database table mappings and primary keys
- Configure calculated columns and transformations
- Set export and update behavior

**No Title Tables (`no_title_tables`)**

- Extract data from tables without headers
- Specify start row and column structure
- Define custom headers and data types
- Configure exclusion rules for unwanted columns

**Calculated Columns (`calculated_columns`)**

- Create derived data fields using formulas
- Support cumulative, rolling, and percentage calculations
- Apply custom pandas expressions
- Add timestamps and metadata

#### Supported Calculation Types

- **Cumulative**: Running sums, averages, counts, min/max values
- **Rolling**: Window-based calculations over N periods
- **Percentage**: Total percentages and change calculations
- **Custom**: User-defined pandas formulas
- **Date**: Current date and timestamp fields

#### Database Integration

- Automatic table creation based on configuration
- Primary key management for upsert operations
- Data type inference from Excel content
- Conflict resolution and update strategies

For detailed configuration examples and complete documentation, see the comments in `config/file_config.py`.

#### Adding New File Configurations

To add support for a new Excel file type:

1. **Analyze the Excel File**

   - Determine the exact file name (including extension)
   - Map out the worksheet names and their content
   - Identify key data points and table structures
   - Document cell coordinates for key values (0-based indexing)
   - Note table boundaries, headers, and data types

2. **Add Configuration Entry**

   ```python
   "New Report.xlsx": {
       "Main Sheet": {
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
                   "title": "Data Table",
                   "add_keys": True,
                   "export_to_db": True,
                   "primary_keys": ["date", "category"],
                   "table_name": "new_report_data",
                   "headers": ["date", "category", "value"],
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
   ```

3. **Test the Configuration**

   - Place a sample file in `data/input/`
   - Run in test mode: `python app_cloud.py filename.xlsx`
   - Check logs for errors or warnings
   - Verify data extraction and database export
   - Validate calculated columns and transformations

4. **Best Practices**
   - Use exact file names including extensions
   - Use 0-based indexing for row/column coordinates
   - Choose appropriate primary keys for upsert operations
   - Test with representative sample files
   - Never hardcode credentials in configuration files

### db_config.py

Contains database connection settings and configuration:

- Database host, port, and credentials
- SSL configuration for secure connections
- Connection pooling and timeout settings
- Environment variable integration

The database configuration is environment-driven and reads all values from environment variables for security.

## Database Schema

The application creates and manages tables based on Excel file structure and configuration. It supports:

- Automatic table creation
- Data type conversion
- Primary key management
- Upsert operations with conflict resolution

## File Processing Status

The application tracks processing status in a dedicated database table:

- File name and COS key
- Processing status (processing, success, failed, archived)
- Error messages and timestamps
- Job run information and metadata

## Logging

Comprehensive logging is provided with:

- Job run identification
- Processing step details
- Error reporting with context
- Performance metrics
- IBM Cloud console integration

## Deployment

### Docker

```bash
docker build -t excel-cos-processor .
docker run -e ENVIRONMENT=prod excel-cos-processor
```

### IBM Cloud Code Engine

The application is designed to run as a job in IBM Cloud Code Engine with automatic trigger detection.

## Development

### Local Setup

1. Create virtual environment: `python3 -m venv venv`
2. Activate environment: `source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Set environment variables
5. Run in test mode: `ENVIRONMENT=test python app_cloud.py filename.xlsx`

### Testing

The application includes comprehensive error handling and can be tested locally using test mode.

## Error Handling

The application provides robust error handling:

- Graceful degradation when services are unavailable
- Detailed error messages with context
- Automatic resource cleanup
- Status tracking for failed operations
- Database error capture and logging

## Security

- Non-root user execution in Docker
- Environment variable configuration
- Secure database connections
- Input validation and sanitization
- Sensitive data masking in logs

## Performance

- Single file processing for optimal resource usage
- Efficient database operations with bulk upserts
- Proper resource cleanup
- Optimized memory usage
- Connection pooling and management

## Support

For issues and questions, please refer to the application logs and database status tables for detailed information about processing results and errors.
