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

Defines file type configurations including:

- File name patterns and matching rules
- Sheet processing configurations
- Column mappings and transformations
- Calculated fields and formulas
- Database table mappings

### db_config.py

Contains database connection settings and configuration.

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
