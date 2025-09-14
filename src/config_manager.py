"""
Centralized Configuration Management
Located in src/ with correct imports to config/
"""

import os
import sys
import importlib.util
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pathlib import Path

# Global logger for config manager
_config_logger = None


def set_config_logger(logger):
    """Set the logger for config manager."""
    global _config_logger
    _config_logger = logger


def config_log(message: str, level: str = "INFO"):
    """Log message using config logger if available, otherwise print."""
    global _config_logger
    if _config_logger:
        if level == "INFO":
            _config_logger.info(message)
        elif level == "WARNING":
            _config_logger.warning(message)
        elif level == "ERROR":
            _config_logger.error(message)
    else:
        print(message)


# Get project root directory (parent of src directory)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add config directory to Python path
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
sys.path.insert(0, CONFIG_DIR)

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv

    env_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        config_log(f"Loaded environment from: {env_path}")
    else:
        config_log(f"No .env file found at: {env_path}")
except ImportError:
    config_log("python-dotenv not available, using system environment variables only")


@dataclass
class DatabaseConfig:
    """Database connection configuration"""

    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "verify-full"
    sslrootcert: str = "config/ibm-cloud-cert.crt"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for psycopg2"""
        # Make SSL cert path absolute
        sslrootcert_path = self.sslrootcert
        if not os.path.isabs(sslrootcert_path):
            sslrootcert_path = os.path.join(PROJECT_ROOT, sslrootcert_path)

        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "sslrootcert": sslrootcert_path,
        }

    def validate(self) -> bool:
        """Validate configuration"""
        required_fields = [self.host, self.database, self.user, self.password]
        if not all(required_fields):
            config_log("ERROR: Missing required database fields", "ERROR")
            return False

        # Check SSL certificate
        ssl_path = self.sslrootcert
        if not os.path.isabs(ssl_path):
            ssl_path = os.path.join(PROJECT_ROOT, ssl_path)

        if ssl_path and not os.path.exists(ssl_path):
            config_log(f"WARNING: SSL certificate not found: {ssl_path}", "WARNING")

        return True


@dataclass
class ProcessingConfig:
    """Data processing configuration"""

    input_dir: str = "data/input"
    output_dir: str = "data/output"
    archive_dir: str = "data/archive"
    logs_dir: str = "data/logs"
    batch_size: int = 1000
    enable_logging: bool = True
    enable_database: bool = True
    parallel_processing: bool = False
    max_workers: int = 4

    def get_absolute_paths(self) -> Dict[str, str]:
        """Get absolute paths for all directories"""
        return {
            "input_dir": os.path.join(PROJECT_ROOT, self.input_dir),
            "output_dir": os.path.join(PROJECT_ROOT, self.output_dir),
            "archive_dir": os.path.join(PROJECT_ROOT, self.archive_dir),
            "logs_dir": os.path.join(PROJECT_ROOT, self.logs_dir),
        }


class ConfigManager:
    """Centralized configuration management"""

    def __init__(self):
        self._db_config = None
        self._processing_config = None
        self._file_configs = None
        self._calculated_column_types = None
        self._configs_loaded = False

    def _ensure_configs_loaded(self):
        """Ensure configurations are loaded."""
        if not self._configs_loaded:
            self._load_configs()
            self.validate_all()
            self.create_directories()
            self._configs_loaded = True

    def _load_configs(self):
        """Load all configurations"""
        try:
            self._load_database_config()
            self._load_processing_config()
            self._load_file_configs()
            config_log("All configurations loaded successfully")
        except Exception as e:
            config_log(f"Configuration loading failed: {e}", "ERROR")
            raise ConfigurationError(f"Failed to load configuration: {e}")

    def _load_database_config(self):
        """Load database configuration"""
        try:
            # Try environment variables first
            if all(
                key in os.environ
                for key in ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
            ):
                config_log("Loading database config from environment variables")

                ssl_cert_path = os.environ.get(
                    "DB_SSLROOTCERT", "/etc/certs/ibm-cloud-cert.crt"
                )

                self._db_config = DatabaseConfig(
                    host=os.environ["DB_HOST"],
                    port=int(os.environ.get("DB_PORT", 31863)),
                    database=os.environ["DB_NAME"],
                    user=os.environ["DB_USER"],
                    password=os.environ["DB_PASSWORD"],
                    sslmode=os.environ.get("DB_SSLMODE", "verify-full"),
                    sslrootcert=ssl_cert_path,
                )
            else:
                # Fallback to config file
                config_log("Loading database config from db_config.py")
                try:
                    # Import with absolute path
                    db_config_path = os.path.join(CONFIG_DIR, "db_config.py")
                    spec = importlib.util.spec_from_file_location(
                        "db_config", db_config_path
                    )
                    db_config_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(db_config_module)

                    self._db_config = DatabaseConfig(**db_config_module.DB_CONFIG)
                except Exception as e:
                    raise ConfigurationError(f"Cannot load db_config.py: {e}")

        except Exception as e:
            raise ConfigurationError(f"Failed to load database configuration: {e}")

    def _load_processing_config(self):
        """Load processing configuration"""
        self._processing_config = ProcessingConfig(
            input_dir=os.environ.get("INPUT_DIR", "data/input"),
            output_dir=os.environ.get("OUTPUT_DIR", "data/output"),
            archive_dir=os.environ.get("ARCHIVE_DIR", "data/archive"),
            logs_dir=os.environ.get("LOGS_DIR", "data/logs"),
            batch_size=int(os.environ.get("BATCH_SIZE", 1000)),
            enable_logging=os.environ.get("ENABLE_LOGGING", "true").lower() == "true",
            enable_database=os.environ.get("ENABLE_DATABASE", "true").lower() == "true",
            parallel_processing=os.environ.get("PARALLEL_PROCESSING", "false").lower()
            == "true",
            max_workers=int(os.environ.get("MAX_WORKERS", 4)),
        )
        config_log("Processing configuration loaded")

    def _load_file_configs(self):
        """Load file processing configurations"""
        try:
            # Import with absolute path
            file_config_path = os.path.join(CONFIG_DIR, "file_config.py")
            spec = importlib.util.spec_from_file_location(
                "file_config", file_config_path
            )
            file_config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(file_config_module)

            self._file_configs = file_config_module.FILE_CONFIG
            self._calculated_column_types = file_config_module.CALCULATED_COLUMN_TYPES
            config_log(
                f"File configurations loaded: {len(self._file_configs)} file types"
            )
        except Exception as e:
            raise ConfigurationError(f"Failed to load file_config.py: {e}")

    @property
    def database(self) -> DatabaseConfig:
        self._ensure_configs_loaded()
        return self._db_config

    @property
    def processing(self) -> ProcessingConfig:
        self._ensure_configs_loaded()
        return self._processing_config

    @property
    def file_configs(self) -> Dict[str, Any]:
        self._ensure_configs_loaded()
        return self._file_configs

    @property
    def calculated_column_types(self) -> Dict[str, str]:
        self._ensure_configs_loaded()
        return self._calculated_column_types

    def get_file_config(self, file_name: str) -> Optional[Dict[str, Any]]:
        self._ensure_configs_loaded()
        return self._file_configs.get(file_name)

    def get_sheet_config(
        self, file_name: str, sheet_name: str
    ) -> Optional[Dict[str, Any]]:
        file_config = self.get_file_config(file_name)
        if file_config:
            return file_config.get(sheet_name)
        return None

    def validate_all(self) -> bool:
        """Validate all configurations"""
        try:
            # Validate database config
            if not self._db_config.validate():
                raise ConfigurationError("Invalid database configuration")

            # Create and validate directories
            abs_paths = self._processing_config.get_absolute_paths()
            for name, dir_path in abs_paths.items():
                Path(dir_path).mkdir(parents=True, exist_ok=True)
                config_log(f"Directory ready: {dir_path}")

            # Validate file configs
            if not self._file_configs:
                raise ConfigurationError("No file configurations found")

            config_log("All configurations validated successfully")
            return True

        except Exception as e:
            config_log(f"Configuration validation failed: {e}", "ERROR")
            raise ConfigurationError(f"Configuration validation failed: {e}")

    def create_directories(self):
        """Create all required directories"""
        abs_paths = self._processing_config.get_absolute_paths()
        for name, directory in abs_paths.items():
            Path(directory).mkdir(parents=True, exist_ok=True)
            config_log(f"Created directory: {directory}")


class ConfigurationError(Exception):
    """Configuration error exception"""

    pass


# Global configuration instance
_config_manager = None


def get_config() -> ConfigManager:
    """Get global configuration manager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager


def reload_config() -> ConfigManager:
    """Reload configuration"""
    global _config_manager
    _config_manager = ConfigManager()
    return _config_manager


# Helper functions for backward compatibility
def get_database_config() -> Dict[str, Any]:
    """Get database configuration as dictionary"""
    return get_config().database.to_dict()


def get_processing_config() -> ProcessingConfig:
    """Get processing configuration"""
    return get_config().processing


def get_file_config(file_name: str) -> Optional[Dict[str, Any]]:
    """Get file configuration"""
    return get_config().get_file_config(file_name)
