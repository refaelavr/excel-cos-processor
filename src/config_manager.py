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
        print(f"Loaded environment from: {env_path}")
    else:
        print(f"No .env file found at: {env_path}")
except ImportError:
    print("python-dotenv not available, using system environment variables only")


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
            print("ERROR: Missing required database fields")
            return False

        # Check SSL certificate
        ssl_path = self.sslrootcert
        if not os.path.isabs(ssl_path):
            ssl_path = os.path.join(PROJECT_ROOT, ssl_path)

        if ssl_path and not os.path.exists(ssl_path):
            print(f"WARNING: SSL certificate not found: {ssl_path}")

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
        self._load_configs()

    def _load_configs(self):
        """Load all configurations"""
        try:
            self._load_database_config()
            self._load_processing_config()
            self._load_file_configs()
            print("All configurations loaded successfully")
        except Exception as e:
            print(f"Configuration loading failed: {e}")
            raise ConfigurationError(f"Failed to load configuration: {e}")

    def _load_database_config(self):
        """Load database configuration"""
        try:
            # Try environment variables first
            if all(
                key in os.environ
                for key in ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
            ):
                print("Loading database config from environment variables")

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
                print("Loading database config from db_config.py")
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
        print("Processing configuration loaded")

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
            print(f"File configurations loaded: {len(self._file_configs)} file types")
        except Exception as e:
            raise ConfigurationError(f"Failed to load file_config.py: {e}")

    @property
    def database(self) -> DatabaseConfig:
        return self._db_config

    @property
    def processing(self) -> ProcessingConfig:
        return self._processing_config

    @property
    def file_configs(self) -> Dict[str, Any]:
        return self._file_configs

    @property
    def calculated_column_types(self) -> Dict[str, str]:
        return self._calculated_column_types

    def get_file_config(self, file_name: str) -> Optional[Dict[str, Any]]:
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
                print(f"Directory ready: {dir_path}")

            # Validate file configs
            if not self._file_configs:
                raise ConfigurationError("No file configurations found")

            print("All configurations validated successfully")
            return True

        except Exception as e:
            print(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Configuration validation failed: {e}")

    def create_directories(self):
        """Create all required directories"""
        abs_paths = self._processing_config.get_absolute_paths()
        for name, directory in abs_paths.items():
            Path(directory).mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {directory}")


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
        _config_manager.validate_all()
        _config_manager.create_directories()
    return _config_manager


def reload_config() -> ConfigManager:
    """Reload configuration"""
    global _config_manager
    _config_manager = ConfigManager()
    _config_manager.validate_all()
    _config_manager.create_directories()
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


def test_config():
    """Test configuration loading"""
    try:
        print("\n" + "=" * 50)
        print("CONFIGURATION TEST")
        print("=" * 50)

        config = get_config()

        print(f"Project Root: {PROJECT_ROOT}")
        print(f"Config Dir: {CONFIG_DIR}")
        print(f"Database Host: {config.database.host}")
        print(f"Database Name: {config.database.database}")

        abs_paths = config.processing.get_absolute_paths()
        print("\nDirectory Structure:")
        for name, path in abs_paths.items():
            exists = "OK" if os.path.exists(path) else "MISSING"
            print(f"  {exists} {name}: {path}")

        print(f"\nBatch Size: {config.processing.batch_size}")
        print(f"Database Enabled: {config.processing.enable_database}")
        print(f"File Configurations: {len(config.file_configs)}")

        print("\nConfiguration test passed!")
        return True
    except Exception as e:
        print(f"\nConfiguration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_config()
