import os
from datetime import datetime


class Colors:
    """Simple ANSI color codes"""

    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"


# Global logging setup
LOG_ENABLED = True
LOG_DIR = "logs"
LOG_FILE = None


def setup_logging(enable=True, log_directory="logs"):
    """
    Setup logging to file with timestamp.

    Args:
        enable: Whether to enable logging to file
        log_directory: Directory to store log files
    """
    global LOG_ENABLED, LOG_DIR, LOG_FILE

    LOG_ENABLED = enable
    LOG_DIR = log_directory

    if enable:
        # Create logs directory if it doesn't exist
        os.makedirs(LOG_DIR, exist_ok=True)

        # Create log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        LOG_FILE = os.path.join(LOG_DIR, f"processing_{timestamp}.log")

        # Write initial log entry
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(
                f"=== Excel Data Processing Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n"
            )

        print(f"Logging enabled: {LOG_FILE}")


def write_to_log(message, level="INFO"):
    """
    Write message to log file if logging is enabled.

    Args:
        message: Message to log
        level: Log level (INFO, SUCCESS, WARNING, ERROR)
    """
    if not LOG_ENABLED or not LOG_FILE:
        return

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}\n"

        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception as e:
        print(f"Error writing to log: {e}")


def print_success(message):
    """Print success message in green and log it"""
    print(f"{Colors.GREEN}{message}{Colors.RESET}")
    write_to_log(message, "SUCCESS")


def print_error(message):
    """Print error message in red and log it"""
    print(f"{Colors.RED}{message}{Colors.RESET}")
    write_to_log(message, "ERROR")


def print_warning(message):
    """Print warning message in yellow and log it"""
    print(f"{Colors.YELLOW}{message}{Colors.RESET}")
    write_to_log(message, "WARNING")


def print_normal(message):
    """Print normal message (no color) and log it"""
    print(message)
    write_to_log(message, "INFO")


def log_only(message, level="INFO"):
    """
    Write message to log file only (no console output).
    Useful for detailed debugging info.
    """
    write_to_log(message, level)


def close_logging():
    """
    Close logging session with summary.
    """
    if LOG_ENABLED and LOG_FILE:
        write_to_log("=== Processing completed ===", "INFO")
        print(f"Log file saved: {LOG_FILE}")
