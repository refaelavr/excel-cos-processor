"""
Environment utility functions for configuration and environment detection.
"""

import os
import sys
import json
import base64
from typing import Optional, Dict, Any


def get_environment() -> str:
    """Get current environment (prod/test)."""
    return os.getenv("ENVIRONMENT", "prod").lower()


def is_production() -> bool:
    """Check if running in production environment."""
    return get_environment() == "prod"


def is_code_engine_job() -> bool:
    """Check if running as IBM Cloud Code Engine job."""
    return bool(os.getenv("CE_JOB"))


def get_job_info() -> Dict[str, str]:
    """Get Code Engine job information."""
    return {
        "job_run_id": os.getenv("CE_JOBRUN", "unknown"),
        "job_name": os.getenv("CE_JOB", "unknown"),
        "project_id": os.getenv("CE_PROJECT_ID", "unknown"),
        "region": os.getenv("CE_REGION", "unknown"),
    }


def get_cos_endpoint() -> str:
    """Get COS endpoint based on environment."""
    if is_production():
        return os.getenv("COS_INTERNAL_ENDPOINT", "")
    else:
        return os.getenv("COS_ENDPOINT", "")


def mask_sensitive_value(value: str, mask_char: str = "*") -> str:
    """Mask sensitive values for logging."""
    if not value or len(value) <= 4:
        return mask_char * len(value) if value else ""
    return value[:2] + mask_char * (len(value) - 4) + value[-2:]


def log_environment_variables(logger, sensitive_vars: Optional[list] = None) -> None:
    """Log environment variables for debugging."""
    if sensitive_vars is None:
        sensitive_vars = ["DB_PASSWORD", "COS_API_KEY"]

    for var in os.environ:
        if var.startswith(("CE_", "COS_", "DB_", "KUBERNETES_")):
            value = os.environ[var]
            if var in sensitive_vars:
                masked = mask_sensitive_value(value)
                logger.info(f"Environment variable {var}: {masked}")
            else:
                logger.info(f"Environment variable {var}: {value}")


def extract_filename_from_trigger() -> Optional[str]:
    """Extract filename from IBM Cloud Code Engine trigger event."""
    # Try CE_SUBJECT first (most reliable)
    ce_subject = os.getenv("CE_SUBJECT")
    if ce_subject:
        return ce_subject

    # Try CE_DATA (base64 encoded JSON)
    ce_data = os.getenv("CE_DATA")
    if ce_data:
        try:
            decoded_data = base64.b64decode(ce_data).decode("utf-8")
            data = json.loads(decoded_data)
            if "key" in data:
                return data["key"]
        except Exception:
            pass

    # Try stdin (fallback)
    try:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.read().strip()
            if stdin_data:
                try:
                    data = json.loads(stdin_data)
                    if "key" in data:
                        return data["key"]
                except json.JSONDecodeError:
                    # Try as plain text
                    if stdin_data and not stdin_data.startswith("{"):
                        return stdin_data
        else:
            return None
    except Exception:
        pass

    return None


def get_environment_info() -> Dict[str, str]:
    """Get comprehensive environment information."""
    job_info = get_job_info()
    return {
        **job_info,
        "environment": get_environment(),
        "cos_endpoint": get_cos_endpoint(),
        "is_production": str(is_production()),
        "is_code_engine": str(is_code_engine_job()),
    }
