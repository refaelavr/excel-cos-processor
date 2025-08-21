"""
Database connection configuration (environment-driven)

This module defines DB_CONFIG but reads all values from environment variables.
It intentionally contains no secrets and is safe to commit.

Expected environment variables:
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
- DB_SSLMODE (default: verify-full)
- DB_SSLROOTCERT (default: config/ibm-cloud-cert.crt)
"""

import os


DB_CONFIG = {
    "host": os.getenv("DB_HOST", ""),
    "port": int(os.getenv("DB_PORT", "31863")),
    "database": os.getenv("DB_NAME", ""),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    "sslmode": os.getenv("DB_SSLMODE", "verify-full"),
    # Keep relative; callers normalize to absolute project path when needed
    "sslrootcert": os.getenv("DB_SSLROOTCERT", "config/ibm-cloud-cert.crt"),
}
