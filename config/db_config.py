"""
Database connection configuration (environment-driven)
====================================================

This module defines DB_CONFIG but reads all values from environment variables.
It intentionally contains no secrets and is safe to commit.

ENVIRONMENT VARIABLES:
---------------------
Required:
- DB_HOST: Database server hostname or IP address
- DB_PORT: Database server port (default: 31863)
- DB_NAME: Database name
- DB_USER: Database username
- DB_PASSWORD: Database password

Optional:
- DB_SSLMODE: SSL mode for connection (default: verify-full)
  Options: disable, require, verify-ca, verify-full
- DB_SSLROOTCERT: Path to SSL root certificate (default: /etc/certs/ibm-cloud-cert.crt)

CONFIGURATION KEYS:
------------------
- host: Database server address
- port: Database server port (integer)
- database: Database name
- user: Database username
- password: Database password
- sslmode: SSL connection mode
- sslrootcert: Path to SSL certificate file

SECURITY NOTES:
--------------
- All sensitive values are read from environment variables
- SSL is enabled by default for secure connections
- Certificate verification is required in production
- No hardcoded credentials in the configuration file

USAGE:
------
The DB_CONFIG dictionary is used by the DatabaseService class
to establish connections to the PostgreSQL database. The service
automatically handles connection pooling, retries, and cleanup.

EXAMPLE ENVIRONMENT SETUP:
-------------------------
export DB_HOST=your-db-host.com
export DB_PORT=5432
export DB_NAME=your_database
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_SSLMODE=verify-full
export DB_SSLROOTCERT=/path/to/cert.crt
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
    "sslrootcert": os.getenv("DB_SSLROOTCERT", "/etc/certs/ibm-cloud-cert.crt"),
}
