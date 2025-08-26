#!/usr/bin/env python3
"""
Excel COS Processor

A modular Excel file processor for IBM Cloud Object Storage.
Supports single file processing in both production (trigger-based) and test modes.

Usage:
    python app_cloud.py                    # Production: Process triggered file
    python app_cloud.py filename.xlsx     # Test: Process specific file

Author: Excel COS Processor Team
"""

import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from services.app_orchestrator import AppOrchestrator
from utils.environment_utils import get_environment, get_job_info


def main():
    """
    Main entry point - Application orchestration.
    
    This function coordinates the application workflow.
    All business logic is delegated to specialized services.
    """
    try:
        # Log startup information
        environment = get_environment()
        job_info = get_job_info()

        print(f"=== EXCEL PROCESSOR STARTING ===")
        print(f"Environment: {environment.upper()}")
        print(f"Job Run ID: {job_info.get('job_run_id', 'unknown')}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"=== PROCESSING START ===")

        # Create and run the orchestrator
        orchestrator = AppOrchestrator()
        return orchestrator.run()

    except KeyboardInterrupt:
        print("\nProcessor stopped by user")
        print("=== PROCESSING END - INTERRUPTED ===")
        return 0

    except Exception as e:
        print(f"Processor failed: {str(e)}")
        print("=== PROCESSING END - ERROR ===")
        return 1


if __name__ == "__main__":
    exit(main())
