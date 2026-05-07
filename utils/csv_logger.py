"""
CSV Logger Utility

Provides functionality to log execution data to CSV files for audit purposes.
"""

import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


def log_execution(
    ticket: str,
    project_name: str,
    system_name: str,
    resource_name: str,
    issue: str,
    request_by: str,
    comments: Optional[str] = None
):
    """
    Log execution data to CSV file.
    
    Args:
        ticket: Ticket number or identifier
        project_name: Name of the project
        system_name: Name of the system
        resource_name: Name of the resource
        issue: Issue description
        request_by: Requestor name
        comments: Optional comments
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create CSV file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"execution_log_{timestamp}.csv"
    
    # Prepare data row
    log_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ticket": ticket,
        "project_name": project_name,
        "system_name": system_name,
        "resource_name": resource_name,
        "issue": issue,
        "request_by": request_by,
        "comments": comments or ""
    }
    
    # Write to CSV
    with open(log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=log_data.keys())
        writer.writeheader()
        writer.writerow(log_data)
