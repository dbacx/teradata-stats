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


# ---------------------------------------------------------------------------
# Analysis history logger — appends one row per successful analysis run
# ---------------------------------------------------------------------------

ANALYSIS_LOG = os.path.join(
    os.path.dirname(__file__), "..", "logs", "analysis_history.csv"
)

ANALYSIS_COLUMNS = [
    "timestamp", "customer", "site_id", "system", "database_filter",
    "total_tables", "zero_stats", "missing_table", "stale_stats", "bloat",
    "total_findings", "critical_count", "high_count", "medium_count",
    "low_count", "collect_ddls", "drop_ddls", "execution_seconds",
]


def log_analysis_result(record: dict) -> None:
    """Append one analysis result row to logs/analysis_history.csv.

    Creates the file with headers if it does not exist.
    Missing keys in *record* default to 0 or empty string.
    """
    os.makedirs(os.path.dirname(ANALYSIS_LOG), exist_ok=True)
    file_exists = os.path.exists(ANALYSIS_LOG)

    row = {col: record.get(col, 0) for col in ANALYSIS_COLUMNS}
    if not row.get("timestamp"):
        row["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(ANALYSIS_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ANALYSIS_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
