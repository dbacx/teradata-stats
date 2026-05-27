"""
CSV Logger Utility

Provides functionality to log execution data to CSV files for audit purposes.
"""

import csv
import os
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

# ---------------------------------------------------------------------------
# Execution log — single persistent file, one row per module execution
# ---------------------------------------------------------------------------

EXECUTION_LOG = os.path.join(LOG_DIR, "execution_log.csv")

EXECUTION_COLUMNS = [
    "timestamp", "customer", "site_id", "system",
    "ticket", "project_name", "system_name", "resource_name",
    "issue", "request_by", "comments",
]


def log_execution(record: dict) -> None:
    """Append one execution record to logs/execution_log.csv.

    Creates the file with headers if it does not exist.
    Fields missing from *record* default to empty string.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    file_exists = os.path.exists(EXECUTION_LOG)

    row = {col: record.get(col, "") for col in EXECUTION_COLUMNS}
    if not row.get("timestamp"):
        row["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(EXECUTION_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXECUTION_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ---------------------------------------------------------------------------
# Analysis history logger — appends one row per successful analysis run
# ---------------------------------------------------------------------------

ANALYSIS_LOG = os.path.join(LOG_DIR, "analysis_history.csv")

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
    os.makedirs(LOG_DIR, exist_ok=True)
    file_exists = os.path.exists(ANALYSIS_LOG)

    row = {col: record.get(col, 0) for col in ANALYSIS_COLUMNS}
    if not row.get("timestamp"):
        row["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(ANALYSIS_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ANALYSIS_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
