"""
Connections Manager — reads client/system credentials from config/connections.xlsx.

Provides helper functions for the sidebar connection selector in the UI.
"""

import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

_CONNECTIONS_FILE = Path(__file__).resolve().parent.parent / "config" / "connections.xlsx"

_REQUIRED_COLUMNS = ["Customer", "System", "Host", "User", "Password"]


def load_connections() -> pd.DataFrame:
    """Load connection entries from config/connections.xlsx.

    Returns an empty DataFrame if the file is missing or unreadable.
    """
    if not _CONNECTIONS_FILE.exists():
        logger.warning("connections.xlsx not found at %s", _CONNECTIONS_FILE)
        return pd.DataFrame()
    try:
        df = pd.read_excel(_CONNECTIONS_FILE, engine="openpyxl")
        missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            logger.error("connections.xlsx missing columns: %s", missing)
            return pd.DataFrame()
        return df
    except Exception as exc:
        logger.error("Failed to read connections.xlsx: %s", exc)
        return pd.DataFrame()


def get_customers(df: pd.DataFrame) -> list:
    """Return sorted list of unique customer names."""
    if df.empty or "Customer" not in df.columns:
        return []
    return sorted(df["Customer"].dropna().unique().tolist())


def get_systems(df: pd.DataFrame, customer: str) -> list:
    """Return sorted list of systems for a given customer."""
    if df.empty or "System" not in df.columns:
        return []
    filtered = df[df["Customer"] == customer]
    return sorted(filtered["System"].dropna().unique().tolist())


def get_connection_params(df: pd.DataFrame, customer: str, system: str) -> dict:
    """Return connection parameter dict for a customer/system pair."""
    if df.empty:
        return {}
    mask = (df["Customer"] == customer) & (df["System"] == system)
    rows = df[mask]
    if rows.empty:
        return {}
    row = rows.iloc[0]
    return {
        "host": str(row.get("Host", "")),
        "user": str(row.get("User", "")),
        "password": str(row.get("Password", "")),
        "database": str(row.get("Database", "")),
        "customer": customer,
        "system": system,
    }
