"""
Connections Manager — reads client/system credentials from config/connections.csv.

Provides helper functions for the sidebar connection selector in the UI.
"""

import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)

CONNECTIONS_FILE = os.path.join(
    os.path.dirname(__file__), "..", "config", "connections.csv"
)

_EXPECTED_COLS = [
    "Customer Name", "System Name", "Site ID",
    "Host Name", "User Name", "Password",
]


def load_connections() -> pd.DataFrame:
    """Load connection entries from config/connections.csv.

    Returns an empty DataFrame if the file is missing or unreadable.
    """
    if not os.path.exists(CONNECTIONS_FILE):
        logger.warning("connections.csv not found at %s", CONNECTIONS_FILE)
        return pd.DataFrame(columns=_EXPECTED_COLS)
    try:
        df = pd.read_csv(
            CONNECTIONS_FILE,
            dtype=str,
            encoding="utf-8",
            keep_default_na=False,
        )
        df.columns = [c.strip() for c in df.columns]
        df = df.dropna(subset=["Host Name", "User Name"])
        df = df.fillna("")
        return df
    except Exception as exc:
        logger.error("Cannot read connections file: %s", exc)
        return pd.DataFrame(columns=_EXPECTED_COLS)


def get_customers(df: pd.DataFrame) -> list:
    """Return sorted list of unique customer names."""
    if df.empty or "Customer Name" not in df.columns:
        return []
    return sorted(df["Customer Name"].dropna().unique().tolist())


def get_systems(df: pd.DataFrame, customer: str) -> list:
    """Return sorted list of systems for a given customer."""
    if df.empty or "System Name" not in df.columns:
        return []
    filtered = df[df["Customer Name"] == customer]
    return sorted(filtered["System Name"].dropna().unique().tolist())


def get_connection_params(df: pd.DataFrame, customer: str, system: str) -> dict:
    """Return connection parameter dict for a customer/system pair."""
    if df.empty:
        return {}
    mask = (df["Customer Name"] == customer) & (df["System Name"] == system)
    rows = df[mask]
    if rows.empty:
        return {}
    row = rows.iloc[0]
    return {
        "host": str(row.get("Host Name", "")),
        "user": str(row.get("User Name", "")),
        "password": str(row.get("Password", "")),
        "site_id": str(row.get("Site ID", "")),
        "customer": customer,
        "system": system,
    }
