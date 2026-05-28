#!/usr/bin/env python3
"""
Create a connections.csv template at config/connections.csv.

Usage:
    python scripts/create_connections_template.py
"""

import csv
import os


def create_template():
    os.makedirs("config", exist_ok=True)
    path = os.path.join("config", "connections.csv")

    headers = [
        "Customer Name", "System Name", "Site ID",
        "Host Name", "User Name", "Password",
    ]
    sample_rows = [
        ("CLIENTE", "SISTEMA", "SITEID", "HOSTNAME", "pdcradmin", "pdcradmin")
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(sample_rows)

    print(f"[OK] Created: {path}")


if __name__ == "__main__":
    create_template()
