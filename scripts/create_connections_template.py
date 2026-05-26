#!/usr/bin/env python3
"""
Create an empty connections.xlsx template at config/connections.xlsx.

Usage:
    python scripts/create_connections_template.py
"""

import os
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    import pandas as pd
except ImportError:
    print("[ERROR] pandas is required. Run: pip install pandas openpyxl")
    sys.exit(1)

try:
    import openpyxl  # noqa: F401 — required by pandas for .xlsx
except ImportError:
    print("[ERROR] openpyxl is required. Run: pip install openpyxl")
    sys.exit(1)


def main():
    config_dir = project_root / "config"
    config_dir.mkdir(exist_ok=True)

    output_path = config_dir / "connections.xlsx"

    if output_path.exists():
        print(f"[INFO] File already exists: {output_path}")
        overwrite = input("Overwrite? (y/N): ").strip().lower()
        if overwrite != "y":
            print("[INFO] Aborted.")
            return

    # Template with one example row
    df = pd.DataFrame({
        "Customer": ["EXAMPLE_CLIENT"],
        "System": ["PRD"],
        "Host": ["your_teradata_host"],
        "User": ["your_username"],
        "Password": ["your_password"],
        "Database": ["your_database"],
    })

    df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"[OK] Template created: {output_path}")
    print()
    print("Next steps:")
    print("  1. Open config/connections.xlsx in Excel")
    print("  2. Replace the example row with your real credentials")
    print("  3. Add one row per customer/system combination")
    print("  4. Save and close Excel")
    print("  5. Run: streamlit run ui/main.py")


if __name__ == "__main__":
    main()
