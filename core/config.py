"""
Configuration Module for TD Stats Optimizer

This module contains system-wide configuration parameters including:
- Thresholds for statistics analysis
- System database exclusions (loaded dynamically from external file)
"""

import os
from typing import Tuple

# System Thresholds
THRESHOLDS = {
    "stats_stale_days": 15,
    "pi_skew_pct": 30,
    "large_scan_size_gb": 10,
    "unused_object_days": 90,
    "space_critical_pct": 80,
    "space_warning_pct": 60,
}

# Load system database exclusions dynamically from external file
_current_dir = os.path.dirname(os.path.abspath(__file__))
_sys_db_file = os.path.join(_current_dir, 'system_databases.txt')

try:
    with open(_sys_db_file, 'r', encoding='utf-8') as f:
        SYSTEM_DATABASES = tuple(
            line.strip() 
            for line in f 
            if line.strip() and not line.startswith('#')
        )
except FileNotFoundError:
    # Fallback minimal list if file is missing
    SYSTEM_DATABASES = (
        'DBC', 
        'Crashdumps', 
        'PDCRINFO', 
        'SYSADMIN', 
        'TDWM',
        'SYS_CALENDAR',
        'SYSLIB',
        'SYSUDTLIB',
        'SYSBAR',
        'SYSTEMFE'
    )
