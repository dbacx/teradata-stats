"""
Centralized Logging Configuration for VantageOps

All modules import `configure_logging()` to attach the shared
RotatingFileHandler to the root logger. The handler appends to
a single persistent file: logs/vantageops.log
"""

import logging
import os
from logging.handlers import RotatingFileHandler

_LOG_CONFIGURED = False

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'vantageops.log')


def configure_logging():
    """Attach a RotatingFileHandler + StreamHandler to the root logger (once)."""
    global _LOG_CONFIGURED
    if _LOG_CONFIGURED:
        return
    _LOG_CONFIGURED = True

    os.makedirs(LOG_DIR, exist_ok=True)

    fmt = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(module)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding='utf-8',
    )
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
