"""
Structured logging configuration.

Called once from create_app(). Supports text (human-readable) and JSON formats
via LOG_FORMAT env var. LOG_LEVEL defaults to INFO.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Single-line JSON log formatter for production log aggregators."""

    def format(self, record):
        entry = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry['exception'] = self.formatException(record.exc_info)
        return json.dumps(entry)


# Third-party loggers that are noisy at INFO
_NOISY_LOGGERS = [
    'urllib3',
    'botocore',
    'boto3',
    'openai',
    'httpcore',
    'httpx',
]


def configure_logging(app=None):
    """
    Set up root logger with format/level from env vars.

    Environment variables:
        LOG_LEVEL  — Python log level name (default: INFO)
        LOG_FORMAT — "text" (default) or "json"
    """
    level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
    level = getattr(logging, level_name, logging.INFO)

    log_format = os.getenv('LOG_FORMAT', 'text').lower()

    root = logging.getLogger()
    root.setLevel(level)

    # Remove any existing handlers to avoid duplicates on re-init
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)

    if log_format == 'json':
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(levelname)s %(name)s — %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        ))

    root.addHandler(handler)

    # Quiet noisy third-party loggers
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
