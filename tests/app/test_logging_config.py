"""Tests for structured logging configuration."""
import json
import logging
import os
from unittest.mock import patch

import pytest

from app.logging_config import configure_logging, JSONFormatter


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Reset root logger state after each test."""
    root = logging.getLogger()
    original_level = root.level
    original_handlers = root.handlers[:]
    yield
    root.setLevel(original_level)
    root.handlers = original_handlers


class TestConfigureLogging:
    """Tests for configure_logging()."""

    def test_default_level_is_info(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LOG_LEVEL', None)
            configure_logging()
        assert logging.getLogger().level == logging.INFO

    def test_log_level_env_var_changes_level(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}):
            configure_logging()
        assert logging.getLogger().level == logging.DEBUG

    def test_log_level_warning(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'WARNING'}):
            configure_logging()
        assert logging.getLogger().level == logging.WARNING

    def test_log_level_case_insensitive(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'debug'}):
            configure_logging()
        assert logging.getLogger().level == logging.DEBUG

    def test_invalid_log_level_defaults_to_info(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'NONSENSE'}):
            configure_logging()
        assert logging.getLogger().level == logging.INFO

    def test_text_format_includes_timestamp_and_logger_name(self, capsys):
        with patch.dict(os.environ, {'LOG_FORMAT': 'text'}):
            configure_logging()
        test_logger = logging.getLogger('test.module')
        test_logger.info("hello world")
        output = capsys.readouterr().err
        assert 'test.module' in output
        assert 'hello world' in output
        assert 'INFO' in output

    def test_json_format_produces_parseable_json(self, capsys):
        with patch.dict(os.environ, {'LOG_FORMAT': 'json'}):
            configure_logging()
        test_logger = logging.getLogger('test.json')
        test_logger.info("structured log")
        output = capsys.readouterr().err.strip()
        parsed = json.loads(output)
        assert parsed['level'] == 'INFO'
        assert parsed['logger'] == 'test.json'
        assert parsed['message'] == 'structured log'
        assert 'timestamp' in parsed

    def test_json_format_includes_exception(self, capsys):
        with patch.dict(os.environ, {'LOG_FORMAT': 'json'}):
            configure_logging()
        test_logger = logging.getLogger('test.exc')
        try:
            raise ValueError("boom")
        except ValueError:
            test_logger.error("failed", exc_info=True)
        output = capsys.readouterr().err.strip()
        parsed = json.loads(output)
        assert parsed['level'] == 'ERROR'
        assert 'exception' in parsed
        assert 'ValueError' in parsed['exception']

    def test_third_party_loggers_quieted_to_warning(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LOG_LEVEL', None)
            configure_logging()
        for name in ['urllib3', 'botocore', 'boto3', 'openai', 'httpcore', 'httpx']:
            assert logging.getLogger(name).level == logging.WARNING

    def test_no_duplicate_handlers_on_repeated_calls(self):
        configure_logging()
        configure_logging()
        assert len(logging.getLogger().handlers) == 1


class TestJSONFormatter:
    """Tests for the JSONFormatter class."""

    def test_format_basic_record(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name='test', level=logging.INFO, pathname='', lineno=0,
            msg='hello %s', args=('world',), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed['message'] == 'hello world'
        assert parsed['level'] == 'INFO'
        assert parsed['logger'] == 'test'
