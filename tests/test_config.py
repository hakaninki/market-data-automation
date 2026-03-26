"""Tests for configuration loading."""

import os
import pytest
from unittest.mock import patch

from src.config import load_config, Config


def test_load_config_defaults():
    """load_config returns expected defaults when env vars are absent."""
    env_overrides = {
        "COINGECKO_API_KEY": None,
        "GOOGLE_CREDENTIALS_JSON": None,
        "GOOGLE_SHEET_ID": None,
        "CSV_PATH": None,
        "SQLITE_PATH": None,
        "PIPELINE_VERSION": None,
        "ENABLE_SHEETS": None,
        "ENABLE_CSV": None,
        "ENABLE_SQLITE": None,
    }
    with patch.dict(os.environ, {}, clear=False):
        # Remove the keys if present so defaults kick in
        for key in env_overrides:
            os.environ.pop(key, None)

        config = load_config()

    assert isinstance(config, Config)
    assert config.coingecko_api_key is None
    assert config.google_credentials_json == ""
    assert config.google_sheet_id == "local"
    assert config.csv_path == "data/market_data.csv"
    assert config.sqlite_path == "data/market_data.db"
    assert config.pipeline_version == "1.0.0"
    assert config.enable_sheets is True
    assert config.enable_csv is True
    assert config.enable_sqlite is True


def test_load_config_custom_values():
    """load_config picks up custom env vars correctly."""
    env = {
        "COINGECKO_API_KEY": "my-api-key",
        "GOOGLE_CREDENTIALS_JSON": '{"type": "service_account"}',
        "GOOGLE_SHEET_ID": "sheet123",
        "CSV_PATH": "/tmp/out.csv",
        "SQLITE_PATH": "/tmp/out.db",
        "PIPELINE_VERSION": "2.5.0",
        "ENABLE_SHEETS": "false",
        "ENABLE_CSV": "false",
        "ENABLE_SQLITE": "true",
    }
    with patch.dict(os.environ, env, clear=False):
        config = load_config()

    assert config.coingecko_api_key == "my-api-key"
    assert config.google_credentials_json == '{"type": "service_account"}'
    assert config.google_sheet_id == "sheet123"
    assert config.csv_path == "/tmp/out.csv"
    assert config.sqlite_path == "/tmp/out.db"
    assert config.pipeline_version == "2.5.0"
    assert config.enable_sheets is False
    assert config.enable_csv is False
    assert config.enable_sqlite is True


def test_load_config_enable_flags_case_insensitive():
    """Enable flags are case-insensitive (True / TRUE / true all work)."""
    with patch.dict(os.environ, {"ENABLE_SHEETS": "TRUE", "ENABLE_CSV": "False", "ENABLE_SQLITE": "TRUE"}, clear=False):
        config = load_config()

    assert config.enable_sheets is True
    assert config.enable_csv is False
    assert config.enable_sqlite is True


def test_load_config_returns_config_dataclass():
    """Return type is Config."""
    with patch.dict(os.environ, {}, clear=False):
        config = load_config()
    assert isinstance(config, Config)
