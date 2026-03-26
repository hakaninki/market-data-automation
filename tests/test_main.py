"""Tests for the main pipeline orchestration."""

import pytest
from unittest.mock import patch, MagicMock

from src.config import Config
from src.models import MarketRecord


def _make_record(symbol="BTC", run_date="2024-01-01"):
    return MarketRecord(
        run_date=run_date,
        fetched_at="2024-01-01T08:00:00+00:00",
        symbol=symbol,
        name=symbol,
        price_usd=50000.0,
        change_pct_24h=1.0,
        high_24h_usd=51000.0,
        low_24h_usd=49000.0,
        momentum_proxy=None,
        volatility_proxy=2.0,
        daily_delta_usd=None,
        data_source="test",
        pipeline_version="1.0.0",
    )


def _make_config(**kwargs):
    defaults = dict(
        coingecko_api_key=None,
        google_credentials_json="",
        google_sheet_id="local",
        csv_path="/tmp/test_pipeline.csv",
        sqlite_path="/tmp/test_pipeline.db",
        pipeline_version="1.0.0",
        enable_sheets=False,
        enable_csv=False,
        enable_sqlite=False,
    )
    defaults.update(kwargs)
    return Config(**defaults)


@patch("main.SheetsStore")
@patch("main.SQLiteStore")
@patch("main.CSVStore")
@patch("main.normalize_commodity")
@patch("main.normalize_crypto")
@patch("main.validate_commodity_raw")
@patch("main.validate_crypto_raw")
@patch("main.fetch_gold")
@patch("main.fetch_crypto")
@patch("main.load_config")
def test_run_pipeline_success(
    mock_load_config,
    mock_fetch_crypto,
    mock_fetch_gold,
    mock_validate_crypto,
    mock_validate_commodity,
    mock_normalize_crypto,
    mock_normalize_commodity,
    mock_csv,
    mock_sqlite,
    mock_sheets,
):
    """Pipeline returns True when all steps succeed and no adapters are enabled."""
    from main import run_pipeline

    config = _make_config()
    mock_load_config.return_value = config
    mock_fetch_crypto.return_value = {"btc_price": 60000.0, "eth_price": 3000.0}
    mock_fetch_gold.return_value = {"gold_price_usd": 2000.0}
    mock_normalize_crypto.return_value = [_make_record("BTC"), _make_record("ETH")]
    mock_normalize_commodity.return_value = _make_record("GOLD")

    result = run_pipeline()

    assert result is True
    mock_fetch_crypto.assert_called_once()
    mock_fetch_gold.assert_called_once()


@patch("main.normalize_commodity")
@patch("main.normalize_crypto")
@patch("main.validate_commodity_raw")
@patch("main.validate_crypto_raw")
@patch("main.fetch_gold")
@patch("main.fetch_crypto")
@patch("main.load_config")
def test_run_pipeline_with_csv_adapter(
    mock_load_config,
    mock_fetch_crypto,
    mock_fetch_gold,
    mock_validate_crypto,
    mock_validate_commodity,
    mock_normalize_crypto,
    mock_normalize_commodity,
):
    """Pipeline uses CSVStore when enable_csv is True."""
    import tempfile, os
    from main import run_pipeline

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "out.csv")
        db_path = os.path.join(tmp, "out.db")
        config = _make_config(enable_csv=True, enable_sqlite=True, csv_path=csv_path, sqlite_path=db_path)
        mock_load_config.return_value = config
        mock_fetch_crypto.return_value = {"btc_price": 60000.0, "eth_price": 3000.0}
        mock_fetch_gold.return_value = {"gold_price_usd": 2000.0}
        mock_normalize_crypto.return_value = [_make_record("BTC"), _make_record("ETH")]
        mock_normalize_commodity.return_value = _make_record("GOLD")

        result = run_pipeline()

    assert result is True


@patch("main.normalize_commodity")
@patch("main.normalize_crypto")
@patch("main.validate_commodity_raw")
@patch("main.validate_crypto_raw")
@patch("main.fetch_gold")
@patch("main.fetch_crypto")
@patch("main.load_config")
def test_run_pipeline_crypto_failure_gold_success(
    mock_load_config,
    mock_fetch_crypto,
    mock_fetch_gold,
    mock_validate_crypto,
    mock_validate_commodity,
    mock_normalize_crypto,
    mock_normalize_commodity,
):
    """Pipeline returns False when crypto fails but gold succeeds (partial failure)."""
    from main import run_pipeline

    config = _make_config()
    mock_load_config.return_value = config
    mock_fetch_crypto.side_effect = Exception("CoinGecko down")
    mock_fetch_gold.return_value = {"gold_price_usd": 2000.0}
    mock_normalize_commodity.return_value = _make_record("GOLD")

    result = run_pipeline()

    assert result is False  # has_errors is True


@patch("main.fetch_gold")
@patch("main.fetch_crypto")
@patch("main.load_config")
def test_run_pipeline_no_records(mock_load_config, mock_fetch_crypto, mock_fetch_gold):
    """Pipeline returns False when all fetches fail (no records)."""
    from main import run_pipeline

    config = _make_config()
    mock_load_config.return_value = config
    mock_fetch_crypto.side_effect = Exception("CoinGecko down")
    mock_fetch_gold.side_effect = Exception("Yahoo Finance down")

    result = run_pipeline()

    assert result is False


@patch("main.load_config")
def test_run_pipeline_config_error(mock_load_config):
    """Pipeline returns False when config loading raises an exception."""
    from main import run_pipeline

    mock_load_config.side_effect = Exception("Missing env var")

    result = run_pipeline()

    assert result is False


def test_init_adapters_none_enabled():
    """init_adapters returns empty list when all adapters disabled."""
    from main import init_adapters

    config = _make_config(enable_csv=False, enable_sqlite=False, enable_sheets=False)
    adapters = init_adapters(config)
    assert adapters == []


def test_init_adapters_csv_only():
    """init_adapters includes only CSVStore when enable_csv=True."""
    from main import init_adapters
    from src.storage.csv_store import CSVStore

    config = _make_config(enable_csv=True, enable_sqlite=False, enable_sheets=False)
    adapters = init_adapters(config)
    assert len(adapters) == 1
    assert isinstance(adapters[0], CSVStore)


def test_init_adapters_sqlite_only():
    """init_adapters includes only SQLiteStore when enable_sqlite=True."""
    from main import init_adapters
    from src.storage.sqlite_store import SQLiteStore

    config = _make_config(enable_csv=False, enable_sqlite=True, enable_sheets=False)
    adapters = init_adapters(config)
    assert len(adapters) == 1
    assert isinstance(adapters[0], SQLiteStore)


def test_init_adapters_sheets_only():
    """init_adapters includes SheetsStore when enable_sheets=True."""
    from main import init_adapters
    from src.storage.sheets import SheetsStore

    config = _make_config(enable_csv=False, enable_sqlite=False, enable_sheets=True)
    adapters = init_adapters(config)
    assert len(adapters) == 1
    assert isinstance(adapters[0], SheetsStore)


@patch("main.normalize_commodity")
@patch("main.normalize_crypto")
@patch("main.validate_commodity_raw")
@patch("main.validate_crypto_raw")
@patch("main.fetch_gold")
@patch("main.fetch_crypto")
@patch("main.load_config")
def test_run_pipeline_storage_failure_counted(
    mock_load_config,
    mock_fetch_crypto,
    mock_fetch_gold,
    mock_validate_crypto,
    mock_validate_commodity,
    mock_normalize_crypto,
    mock_normalize_commodity,
):
    """Pipeline returns False when a storage adapter write fails."""
    from main import run_pipeline
    from src.storage.csv_store import CSVStore

    config = _make_config(enable_csv=True, csv_path="/nonexistent_dir/out.csv")
    mock_load_config.return_value = config
    mock_fetch_crypto.return_value = {"btc_price": 60000.0, "eth_price": 3000.0}
    mock_fetch_gold.return_value = {"gold_price_usd": 2000.0}
    mock_normalize_crypto.return_value = [_make_record("BTC")]
    mock_normalize_commodity.return_value = _make_record("GOLD")

    result = run_pipeline()

    # Writing to /nonexistent_dir should fail, making has_errors True
    assert result is False
