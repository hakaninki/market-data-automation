"""Tests for transformers."""

import sqlite3
import tempfile
import os
import pytest
from unittest.mock import patch

from src.transformers.normalizer import (
    normalize_crypto,
    normalize_commodity,
    compute_volatility,
    compute_momentum_proxy,
    compute_daily_delta_usd,
)

def test_normalize_crypto_returns_two_records():
    raw = {
        "btc_price": 60000.0,
        "btc_change_pct_24h": 1.5,
        "btc_high_24h": 61000.0,
        "btc_low_24h": 59000.0,
        "eth_price": 3000.0,
        "eth_change_pct_24h": -0.5,
        "eth_high_24h": 3100.0,
        "eth_low_24h": 2900.0,
    }
    
    # We pass an empty/dummy db path. Since table won't exist, it should return None for momentum and delta
    records = normalize_crypto(raw, "dummy.db", "1.0.0")
    
    assert len(records) == 2
    assert records[0].symbol == "BTC"
    assert records[1].symbol == "ETH"
    assert records[0].momentum_proxy is None
    
def test_compute_volatility():
    vol = compute_volatility(high=110.0, low=90.0, price=100.0)
    assert vol == 20.0  # (110-90)/100 * 100

def test_compute_momentum_proxy_no_db():
    # Because there is no DB, OperationalError is caught and None returned
    proxy = compute_momentum_proxy("dummy_non_existent.db", "BTC", 60000.0)
    assert proxy is None


# ---------------------------------------------------------------------------
# compute_volatility edge cases
# ---------------------------------------------------------------------------

def test_compute_volatility_none_high():
    assert compute_volatility(high=None, low=90.0, price=100.0) is None


def test_compute_volatility_none_low():
    assert compute_volatility(high=110.0, low=None, price=100.0) is None


def test_compute_volatility_zero_price():
    assert compute_volatility(high=110.0, low=90.0, price=0.0) is None


def test_compute_volatility_both_none():
    assert compute_volatility(high=None, low=None, price=100.0) is None


# ---------------------------------------------------------------------------
# compute_momentum_proxy with real SQLite data
# ---------------------------------------------------------------------------

def _make_db_with_prices(db_path: str, symbol: str, prices: list) -> None:
    """Populate a SQLite market_data table with the given prices."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS market_data (
            run_date TEXT, fetched_at TEXT, symbol TEXT, name TEXT,
            price_usd REAL, change_pct_24h REAL, high_24h_usd REAL,
            low_24h_usd REAL, momentum_proxy REAL, volatility_proxy REAL,
            daily_delta_usd REAL, data_source TEXT, pipeline_version TEXT,
            PRIMARY KEY (run_date, symbol)
        )"""
    )
    for i, price in enumerate(prices):
        conn.execute(
            "INSERT INTO market_data (run_date, fetched_at, symbol, name, price_usd, "
            "data_source, pipeline_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"2024-01-{i+1:02d}", f"2024-01-{i+1:02d}T00:00:00", symbol, symbol, price, "test", "1.0.0"),
        )
    conn.commit()
    conn.close()


def test_compute_momentum_proxy_with_enough_history():
    """Returns a float when >= 2 historical records exist."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        _make_db_with_prices(db_path, "BTC", [50000.0, 55000.0, 60000.0])
        result = compute_momentum_proxy(db_path, "BTC", 65000.0)
    assert result is not None
    assert isinstance(result, float)


def test_compute_momentum_proxy_insufficient_history():
    """Returns None when only one historical record exists."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        _make_db_with_prices(db_path, "BTC", [50000.0])
        result = compute_momentum_proxy(db_path, "BTC", 55000.0)
    assert result is None


def test_compute_momentum_proxy_zero_mean():
    """Returns None when the historical mean is zero (avoids division by zero)."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        _make_db_with_prices(db_path, "BTC", [0.0, 0.0, 0.0])
        result = compute_momentum_proxy(db_path, "BTC", 1000.0)
    assert result is None


# ---------------------------------------------------------------------------
# compute_daily_delta_usd
# ---------------------------------------------------------------------------

def test_compute_daily_delta_usd_with_yesterday():
    """Returns today_price - yesterday_price when yesterday's record exists."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute(
            """CREATE TABLE market_data (
                run_date TEXT, fetched_at TEXT, symbol TEXT, name TEXT,
                price_usd REAL, change_pct_24h REAL, high_24h_usd REAL,
                low_24h_usd REAL, momentum_proxy REAL, volatility_proxy REAL,
                daily_delta_usd REAL, data_source TEXT, pipeline_version TEXT,
                PRIMARY KEY (run_date, symbol)
            )"""
        )
        conn.execute(
            "INSERT INTO market_data (run_date, fetched_at, symbol, name, price_usd, "
            "data_source, pipeline_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("2024-01-01", "2024-01-01T00:00:00", "BTC", "Bitcoin", 50000.0, "test", "1.0.0"),
        )
        conn.commit()
        conn.close()

        delta = compute_daily_delta_usd(db_path, "BTC", "2024-01-02", 52000.0)

    assert delta == pytest.approx(2000.0)


def test_compute_daily_delta_usd_no_yesterday():
    """Returns None when no yesterday record exists."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        delta = compute_daily_delta_usd(db_path, "BTC", "2024-01-01", 50000.0)
    assert delta is None


def test_compute_daily_delta_usd_no_table():
    """Returns None gracefully when market_data table does not exist yet."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "empty.db")
        sqlite3.connect(db_path).close()  # empty DB, no tables
        delta = compute_daily_delta_usd(db_path, "BTC", "2024-01-02", 50000.0)
    assert delta is None


# ---------------------------------------------------------------------------
# normalize_commodity
# ---------------------------------------------------------------------------

def test_normalize_commodity_returns_gold_record():
    """normalize_commodity produces a GOLD MarketRecord."""
    raw = {"gold_price_usd": 2050.0, "gold_change_pct_24h": None}
    record = normalize_commodity(raw, "dummy.db", "1.0.0")

    assert record.symbol == "GOLD"
    assert record.name == "Gold"
    assert record.price_usd == pytest.approx(2050.0)
    assert record.data_source == "yfinance"
    assert record.pipeline_version == "1.0.0"
    assert record.high_24h_usd is None
    assert record.low_24h_usd is None
    assert record.volatility_proxy is None


def test_normalize_commodity_fields_populated():
    """normalize_commodity sets run_date and fetched_at."""
    raw = {"gold_price_usd": 1900.0, "gold_change_pct_24h": 0.3}
    record = normalize_commodity(raw, "dummy.db", "2.0.0")

    assert record.run_date is not None
    assert record.fetched_at is not None
    assert record.pipeline_version == "2.0.0"


# ---------------------------------------------------------------------------
# normalize_crypto with historical data (momentum + delta populated)
# ---------------------------------------------------------------------------

def test_normalize_crypto_computes_metrics_from_history():
    """When historical data exists, normalize_crypto computes non-None metrics."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        # Seed with several BTC and ETH records
        _make_db_with_prices(db_path, "BTC", [58000.0, 59000.0, 60000.0])
        _make_db_with_prices(db_path, "ETH", [2900.0, 2950.0, 3000.0])

        raw = {
            "btc_price": 62000.0,
            "btc_change_pct_24h": 2.0,
            "btc_high_24h": 63000.0,
            "btc_low_24h": 61000.0,
            "eth_price": 3100.0,
            "eth_change_pct_24h": 1.0,
            "eth_high_24h": 3150.0,
            "eth_low_24h": 3050.0,
        }
        records = normalize_crypto(raw, db_path, "1.0.0")

    assert len(records) == 2
    btc = records[0]
    assert btc.momentum_proxy is not None
    assert btc.volatility_proxy is not None


# ---------------------------------------------------------------------------
# Generic exception handler paths in normalizer (lines 52-54 and 87-89)
# ---------------------------------------------------------------------------

def test_compute_momentum_proxy_generic_exception_returns_none():
    """A non-OperationalError exception in compute_momentum_proxy returns None."""
    with patch("src.transformers.normalizer.sqlite3.connect", side_effect=ValueError("unexpected")):
        result = compute_momentum_proxy("/any/path.db", "BTC", 60000.0)
    assert result is None


def test_compute_daily_delta_usd_generic_exception_returns_none():
    """A non-OperationalError exception in compute_daily_delta_usd returns None."""
    with patch("src.transformers.normalizer.sqlite3.connect", side_effect=ValueError("unexpected")):
        result = compute_daily_delta_usd("/any/path.db", "BTC", "2024-01-02", 60000.0)
    assert result is None
