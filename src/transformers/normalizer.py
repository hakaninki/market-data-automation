"""Data normalization and metric computation for market data."""

import datetime
import sqlite3
from typing import List, Optional

from src.models import MarketRecord
from src.utils.logger import get_logger

logger = get_logger(__name__)


def compute_volatility(
    high: Optional[float], low: Optional[float], price: float
) -> Optional[float]:
    """Compute 24h volatility proxy: (high-low)/price*100."""
    if high is None or low is None or price <= 0:
        return None
    return (high - low) / price * 100.0


def compute_momentum_proxy(db_path: str, symbol: str, today_price: float) -> Optional[float]:
    """
    Compute momentum proxy: % difference from 7-day rolling mean.
    Requires at least 2 historical records in the 7 days prior to today.
    """
    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT price_usd FROM market_data WHERE symbol = ? ORDER BY run_date DESC LIMIT 7",
                (symbol,)
            )
            rows = cursor.fetchall()
        finally:
            conn.close()
            
        if len(rows) < 2:
            return None
            
        history = [r[0] for r in rows]
        mean_price = sum(history) / len(history)
        
        if mean_price == 0:
            return None
            
        return (today_price - mean_price) / mean_price * 100.0
    except sqlite3.OperationalError:
        # Table might not exist yet
        return None
    except Exception as e:
        logger.warning(f"Failed to compute momentum proxy for {symbol}: {e}")
        return None


def compute_daily_delta_usd(
    db_path: str, symbol: str, run_date: str, today_price: float
) -> Optional[float]:
    """
    Compute daily delta USD: today price - yesterday price.
    Returns None if yesterday's record does not exist.
    """
    try:
        today_obj = datetime.datetime.strptime(run_date, "%Y-%m-%d")
        yesterday_str = (today_obj - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT price_usd FROM market_data WHERE symbol = ? AND run_date = ?",
                (symbol, yesterday_str)
            )
            row = cursor.fetchone()
        finally:
            conn.close()
            
        if row is None:
            return None
            
        yesterday_price = row[0]
        return today_price - yesterday_price
    except sqlite3.OperationalError:
        # Table might not exist yet
        return None
    except Exception as e:
        logger.warning(f"Failed to compute daily delta for {symbol}: {e}")
        return None


def normalize_crypto(
    raw: dict, db_path: str, pipeline_version: str = "1.0.0"
) -> List[MarketRecord]:
    """
    Produce one MarketRecord for BTC and one for ETH from raw crypto data.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    run_date = now.strftime("%Y-%m-%d")
    fetched_at = now.isoformat()

    records = []

    # Process BTC
    btc_price = raw.get("btc_price", 0.0)
    btc_high = raw.get("btc_high_24h")
    btc_low = raw.get("btc_low_24h")

    btc_momentum = compute_momentum_proxy(db_path, "BTC", btc_price)
    btc_delta = compute_daily_delta_usd(db_path, "BTC", run_date, btc_price)
    btc_volatility = compute_volatility(btc_high, btc_low, btc_price)

    btc_record = MarketRecord(
        run_date=run_date,
        fetched_at=fetched_at,
        symbol="BTC",
        name="Bitcoin",
        price_usd=btc_price,
        change_pct_24h=raw.get("btc_change_pct_24h"),
        high_24h_usd=btc_high,
        low_24h_usd=btc_low,
        momentum_proxy=btc_momentum,
        volatility_proxy=btc_volatility,
        daily_delta_usd=btc_delta,
        data_source="coingecko",
        pipeline_version=pipeline_version,
    )
    records.append(btc_record)

    # Process ETH
    eth_price = raw.get("eth_price", 0.0)
    eth_high = raw.get("eth_high_24h")
    eth_low = raw.get("eth_low_24h")

    eth_momentum = compute_momentum_proxy(db_path, "ETH", eth_price)
    eth_delta = compute_daily_delta_usd(db_path, "ETH", run_date, eth_price)
    eth_volatility = compute_volatility(eth_high, eth_low, eth_price)

    eth_record = MarketRecord(
        run_date=run_date,
        fetched_at=fetched_at,
        symbol="ETH",
        name="Ethereum",
        price_usd=eth_price,
        change_pct_24h=raw.get("eth_change_pct_24h"),
        high_24h_usd=eth_high,
        low_24h_usd=eth_low,
        momentum_proxy=eth_momentum,
        volatility_proxy=eth_volatility,
        daily_delta_usd=eth_delta,
        data_source="coingecko",
        pipeline_version=pipeline_version,
    )
    records.append(eth_record)

    return records


def normalize_commodity(raw: dict, db_path: str, pipeline_version: str = "1.0.0") -> MarketRecord:
    """
    Produce one MarketRecord for GOLD.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    run_date = now.strftime("%Y-%m-%d")
    fetched_at = now.isoformat()

    gold_price = raw.get("gold_price_usd", 0.0)

    gold_momentum = compute_momentum_proxy(db_path, "GOLD", gold_price)
    gold_delta = compute_daily_delta_usd(db_path, "GOLD", run_date, gold_price)

    return MarketRecord(
        run_date=run_date,
        fetched_at=fetched_at,
        symbol="GOLD",
        name="Gold",
        price_usd=gold_price,
        change_pct_24h=raw.get("gold_change_pct_24h"),
        high_24h_usd=None,
        low_24h_usd=None,
        momentum_proxy=gold_momentum,
        volatility_proxy=None,
        daily_delta_usd=gold_delta,
        data_source="yfinance",
        pipeline_version=pipeline_version,
    )
