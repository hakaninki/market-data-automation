"""Data models for market data."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class MarketRecord:
    """Core domain model representing a single market data observation."""

    run_date: str  # ISO date YYYY-MM-DD UTC
    fetched_at: str  # ISO datetime UTC
    symbol: str  # BTC | ETH | GOLD
    name: str  # Bitcoin | Ethereum | Gold
    price_usd: float
    change_pct_24h: Optional[float]
    high_24h_usd: Optional[float]
    low_24h_usd: Optional[float]
    momentum_proxy: Optional[float]  # % diff from 7d rolling mean
    volatility_proxy: Optional[float]  # (high-low)/price*100
    daily_delta_usd: Optional[float]  # today price - yesterday price
    data_source: str  # coingecko | yfinance
    pipeline_version: str  # e.g. 1.0.0
