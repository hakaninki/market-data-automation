"""Yahoo Finance client for commodity market data."""

from typing import Any, Dict

import yfinance as yf

from src.utils.logger import get_logger

logger = get_logger(__name__)

GOLD_TICKER = "GC=F"


class FetchError(Exception):
    """Custom exception for fetching errors."""

    pass


def fetch_gold() -> Dict[str, Any]:
    """
    Fetch current market data for Gold from Yahoo Finance.

    Returns a dictionary structured as:
    {
        "gold_price_usd": float,
        "gold_change_pct_24h": None
    }
    """
    logger.info("Fetching gold prices from Yahoo Finance")

    try:
        df = yf.download(GOLD_TICKER, period="2d", interval="1d", progress=False)

        if df is None or df.empty:
            raise FetchError(f"No data returned for ticker {GOLD_TICKER} (market may be closed)")

        # Extract the scalar price correctly regardless of pandas return structures
        # Use simple cast after obtaining the latest item
        close_series = df["Close"]
        latest_close = float(
            close_series.iloc[-1].item()
            if hasattr(close_series.iloc[-1], "item")
            else close_series.iloc[-1]
        )

        return {"gold_price_usd": latest_close, "gold_change_pct_24h": None}
    except FetchError:
        raise
    except Exception as e:
        logger.error(f"Error fetching gold data from Yahoo Finance: {e}")
        raise FetchError(f"Failed to fetch gold data: {e}") from e
