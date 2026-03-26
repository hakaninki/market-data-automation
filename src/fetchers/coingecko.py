"""CoinGecko API client for cryptocurrency market data."""

from typing import Any, Dict, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

logger = get_logger(__name__)

COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def fetch_crypto(api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch current market data for Bitcoin and Ethereum from CoinGecko.

    Returns a dictionary structured as:
    {
        "btc_price": float,
        "btc_change_pct_24h": float,
        "btc_high_24h": float,
        "btc_low_24h": float,
        ... (same for eth)
    }
    """
    logger.info("Fetching crypto prices from CoinGecko")

    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_24hr_vol": "false",
        "include_high_low": "true",
    }

    headers = {}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    with httpx.Client(timeout=10.0) as client:
        response = client.get(COINGECKO_API_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

    try:
        btc_data = data.get("bitcoin", {})
        eth_data = data.get("ethereum", {})

        return {
            "btc_price": btc_data.get("usd"),
            "btc_change_pct_24h": btc_data.get("usd_24h_change"),
            "btc_high_24h": btc_data.get("usd_24h_high"),
            "btc_low_24h": btc_data.get("usd_24h_low"),
            "eth_price": eth_data.get("usd"),
            "eth_change_pct_24h": eth_data.get("usd_24h_change"),
            "eth_high_24h": eth_data.get("usd_24h_high"),
            "eth_low_24h": eth_data.get("usd_24h_low"),
        }
    except Exception as e:
        logger.error(f"Error parsing CoinGecko response: {e}")
        raise ValueError(f"Unexpected CoinGecko response format: {e}") from e
