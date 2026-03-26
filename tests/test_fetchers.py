"""Tests for data fetchers."""

import pytest
from unittest.mock import patch, MagicMock
from httpx import HTTPStatusError, RequestError

from src.fetchers.coingecko import fetch_crypto
from src.fetchers.commodities import fetch_gold, FetchError


def test_fetch_crypto_success():
    mock_data = {
        "bitcoin": {
            "usd": 60000.0,
            "usd_24h_change": 1.5,
            "usd_24h_high": 61000.0,
            "usd_24h_low": 59000.0,
        },
        "ethereum": {
            "usd": 3000.0,
            "usd_24h_change": -0.5,
            "usd_24h_high": 3100.0,
            "usd_24h_low": 2900.0,
        }
    }
    
    with patch("src.fetchers.coingecko.httpx.Client.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_data
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp
        
        result = fetch_crypto()
        
        assert result["btc_price"] == 60000.0
        assert result["eth_price"] == 3000.0
        assert "btc_change_pct_24h" in result
        assert "eth_high_24h" in result


def test_fetch_crypto_retry_behavior():
    with patch("src.fetchers.coingecko.httpx.Client.get") as mock_get:
        # Create a mock error response for 429
        mock_error_resp = MagicMock()
        mock_error_resp.status_code = 429
        error = HTTPStatusError("429 Too Many Requests", request=MagicMock(), response=mock_error_resp)
        
        mock_success_resp = MagicMock()
        mock_success_resp.json.return_value = {
            "bitcoin": {"usd": 50000}, 
            "ethereum": {"usd": 2000}
        }
        mock_success_resp.raise_for_status.return_value = None
        
        mock_get.side_effect = [error, error, mock_success_resp]
        
        # Patch sleep to avoid waiting during tests
        with patch("tenacity.nap.time.sleep"):
            result = fetch_crypto()
            
            assert result["btc_price"] == 50000.0
            assert mock_get.call_count == 3
