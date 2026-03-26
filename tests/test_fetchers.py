"""Tests for data fetchers."""

import pandas as pd
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


def test_fetch_crypto_with_api_key_sets_header():
    """When api_key is provided the x-cg-demo-api-key header should be sent."""
    mock_data = {
        "bitcoin": {"usd": 60000.0, "usd_24h_change": 0.0, "usd_24h_high": 60500.0, "usd_24h_low": 59500.0},
        "ethereum": {"usd": 3000.0, "usd_24h_change": 0.0, "usd_24h_high": 3050.0, "usd_24h_low": 2950.0},
    }

    with patch("src.fetchers.coingecko.httpx.Client") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=mock_instance)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_data
        mock_resp.raise_for_status.return_value = None
        mock_instance.get.return_value = mock_resp

        result = fetch_crypto(api_key="test-key-123")

        call_kwargs = mock_instance.get.call_args
        assert call_kwargs is not None
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert headers.get("x-cg-demo-api-key") == "test-key-123"
        assert result["btc_price"] == 60000.0


def test_fetch_crypto_all_fields_mapped():
    """All eight expected keys are present in the returned dict."""
    mock_data = {
        "bitcoin": {"usd": 60000.0, "usd_24h_change": 1.5, "usd_24h_high": 61000.0, "usd_24h_low": 59000.0},
        "ethereum": {"usd": 3000.0, "usd_24h_change": -0.5, "usd_24h_high": 3100.0, "usd_24h_low": 2900.0},
    }

    with patch("src.fetchers.coingecko.httpx.Client.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_data
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_crypto()

    expected_keys = {
        "btc_price", "btc_change_pct_24h", "btc_high_24h", "btc_low_24h",
        "eth_price", "eth_change_pct_24h", "eth_high_24h", "eth_low_24h",
    }
    assert expected_keys == set(result.keys())


def test_fetch_crypto_missing_coin_returns_none_fields():
    """If CoinGecko omits a coin the corresponding fields are None."""
    mock_data = {"bitcoin": {"usd": 60000.0}, "ethereum": {}}

    with patch("src.fetchers.coingecko.httpx.Client.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_data
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_crypto()

    assert result["eth_price"] is None
    assert result["eth_change_pct_24h"] is None


# ---------------------------------------------------------------------------
# fetch_gold tests
# ---------------------------------------------------------------------------

def _make_gold_df(price: float) -> pd.DataFrame:
    """Helper: build a minimal DataFrame that mimics yfinance output."""
    return pd.DataFrame({"Close": [price - 5, price]})


def test_fetch_gold_success():
    """fetch_gold returns correct price from a non-empty DataFrame."""
    with patch("src.fetchers.commodities.yf.download", return_value=_make_gold_df(2050.0)):
        result = fetch_gold()

    assert result["gold_price_usd"] == pytest.approx(2050.0)
    assert result["gold_change_pct_24h"] is None


def test_fetch_gold_empty_dataframe_raises_fetch_error():
    """fetch_gold raises FetchError when yfinance returns an empty DataFrame."""
    with patch("src.fetchers.commodities.yf.download", return_value=pd.DataFrame()):
        with pytest.raises(FetchError, match="No data returned"):
            fetch_gold()


def test_fetch_gold_none_result_raises_fetch_error():
    """fetch_gold raises FetchError when yfinance returns None."""
    with patch("src.fetchers.commodities.yf.download", return_value=None):
        with pytest.raises(FetchError):
            fetch_gold()


def test_fetch_gold_yfinance_exception_raises_fetch_error():
    """Unexpected exceptions from yfinance are wrapped in FetchError."""
    with patch("src.fetchers.commodities.yf.download", side_effect=RuntimeError("network error")):
        with pytest.raises(FetchError, match="Failed to fetch gold data"):
            fetch_gold()


def test_fetch_crypto_parse_error_retries_then_raises():
    """fetch_crypto raises after tenacity exhausts retries when the JSON response cannot be parsed."""
    import tenacity

    bad_data = MagicMock()
    bad_data.get.side_effect = TypeError("unexpected structure")

    with patch("src.fetchers.coingecko.httpx.Client.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = bad_data
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        with patch("tenacity.nap.time.sleep"):
            with pytest.raises((ValueError, TypeError, tenacity.RetryError)):
                fetch_crypto()
