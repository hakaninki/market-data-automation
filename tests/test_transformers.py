"""Tests for transformers."""

from src.transformers.normalizer import normalize_crypto, compute_volatility, compute_momentum_proxy

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
