"""Tests for raw data validators."""

import pytest
from src.validators.schema import validate_crypto_raw, validate_commodity_raw, ValidationError

def test_validate_crypto_raw_valid():
    raw = {"btc_price": 60000.0, "eth_price": 3000.0}
    # Should not raise exception
    validate_crypto_raw(raw)

def test_validate_crypto_raw_missing():
    raw = {"btc_price": 60000.0}
    with pytest.raises(ValidationError, match="Missing price field"):
        validate_crypto_raw(raw)

def test_validate_crypto_raw_non_numeric():
    raw = {"btc_price": "60000", "eth_price": 3000.0}
    with pytest.raises(ValidationError, match="Invalid price type"):
        validate_crypto_raw(raw)

def test_validate_commodity_raw_valid():
    raw = {"gold_price_usd": 2000.0}
    validate_commodity_raw(raw)

def test_validate_commodity_raw_missing():
    raw = {}
    with pytest.raises(ValidationError, match="Missing required field for Gold"):
        validate_commodity_raw(raw)
