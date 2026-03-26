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


def test_validate_crypto_raw_zero_price():
    """Zero price should raise ValidationError (must be > 0)."""
    raw = {"btc_price": 0.0, "eth_price": 3000.0}
    with pytest.raises(ValidationError, match="must be > 0"):
        validate_crypto_raw(raw)


def test_validate_crypto_raw_negative_price():
    """Negative price should raise ValidationError."""
    raw = {"btc_price": -100.0, "eth_price": 3000.0}
    with pytest.raises(ValidationError, match="must be > 0"):
        validate_crypto_raw(raw)


def test_validate_commodity_raw_non_numeric():
    """Non-numeric gold price should raise ValidationError."""
    raw = {"gold_price_usd": "2000"}
    with pytest.raises(ValidationError, match="Invalid price type"):
        validate_commodity_raw(raw)


def test_validate_commodity_raw_zero_price():
    """Zero gold price should raise ValidationError."""
    raw = {"gold_price_usd": 0.0}
    with pytest.raises(ValidationError, match="must be > 0"):
        validate_commodity_raw(raw)


def test_validate_commodity_raw_negative_price():
    """Negative gold price should raise ValidationError."""
    raw = {"gold_price_usd": -500.0}
    with pytest.raises(ValidationError, match="must be > 0"):
        validate_commodity_raw(raw)


def test_validate_crypto_raw_integer_price_accepted():
    """Integer prices are acceptable (isinstance check includes int)."""
    raw = {"btc_price": 60000, "eth_price": 3000}
    # Should not raise
    validate_crypto_raw(raw)


def test_validate_commodity_raw_integer_price_accepted():
    """Integer gold price is acceptable."""
    raw = {"gold_price_usd": 2000}
    validate_commodity_raw(raw)
