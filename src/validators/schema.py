"""Data validation payload schemas."""


class ValidationError(Exception):
    """Custom exception for validation errors."""

    pass


def validate_crypto_raw(raw: dict) -> None:
    """Validate the raw crypto price payload from CoinGecko."""
    for symbol, prefix in [("BTC", "btc"), ("ETH", "eth")]:
        price_key = f"{prefix}_price"
        if price_key not in raw:
            raise ValidationError(f"Missing price field for {symbol}: '{price_key}'")

        value = raw[price_key]
        if not isinstance(value, (int, float)):
            raise ValidationError(
                f"Invalid price type for {symbol}: expected numeric, got {type(value)}"
            )

        if value <= 0:
            raise ValidationError(f"Invalid price for {symbol}: must be > 0, got {value}")


def validate_commodity_raw(raw: dict) -> None:
    """Validate the raw commodity price payload from Yahoo Finance."""
    price_key = "gold_price_usd"
    if price_key not in raw:
        raise ValidationError(f"Missing required field for Gold: '{price_key}'")

    value = raw[price_key]
    if not isinstance(value, (int, float)):
        raise ValidationError(f"Invalid price type for Gold: expected numeric, got {type(value)}")

    if value <= 0:
        raise ValidationError(f"Invalid price for Gold: must be > 0, got {value}")
