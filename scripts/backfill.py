"""
Backfill script placeholder.
Can be expanded in the future to fetch historical data and populate the database.
"""

from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_backfill():
    logger.info("Starting backfill logic (not fully implemented yet)...")
    logger.info("This script is a placeholder for historical data backfill functionality.")
    # In a full implementation, you would:
    # 1. Loop through historical dates
    # 2. Call CoinGecko / yfinance historical endpoints
    # 3. Validate, Normalize
    # 4. Save to DB
    logger.info("Backfill completed.")


if __name__ == "__main__":
    run_backfill()
