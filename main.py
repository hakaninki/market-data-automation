"""
Main pipeline orchestration for the market-data-automation project.
Fetches, validates, normalizes, and stores daily market data.
"""

import sys
from typing import List

from src.config import load_config, Config
from src.utils.logger import get_logger
from src.models import MarketRecord
from src.fetchers.coingecko import fetch_crypto
from src.fetchers.commodities import fetch_gold
from src.validators.schema import validate_crypto_raw, validate_commodity_raw, ValidationError
from src.transformers.normalizer import normalize_crypto, normalize_commodity
from src.storage.csv_store import CSVStore
from src.storage.sqlite_store import SQLiteStore
from src.storage.sheets import SheetsStore

logger = get_logger(__name__)


def init_adapters(config: Config) -> List:
    """Initialize and return a list of enabled storage adapters."""
    adapters = []
    
    if config.enable_csv:
        adapters.append(CSVStore(config.csv_path))
    
    if config.enable_sqlite:
        adapters.append(SQLiteStore(config.sqlite_path))
        
    if config.enable_sheets:
        adapters.append(SheetsStore(config.google_credentials_json, config.google_sheet_id))
        
    return adapters


def run_pipeline() -> bool:
    """Execute the main data pipeline end-to-end. Returns True if completely successful."""
    try:
        config = load_config()
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return False
        
    logger.info(f"Starting pipeline (version {config.pipeline_version}) in UTC")
    
    try:
        adapters = init_adapters(config)
    except Exception as e:
        logger.error(f"Failed to initialize storage adapters: {e}")
        return False
        
    if not adapters:
        logger.warning("No storage adapters enabled. Pipeline will fetch data but not save it.")
        
    all_records: List[MarketRecord] = []
    has_errors = False
    
    # 1. Fetch & Process Crypto (BTC, ETH)
    try:
        raw_crypto = fetch_crypto(config.coingecko_api_key)
        validate_crypto_raw(raw_crypto)
        crypto_records = normalize_crypto(raw_crypto, config.sqlite_path, config.pipeline_version)
        all_records.extend(crypto_records)
        logger.info(f"Successfully processed {len(crypto_records)} crypto records.")
    except Exception as e:
        logger.error(f"Crypto processing failed: {e}")
        has_errors = True
        
    # 2. Fetch & Process Commodities (Gold)
    try:
        raw_gold = fetch_gold()
        validate_commodity_raw(raw_gold)
        gold_record = normalize_commodity(raw_gold, config.sqlite_path, config.pipeline_version)
        all_records.append(gold_record)
        logger.info("Successfully processed Gold record.")
    except Exception as e:
        logger.error(f"Gold processing failed: {e}")
        has_errors = True
        
    # 3. Store Data
    if not all_records:
        logger.error("No records successfully processed. Exiting pipeline.")
        return False
        
    storage_failures = 0
    for adapter in adapters:
        try:
            logger.info(f"Writing data using {adapter.__class__.__name__}...")
            adapter.write(all_records)
        except Exception as e:
            logger.error(f"{adapter.__class__.__name__} write failed: {e}")
            storage_failures += 1
            has_errors = True
            
    # Final Summary
    total_adapters = len(adapters)
    success_adapters = total_adapters - storage_failures
    
    logger.info(
        f"Pipeline finished. Processed {len(all_records)} total records from API. "
        f"Storage adapters: {success_adapters}/{total_adapters} succeeded."
    )
    
    return not has_errors


if __name__ == "__main__":
    success = run_pipeline()
    if not success:
        sys.exit(1)
    sys.exit(0)
