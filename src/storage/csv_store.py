"""CSV storage adapter."""

import csv
import os
from typing import List

from src.models import MarketRecord
from src.storage.base import StorageAdapter
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CSVStore(StorageAdapter):
    """Stores market data records in a CSV file."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def write(self, records: List[MarketRecord]) -> None:
        if not records:
            return

        file_exists = os.path.exists(self.file_path)

        # Determine existing combinations of (run_date, symbol) to avoid duplicates
        existing_keys = set()
        if file_exists:
            try:
                with open(self.file_path, mode="r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_keys.add((row.get("run_date"), row.get("symbol")))
            except Exception as e:
                logger.warning(f"Failed to read existing CSV for deduplication: {e}")

        rows_to_write = []
        skipped = 0
        for record in records:
            key = (record.run_date, record.symbol)
            if key in existing_keys:
                skipped += 1
                logger.debug(f"Skipping duplicate record in CSV: {key}")
                continue

            row_dict = {
                "run_date": record.run_date,
                "fetched_at": record.fetched_at,
                "symbol": record.symbol,
                "name": record.name,
                "price_usd": record.price_usd,
                "change_pct_24h": (
                    record.change_pct_24h if record.change_pct_24h is not None else ""
                ),
                "high_24h_usd": record.high_24h_usd if record.high_24h_usd is not None else "",
                "low_24h_usd": record.low_24h_usd if record.low_24h_usd is not None else "",
                "momentum_proxy": (
                    record.momentum_proxy if record.momentum_proxy is not None else ""
                ),
                "volatility_proxy": (
                    record.volatility_proxy if record.volatility_proxy is not None else ""
                ),
                "daily_delta_usd": (
                    record.daily_delta_usd if record.daily_delta_usd is not None else ""
                ),
                "data_source": record.data_source,
                "pipeline_version": record.pipeline_version,
            }
            rows_to_write.append(row_dict)

        if not rows_to_write:
            logger.info(f"No new records to write to CSV ({skipped} skipped).")
            return

        fieldnames = list(rows_to_write[0].keys())

        try:
            with open(self.file_path, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(rows_to_write)
            logger.info(f"Wrote {len(rows_to_write)} records to CSV ({skipped} skipped).")
        except Exception as e:
            logger.error(f"Failed to write to CSV: {e}")
            raise
