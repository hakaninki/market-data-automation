"""SQLite storage adapter."""

import sqlite3
from typing import List

from src.models import MarketRecord
from src.storage.base import StorageAdapter
from src.utils.logger import get_logger

logger = get_logger(__name__)

DDL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS market_data (
    run_date TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    price_usd REAL NOT NULL,
    change_pct_24h REAL,
    high_24h_usd REAL,
    low_24h_usd REAL,
    momentum_proxy REAL,
    volatility_proxy REAL,
    daily_delta_usd REAL,
    data_source TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    PRIMARY KEY (run_date, symbol)
);
"""


class SQLiteStore(StorageAdapter):
    """Stores market data records in an SQLite database."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def write(self, records: List[MarketRecord]) -> None:
        if not records:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.cursor()
                cursor.execute(DDL_CREATE_TABLE)

                # Use INSERT OR IGNORE for deduplication based on PRIMARY KEY
                insert_stmt = """
                INSERT OR IGNORE INTO market_data (
                    run_date, fetched_at, symbol, name, price_usd,
                    change_pct_24h, high_24h_usd, low_24h_usd,
                    momentum_proxy, volatility_proxy, daily_delta_usd,
                    data_source, pipeline_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                rows = [
                    (
                        r.run_date,
                        r.fetched_at,
                        r.symbol,
                        r.name,
                        r.price_usd,
                        r.change_pct_24h,
                        r.high_24h_usd,
                        r.low_24h_usd,
                        r.momentum_proxy,
                        r.volatility_proxy,
                        r.daily_delta_usd,
                        r.data_source,
                        r.pipeline_version,
                    )
                    for r in records
                ]

                cursor.executemany(insert_stmt, rows)
                conn.commit()
            finally:
                conn.close()

            logger.info(
                    f"Wrote {cursor.rowcount} new records to SQLite "
                    f"({len(records) - cursor.rowcount} skipped)."
                )
        except Exception as e:
            logger.error(f"Failed to write to SQLite: {e}")
            raise
