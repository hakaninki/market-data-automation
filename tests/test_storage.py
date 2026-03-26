"""Tests for storage adapters."""

import os
import sqlite3
import tempfile
import pytest

from src.models import MarketRecord
from src.storage.csv_store import CSVStore
from src.storage.sqlite_store import SQLiteStore

@pytest.fixture
def dummy_record():
    return MarketRecord(
        run_date="2024-01-01",
        fetched_at="2024-01-01T08:00:00",
        symbol="BTC",
        name="Bitcoin",
        price_usd=50000.0,
        change_pct_24h=1.0,
        high_24h_usd=51000.0,
        low_24h_usd=49000.0,
        momentum_proxy=None,
        volatility_proxy=None,
        daily_delta_usd=None,
        data_source="test",
        pipeline_version="1.0.0"
    )

def test_csv_store(dummy_record):
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, "test.csv")
        store = CSVStore(csv_path)
        
        # Write once
        store.write([dummy_record])
        assert os.path.exists(csv_path)
        
        with open(csv_path, 'r') as f:
            lines = f.readlines()
            # 1 header + 1 row
            assert len(lines) == 2 
            
        # Write again (duplicate)
        store.write([dummy_record])
        
        with open(csv_path, 'r') as f:
            lines = f.readlines()
            # Should still be 2 due to deduplication
            assert len(lines) == 2 


def test_sqlite_store(dummy_record):
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test.db")
        store = SQLiteStore(db_path)
        
        # Write once
        store.write([dummy_record])
        
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM market_data")
            count = cursor.fetchone()[0]
            assert count == 1
        finally:
            conn.close()
            
        # Write again (duplicate)
        store.write([dummy_record])
        
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM market_data")
            count = cursor.fetchone()[0]
            # Deduplicated by INSERT OR IGNORE
            assert count == 1 
        finally:
            conn.close()
