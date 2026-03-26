"""Tests for storage adapters."""

import csv
import json
import os
import sqlite3
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from src.models import MarketRecord
from src.storage.csv_store import CSVStore
from src.storage.sqlite_store import SQLiteStore
from src.storage.sheets import SheetsStore

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


# ---------------------------------------------------------------------------
# CSVStore edge cases
# ---------------------------------------------------------------------------

def test_csv_store_empty_records_is_noop(dummy_record):
    """Writing an empty list should not create a file."""
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "out.csv")
        store = CSVStore(csv_path)
        store.write([])
        assert not os.path.exists(csv_path)


def test_csv_store_none_optional_fields():
    """Optional None fields are stored as empty strings in CSV."""
    record = MarketRecord(
        run_date="2024-06-01",
        fetched_at="2024-06-01T10:00:00",
        symbol="GOLD",
        name="Gold",
        price_usd=2000.0,
        change_pct_24h=None,
        high_24h_usd=None,
        low_24h_usd=None,
        momentum_proxy=None,
        volatility_proxy=None,
        daily_delta_usd=None,
        data_source="yfinance",
        pipeline_version="1.0.0",
    )
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "out.csv")
        store = CSVStore(csv_path)
        store.write([record])

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["change_pct_24h"] == ""
    assert rows[0]["high_24h_usd"] == ""


def test_csv_store_multiple_records(dummy_record):
    """Multiple distinct records are all written."""
    record2 = MarketRecord(
        run_date="2024-01-02",
        fetched_at="2024-01-02T08:00:00",
        symbol="ETH",
        name="Ethereum",
        price_usd=3000.0,
        change_pct_24h=-0.5,
        high_24h_usd=3100.0,
        low_24h_usd=2900.0,
        momentum_proxy=None,
        volatility_proxy=None,
        daily_delta_usd=None,
        data_source="coingecko",
        pipeline_version="1.0.0",
    )
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "out.csv")
        store = CSVStore(csv_path)
        store.write([dummy_record, record2])

        with open(csv_path, newline="", encoding="utf-8") as f:
            lines = f.readlines()

    # header + 2 data rows
    assert len(lines) == 3


def test_csv_store_write_error_raises(dummy_record):
    """CSVStore.write re-raises exceptions from failed file writes."""
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "out.csv")
        store = CSVStore(csv_path)
        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                store.write([dummy_record])


def test_csv_store_read_error_is_logged_and_continues(dummy_record):
    """When reading existing CSV for deduplication fails, a warning is logged
    and writing still proceeds (no crash)."""
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "out.csv")
        # Create a dummy existing file so file_exists=True
        with open(csv_path, "w") as f:
            f.write("garbage")

        store = CSVStore(csv_path)

        real_open = open

        def patched_open(path, mode="r", **kwargs):
            if mode == "r" and path == csv_path:
                raise OSError("permission denied")
            return real_open(path, mode, **kwargs)

        with patch("builtins.open", side_effect=patched_open):
            # Should not raise — the read error is caught and logged
            store.write([dummy_record])


# ---------------------------------------------------------------------------
# SQLiteStore edge cases
# ---------------------------------------------------------------------------

def test_sqlite_store_empty_records_is_noop():
    """Writing an empty list does not create a database."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store = SQLiteStore(db_path)
        store.write([])
        # DB should not exist because write returned early
        assert not os.path.exists(db_path)


def test_sqlite_store_all_fields_persisted(dummy_record):
    """All columns are written correctly to the database."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store = SQLiteStore(db_path)
        store.write([dummy_record])

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT symbol, price_usd, data_source FROM market_data WHERE symbol='BTC'"
        ).fetchone()
        conn.close()

    assert row[0] == "BTC"
    assert row[1] == pytest.approx(50000.0)
    assert row[2] == "test"


def test_sqlite_store_error_raises(dummy_record):
    """SQLiteStore.write re-raises on connection errors."""
    store = SQLiteStore("/nonexistent_dir/test.db")
    with pytest.raises(Exception):
        store.write([dummy_record])


# ---------------------------------------------------------------------------
# SheetsStore tests (fully mocked — no network required)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_gspread_client():
    """Return a gspread client mock with a usable sheet mock."""
    mock_sheet = MagicMock()
    mock_sheet.get_all_values.return_value = []  # empty sheet → header will be appended

    mock_spreadsheet = MagicMock()
    mock_spreadsheet.sheet1 = mock_sheet

    mock_client = MagicMock()
    mock_client.open_by_key.return_value = mock_spreadsheet

    return mock_client, mock_sheet


def test_sheets_store_write_empty_records():
    """SheetsStore.write returns early when no records provided."""
    store = SheetsStore(credentials_json='{"type":"service_account"}', sheet_id="abc123")
    with patch.object(store, "_authenticate") as mock_auth:
        store.write([])
        mock_auth.assert_not_called()


def test_sheets_store_write_to_empty_sheet(dummy_record, mock_gspread_client):
    """Writes header + row when sheet is empty."""
    mock_client, mock_sheet = mock_gspread_client
    store = SheetsStore(credentials_json='{"type":"service_account"}', sheet_id="sheet1")

    with patch.object(store, "_authenticate", return_value=mock_client):
        store.write([dummy_record])

    # Header row appended (empty sheet)
    mock_sheet.append_row.assert_called_once()
    # Data rows appended
    mock_sheet.append_rows.assert_called_once()
    rows_written = mock_sheet.append_rows.call_args[0][0]
    assert len(rows_written) == 1
    assert rows_written[0][2] == "BTC"  # symbol at index 2


def test_sheets_store_deduplication(dummy_record, mock_gspread_client):
    """Duplicate records (same run_date + symbol) are skipped."""
    mock_client, mock_sheet = mock_gspread_client
    # Sheet already has one row with the same key
    mock_sheet.get_all_values.return_value = [
        ["run_date", "fetched_at", "symbol"],  # header
        ["2024-01-01", "2024-01-01T08:00:00", "BTC"],  # existing duplicate
    ]
    store = SheetsStore(credentials_json='{"type":"service_account"}', sheet_id="sheet1")

    with patch.object(store, "_authenticate", return_value=mock_client):
        store.write([dummy_record])

    # No new rows should be appended
    mock_sheet.append_rows.assert_not_called()


def test_sheets_store_authenticate_json_string():
    """_authenticate parses a JSON string for credentials."""
    creds_dict = {
        "type": "service_account",
        "project_id": "proj",
        "private_key_id": "kid",
        "private_key": "-----BEGIN RSA PRIVATE KEY-----\n-----END RSA PRIVATE KEY-----\n",
        "client_email": "svc@proj.iam.gserviceaccount.com",
        "client_id": "123",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    store = SheetsStore(credentials_json=json.dumps(creds_dict), sheet_id="sid")

    mock_creds = MagicMock()
    with patch("src.storage.sheets.Credentials.from_service_account_info", return_value=mock_creds):
        with patch("src.storage.sheets.gspread.authorize", return_value=MagicMock()):
            store._authenticate()


def test_sheets_store_authenticate_invalid_json_raises():
    """_authenticate raises ValueError for invalid JSON that is not a file path."""
    store = SheetsStore(credentials_json="not-valid-json-not-a-file", sheet_id="sid")
    with pytest.raises(ValueError, match="credentials_json must be a valid file path or JSON string"):
        store._authenticate()


def test_sheets_store_authenticate_file_path(tmp_path):
    """_authenticate reads credentials from a file when a valid path is given."""
    creds_file = tmp_path / "creds.json"
    creds_file.write_text('{"type": "service_account"}')

    store = SheetsStore(credentials_json=str(creds_file), sheet_id="sid")

    mock_creds = MagicMock()
    with patch("src.storage.sheets.Credentials.from_service_account_file", return_value=mock_creds):
        with patch("src.storage.sheets.gspread.authorize", return_value=MagicMock()):
            client = store._authenticate()

    assert client is not None


def test_sheets_store_write_exception_is_swallowed(dummy_record):
    """SheetsStore.write logs errors on failure and does NOT re-raise."""
    store = SheetsStore(credentials_json='{"type":"service_account"}', sheet_id="sheet1")
    with patch.object(store, "_authenticate", side_effect=Exception("auth error")):
        # Should not raise — exception is caught and logged internally
        store.write([dummy_record])
