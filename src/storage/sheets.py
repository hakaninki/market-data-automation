"""Google Sheets storage adapter."""

import json
import os
from typing import List

import gspread
from google.oauth2.service_account import Credentials

from src.models import MarketRecord
from src.storage.base import StorageAdapter
from src.utils.logger import get_logger

logger = get_logger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


class SheetsStore(StorageAdapter):
    """Stores market data records in a Google Sheet."""

    def __init__(self, credentials_json: str, sheet_id: str):
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id

    def _authenticate(self) -> gspread.client.Client:
        """Authenticate with Google APIs."""
        if os.path.isfile(self.credentials_json):
            creds = Credentials.from_service_account_file(self.credentials_json, scopes=SCOPES)
        else:
            try:
                # Attempt to parse as JSON string
                creds_dict = json.loads(self.credentials_json)
                creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"credentials_json must be a valid file path or JSON string. Error: {e}"
                )

        return gspread.authorize(creds)

    def write(self, records: List[MarketRecord]) -> None:
        if not records:
            return

        try:
            client = self._authenticate()
            sheet = client.open_by_key(self.sheet_id).sheet1

            # Check for header and duplicates
            existing_data = sheet.get_all_values()

            fieldnames = [
                "run_date",
                "fetched_at",
                "symbol",
                "name",
                "price_usd",
                "change_pct_24h",
                "high_24h_usd",
                "low_24h_usd",
                "momentum_proxy",
                "volatility_proxy",
                "daily_delta_usd",
                "data_source",
                "pipeline_version",
            ]

            existing_keys = set()
            if not existing_data:
                sheet.append_row(fieldnames)
                existing_data = [fieldnames]  # Initialize to correctly skip header

            for row in existing_data:
                # Only evaluate if row has data and is not the header row
                if not row or str(row[0]).strip() == "run_date":
                    continue
                if len(row) >= 3:
                    # Deduplication uses (run_date, symbol) as composite key
                    run_date_val = str(row[0]).strip()
                    symbol_val = str(row[2]).strip()
                    existing_keys.add((run_date_val, symbol_val))

            rows_to_write = []
            skipped = 0

            for record in records:
                # Same date must allow multiple symbols, only skip if BOTH match
                rec_run_date = str(record.run_date).strip()
                rec_symbol = str(record.symbol).strip()
                composite_key = (rec_run_date, rec_symbol)

                if composite_key in existing_keys:
                    skipped += 1
                    logger.warning(f"Skipping duplicate record in Sheets: {composite_key}")
                    continue

                row_data = [
                    rec_run_date,
                    str(record.fetched_at),
                    rec_symbol,
                    str(record.name),
                    record.price_usd,
                    record.change_pct_24h if record.change_pct_24h is not None else "",
                    record.high_24h_usd if record.high_24h_usd is not None else "",
                    record.low_24h_usd if record.low_24h_usd is not None else "",
                    record.momentum_proxy if record.momentum_proxy is not None else "",
                    record.volatility_proxy if record.volatility_proxy is not None else "",
                    record.daily_delta_usd if record.daily_delta_usd is not None else "",
                    str(record.data_source),
                    str(record.pipeline_version),
                ]
                rows_to_write.append(row_data)

            if rows_to_write:
                # Use append_rows to write vertically under existing data
                sheet.append_rows(rows_to_write)
                logger.info(f"Wrote {len(rows_to_write)} records to Sheets ({skipped} skipped).")
            else:
                logger.info(f"No new records to write to Sheets ({skipped} skipped).")

        except Exception as e:
            logger.error(f"Failed to write to Google Sheets: {e}")
            # Exception caught and logged per requirements, do not abort
