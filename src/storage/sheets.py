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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FIELDNAMES = [
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

HEADER_RANGE = "A1:M1"
TABLE_RANGE = "A1:M"


class SheetsStore(StorageAdapter):
    """Stores market data records in a Google Sheet."""

    def __init__(self, credentials_json: str, sheet_id: str):
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id

    def _authenticate(self) -> gspread.client.Client:
        """Authenticate with Google APIs."""
        if os.path.isfile(self.credentials_json):
            creds = Credentials.from_service_account_file(
                self.credentials_json,
                scopes=SCOPES,
            )
        else:
            try:
                creds_dict = json.loads(self.credentials_json)
                creds = Credentials.from_service_account_info(
                    creds_dict,
                    scopes=SCOPES,
                )
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"credentials_json must be a valid file path or JSON string. Error: {e}"
                ) from e

        return gspread.authorize(creds)

    def _ensure_header(self, sheet: gspread.worksheet.Worksheet) -> List[List[str]]:
        """Ensure the sheet has the correct header in row 1."""
        existing_data = sheet.get_all_values()

        if not existing_data:
            sheet.update(HEADER_RANGE, [FIELDNAMES])
            logger.info("Header row created in Google Sheets.")
            return [FIELDNAMES]

        first_row = existing_data[0]
        if first_row != FIELDNAMES:
            logger.warning(
                "Header row is missing or malformed in Google Sheets. "
                "Rewriting header to A1:M1."
            )
            sheet.update(HEADER_RANGE, [FIELDNAMES])
            existing_data = sheet.get_all_values()

        return existing_data

    def _build_existing_keys(self, existing_data: List[List[str]]) -> set[tuple[str, str]]:
        """Build a set of existing (run_date, symbol) keys from the sheet."""
        existing_keys: set[tuple[str, str]] = set()

        for row in existing_data[1:]:
            if len(row) >= 3:
                run_date_val = str(row[0]).strip()
                symbol_val = str(row[2]).strip()

                if run_date_val and symbol_val:
                    existing_keys.add((run_date_val, symbol_val))

        return existing_keys

    def _record_to_row(self, record: MarketRecord) -> List[object]:
        """Convert a MarketRecord into a Google Sheets row."""
        return [
            str(record.run_date).strip(),
            str(record.fetched_at).strip(),
            str(record.symbol).strip(),
            str(record.name).strip(),
            record.price_usd,
            record.change_pct_24h if record.change_pct_24h is not None else "",
            record.high_24h_usd if record.high_24h_usd is not None else "",
            record.low_24h_usd if record.low_24h_usd is not None else "",
            record.momentum_proxy if record.momentum_proxy is not None else "",
            record.volatility_proxy if record.volatility_proxy is not None else "",
            record.daily_delta_usd if record.daily_delta_usd is not None else "",
            str(record.data_source).strip(),
            str(record.pipeline_version).strip(),
        ]

    def write(self, records: List[MarketRecord]) -> None:
        """Write records to Google Sheets as vertically appended rows."""
        if not records:
            return

        try:
            client = self._authenticate()
            sheet = client.open_by_key(self.sheet_id).sheet1

            existing_data = self._ensure_header(sheet)
            existing_keys = self._build_existing_keys(existing_data)

            rows_to_write: List[List[object]] = []
            skipped = 0

            for record in records:
                composite_key = (
                    str(record.run_date).strip(),
                    str(record.symbol).strip(),
                )

                if composite_key in existing_keys:
                    skipped += 1
                    logger.warning(
                        "Skipping duplicate record in Sheets: %s",
                        composite_key,
                    )
                    continue

                rows_to_write.append(self._record_to_row(record))
                existing_keys.add(composite_key)

            if rows_to_write:
                sheet.append_rows(
                    rows_to_write,
                    value_input_option="USER_ENTERED",
                    insert_data_option="INSERT_ROWS",
                    table_range=TABLE_RANGE,
                )
                logger.info(
                    "Wrote %s records to Sheets (%s skipped).",
                    len(rows_to_write),
                    skipped,
                )
            else:
                logger.info("No new records to write to Sheets (%s skipped).", skipped)

        except Exception as e:
            logger.error("Failed to write to Google Sheets: %s", e)
            # Exception caught and logged per requirements, do not abort