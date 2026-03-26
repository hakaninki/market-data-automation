# Architecture Overview

The `market-data-automation` pipeline is structured using clean architectural boundaries. This keeps data operations pure, testable, and interchangeable.

## 1. Domain Models (`src/models.py`)
All internal communications are passed using the `MarketRecord` dataclass. This guarantees type-safety across components.

## 2. Fetchers (`src/fetchers/`)
External API logic is fully isolated here. 
- `coingecko.py`: Makes calls to CoinGecko and includes resilient retries via `tenacity`.
- `commodities.py`: Uses `yfinance`. 

## 3. Validators (`src/validators/`)
Before transformations, we validate raw inputs here using pure Python standard library types to ensure stability even if external API shapes unexpectedly change.

## 4. Transformers (`src/transformers/`)
`normalizer.py` converts validated dictionaries into structured `MarketRecord` objects, injecting SQL logic dependencies safely via pure functional arguments (such as `db_path`) rather than direct connections, though it uses SQLite ad-hoc to construct rolling metrics (like `momentum_proxy`).

## 5. Storage (`src/storage/`)
The `StorageAdapter` abstract base class dictates that all storage destinations implement `write(List[MarketRecord])`. This allows appending new outputs synchronously like CSV, SQLite, and Google Sheets, without breaking changes.
