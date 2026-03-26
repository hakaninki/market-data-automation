# Market Data Automation

This project is a clean, reliable data pipeline designed as the data layer for a future AI project. 
It is a scheduled Python data pipeline that fetches real financial market data (BTC, ETH, Gold), validates and normalizes it, computes simple derived metrics, stores structured time-series data, writes to Google Sheets, CSV, and SQLite, and runs automatically daily via GitHub Actions.

![Daily Market Data Pipeline](https://github.com/hakaninki/market-data-automation/actions/workflows/daily_pipeline.yml/badge.svg)

*(Last Run Note: This is an automatically scheduled pipeline running at 08:00 UTC daily.)*

## Architecture

```text
┌──────────────┐     ┌──────────────┐
│  CoinGecko   │     │ Yahoo Finance│
│ (BTC, ETH)   │     │    (Gold)    │
└──────┬───────┘     └──────┬───────┘
       │                    │
       ▼                    ▼
┌───────────────────────────────────┐
│           fetchers/               │
└─────────────────┬─────────────────┘
                  │
                  ▼
┌───────────────────────────────────┐
│          validators/              │
│       (schema validation)         │
└─────────────────┬─────────────────┘
                  │
                  ▼
┌───────────────────────────────────┐
│         transformers/             │
│ (normalization, derived metrics)  │
└─────────────────┬─────────────────┘
                  │
                  ▼
┌───────────────────────────────────┐
│           storage/                │
│ ┌─────┐   ┌────────┐   ┌────────┐ │
│ │ CSV │   │ SQLite │   │ Sheets │ │
│ └─────┘   └────────┘   └────────┘ │
└───────────────────────────────────┘
```

## Environment Variables

| Variable | Description | Required | Default |
| -------- | ----------- | -------- | ------- |
| `COINGECKO_API_KEY` | Optional API key for CoinGecko | No | None |
| `GOOGLE_CREDENTIALS_JSON`| Path to service account JSON OR the raw JSON string | Yes | None |
| `GOOGLE_SHEET_ID` | ID of the Google Sheet to append to | Yes | None |
| `CSV_PATH` | Path to save the local CSV data | No | `data/market_data.csv` |
| `SQLITE_PATH` | Path to the local SQLite DB | No | `data/market_data.db` |
| `PIPELINE_VERSION` | Version written into every row | No | `1.0.0` |
| `ENABLE_SHEETS` | Enable writing to Google Sheets | No | `true` |
| `ENABLE_CSV` | Enable writing to CSV file | No | `true` |
| `ENABLE_SQLITE` | Enable writing to local SQLite | No | `true` |

## Derived Metrics

| Metric | Formula/Description |
| ------ | ------------------- |
| `volatility_proxy` | `(high_24h_usd - low_24h_usd) / price_usd * 100` |
| `momentum_proxy` | `% diff from 7d rolling mean (calculated from SQLite history)` |
| `daily_delta_usd`| `today_price - yesterday_price (calculated from SQLite history)` |

## Setup & Running Locally

1. **Install requirements:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Setup environment:**
   Copy `.env.example` to `.env` and fill out your variables.
3. **Run Pipeline:**
   ```bash
   python main.py
   ```

## Google Sheets Service Account Setup
1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new Service Account and download the JSON key.
3. Share your target Google Sheet with the client email address found in the JSON file (giving it Editor access).
4. Set the `GOOGLE_CREDENTIALS_JSON` environment variable to the path to the JSON file (or the full JSON string itself).
5. Set `GOOGLE_SHEET_ID` to the ID of the sheet (found in the URL: `https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit`).

## Known Limitations
- The `momentum_proxy` requires at least 2 days of historical data existing in SQLite to compute.
- Gold data from Yahoo Finance only represents closing prices; 24h highs/lows and percent changes are omitted as they can be less reliably obtained from this specific configuration without more granular Yahoo API endpoints.

## Future Improvements
- Migrate storage fully to a cloud data warehouse (e.g., BigQuery, Snowflake) in Project 2.
- Integrate the structured dataset as the foundation for an advanced machine learning/AI predictive application.

