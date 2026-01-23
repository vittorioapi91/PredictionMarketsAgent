# Airflow DAGs for PredictionMarketsAgent

This directory contains Airflow DAGs for orchestrating the Polymarket data collection pipeline.

## DAG: `polymarket_data_pipeline`

### Overview
This DAG orchestrates the complete data collection and database upload process with resumable SQL uploads.

### Task Flow

```
collect_markets_to_csv
    ↓
filter_open_markets_to_csv
    ↓
collect_order_books_to_csv
    ↓
upload_markets_to_sql (resumable)
    ↓
upload_order_books_to_sql (resumable)
```

### Tasks

#### 1. CSV Generation Tasks

**`collect_markets_to_csv`**
- Collects all markets from Polymarket API
- Saves to CSV: `storage/{env}/raw_data/polymarket_data_YYYYMMDD.csv`
- Returns CSV file path via XCom

**`filter_open_markets_to_csv`**
- Filters for open markets from raw data CSV
- Saves to CSV: `storage/{env}/open_markets/open_markets_YYYYMMDD.csv`
- Can resume from existing CSV if previous task was skipped

**`collect_order_books_to_csv`**
- Collects order books for open markets
- Saves to CSV: `storage/{env}/order_books/order_books_YYYYMMDD.csv`
- Can resume from existing CSV if previous task was skipped

#### 2. SQL Upload Tasks (Resumable)

**`upload_markets_to_sql`**
- Uploads markets from CSV to PostgreSQL
- **Resumable**: If task fails, it can be retried and will read from the existing CSV file
- Uses XCom to get CSV path, but falls back to finding the CSV file if XCom is empty
- Creates/updates database tables automatically

**`upload_order_books_to_sql`**
- Uploads order books from CSV to PostgreSQL
- **Resumable**: If task fails, it can be retried and will read from the existing CSV file
- Handles JSON parsing of bids/asks from CSV

### Resumability Features

Both SQL upload tasks are designed to be resumable:

1. **XCom Fallback**: If the CSV path is not in XCom (e.g., after a restart), the task will look for the CSV file based on today's date
2. **File Existence Check**: Before attempting upload, the task verifies the CSV file exists
3. **Idempotent Operations**: Database operations use `ON CONFLICT` clauses to handle duplicate inserts gracefully

### Schedule

- **Default**: Runs every 6 hours
- **Start Date**: 1 day ago (to catch up on any missed runs)
- **Catchup**: Disabled (won't backfill)

### Configuration

The DAG automatically:
- Detects environment from Git branch (main → prod, staging → test, dev/* → dev)
- Loads appropriate `.env-{env}` file
- Connects to the correct PostgreSQL database

### Dependencies

Make sure Airflow is installed:
```bash
pip install apache-airflow>=2.8.0
```

### Usage

1. Copy the DAG file to your Airflow `dags/` directory:
   ```bash
   cp src/_airflow_dags_/polymarket_pipeline_dag.py /path/to/airflow/dags/
   ```

2. Ensure the `src/` directory is accessible to Airflow (either in the same directory or add to PYTHONPATH)

3. The DAG will appear in the Airflow UI and can be triggered manually or will run on schedule

### Error Handling

- Tasks have 1 retry with 5-minute delay
- SQL upload tasks can be manually retried if they fail (will resume from CSV)
- All tasks log progress and errors for debugging

### Environment Variables

The DAG uses environment-specific `.env` files:
- `.env-dev` for dev branches
- `.env-test` for staging branch
- `.env-prod` for main branch

Each `.env` file should contain:
- `PolyMarketPrivateKey`: Polymarket API private key
- `DB_HOST`: PostgreSQL host
- `DB_PORT`: PostgreSQL port
- `DB_NAME`: Database name (e.g., `dev.PredictionMarketsAgent`)
- `DB_USER`: Database user
- `DB_PASSWORD`: Database password
