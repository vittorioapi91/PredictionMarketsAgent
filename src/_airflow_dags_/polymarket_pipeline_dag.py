"""
Airflow DAG for Polymarket data collection pipeline.

This DAG:
1. Collects market data and saves to CSV
2. Uploads CSV data to PostgreSQL (resumable)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.polymarket import PolymarketClient, MarketDataProcessor, DatabaseManager
from src.polymarket.data_pipeline import run_gamma_fetch_and_save
from src.utils import get_environment, get_storage_path, load_environment_file
from pathlib import Path

logger = logging.getLogger(__name__)


default_args = {
    "owner": "prediction-markets-agent",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def collect_trade_data_to_csv(**context):
    """Task 1a: Collect CLOB markets, save to raw_data CSV."""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")

    logger.info(f"Starting CLOB market collection for {date_today}...")

    client = PolymarketClient()
    processor = MarketDataProcessor()
    processor.setup_directories()

    markets = client.fetch_all_markets()
    if not markets:
        raise ValueError("No CLOB markets collected")

    csv_file = processor.get_output_path("raw_data", date_today)
    processor.save_markets_to_csv(markets, csv_file, source="clob-api")

    logger.info(f"Collected {len(markets)} CLOB markets, saved to {csv_file}")
    return csv_file


def collect_gamma_to_csv(**context):
    """Task 1b: Collect Gamma events, flatten to markets, save to gamma_api CSV (reuses run_gamma_fetch_and_save)."""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")

    logger.info("Starting Gamma event collection for %s...", date_today)

    processor = MarketDataProcessor()
    processor.setup_directories()

    csv_file = run_gamma_fetch_and_save(
        processor,
        date_today,
        limit=100,
        include_closed=True,
    )
    if not csv_file:
        raise ValueError("No Gamma events collected or no markets flattened")

    return csv_file


def filter_open_markets_to_csv(**context):
    """Task 1c: Filter open markets from raw_data and save to CSV"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")

    logger.info(f"Filtering open markets for {date_today}...")

    ti = context["ti"]
    raw_data_csv = ti.xcom_pull(task_ids="collect_trade_data_to_csv")
    
    # If task was skipped, try to find existing CSV
    if not raw_data_csv:
        processor = MarketDataProcessor()
        raw_data_csv = processor.get_output_path("raw_data", date_today)
        if not os.path.exists(raw_data_csv):
            raise FileNotFoundError(f"Raw data CSV not found: {raw_data_csv}")
        logger.info(f"Using existing raw_data CSV: {raw_data_csv}")
    
    processor = MarketDataProcessor()
    markets_data = processor.load_markets_from_csv(raw_data_csv)
    open_markets = processor.filter_open_markets(markets_data)
    
    # Save filtered markets
    output_file = processor.get_output_path("open_markets", date_today)
    processor.save_markets_to_csv(open_markets, output_file)
    
    logger.info(f"Total markets: {len(markets_data)}")
    logger.info(f"Open markets: {len(open_markets)}")
    logger.info(f"Saved to {output_file}")
    
    return output_file


def collect_order_books_to_csv(**context):
    """Task 1d: Collect order books and save to CSV"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Collecting order books for {date_today}...")
    
    # Get open markets CSV from previous task
    ti = context["ti"]
    open_markets_csv = ti.xcom_pull(task_ids="filter_open_markets_to_csv")
    
    # If task was skipped, try to find existing CSV
    if not open_markets_csv:
        processor = MarketDataProcessor()
        open_markets_csv = processor.get_output_path("open_markets", date_today)
        if not os.path.exists(open_markets_csv):
            logger.warning(f"Open markets CSV not found: {open_markets_csv}")
            return None
    
    processor = MarketDataProcessor()
    client = PolymarketClient()
    
    open_markets = processor.load_markets_from_csv(open_markets_csv)
    
    if not open_markets:
        logger.warning("No open markets found")
        return None
    
    # Fetch order books
    order_books_data = client.get_order_books_for_markets(open_markets)
    
    if order_books_data:
        output_file = processor.get_output_path("order_books", date_today)
        processor.save_order_books_to_csv(order_books_data, output_file)
        logger.info(f"Collected order books for {len(order_books_data)} tokens")
        logger.info(f"Saved to {output_file}")
        return output_file
    else:
        logger.warning("No order books collected")
        return None


def upload_trade_data_to_sql(**context):
    """Task 2a: Upload raw_data CSV to trade_data table (resumable)."""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")

    logger.info(f"Uploading trade_data for {date_today}...")

    ti = context["ti"]
    csv_file = ti.xcom_pull(task_ids="collect_trade_data_to_csv")

    if not csv_file or not os.path.exists(csv_file):
        processor = MarketDataProcessor()
        csv_file = processor.get_output_path("raw_data", date_today)
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Raw data CSV not found: {csv_file}")
        logger.info(f"Using existing raw_data CSV: {csv_file}")

    db_manager = DatabaseManager()
    db_manager.create_tables()

    try:
        count = db_manager.upload_csv_to_trade_data(
            csv_path=csv_file,
            db_name="polymarket",
            table_name="trade_data",
        )
        logger.info(f"Uploaded {count} records to trade_data table")
        return count
    except Exception as e:
        logger.error(f"Failed to upload to trade_data table: {str(e)}")
        raise


def upload_gamma_markets_to_sql(**context):
    """Task 2b: Upload gamma_api CSV to gamma_markets table (resumable)."""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")

    logger.info(f"Uploading gamma_markets for {date_today}...")

    ti = context["ti"]
    csv_file = ti.xcom_pull(task_ids="collect_gamma_to_csv")

    if not csv_file or not os.path.exists(csv_file):
        processor = MarketDataProcessor()
        csv_file = processor.get_output_path("gamma_api", date_today)
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Gamma API CSV not found: {csv_file}")
        logger.info(f"Using existing gamma_api CSV: {csv_file}")

    db_manager = DatabaseManager()
    db_manager.create_tables()

    try:
        count = db_manager.upload_csv_to_gamma_markets(
            csv_path=csv_file,
            db_name="polymarket",
            table_name="gamma_markets",
        )
        logger.info(f"Uploaded {count} records to gamma_markets table")
        return count
    except Exception as e:
        logger.error(f"Failed to upload to gamma_markets table: {str(e)}")
        raise


def upload_order_books_to_sql(**context):
    """Task 2c: Upload order books from CSV to PostgreSQL (resumable)"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Uploading order books to SQL for {date_today}...")
    
    # Get CSV file from previous task
    ti = context["ti"]
    csv_file = ti.xcom_pull(task_ids="collect_order_books_to_csv")
    
    # If task was skipped or CSV path not in XCom, try to find existing CSV
    if not csv_file or not os.path.exists(csv_file):
        processor = MarketDataProcessor()
        csv_file = processor.get_output_path("order_books", date_today)
        if not os.path.exists(csv_file):
            logger.warning(f"Order books CSV not found: {csv_file}")
            return 0
        logger.info(f"Using existing CSV: {csv_file}")
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    # Load order books from CSV
    import pandas as pd
    import json
    
    df = pd.read_csv(csv_file)
    order_books_data = []
    
    for _, row in df.iterrows():
        # Parse bids and asks from CSV (they're stored as Python list strings)
        bids_str = str(row.get("bids", "")) if pd.notna(row.get("bids")) else ""
        asks_str = str(row.get("asks", "")) if pd.notna(row.get("asks")) else ""
        
        # Try to parse as JSON first, then as Python literal
        bids = []
        asks = []
        
        if bids_str:
            try:
                bids = json.loads(bids_str)
            except (json.JSONDecodeError, TypeError):
                try:
                    import ast
                    bids = ast.literal_eval(bids_str)
                except (ValueError, SyntaxError):
                    bids = []
        
        if asks_str:
            try:
                asks = json.loads(asks_str)
            except (json.JSONDecodeError, TypeError):
                try:
                    import ast
                    asks = ast.literal_eval(asks_str)
                except (ValueError, SyntaxError):
                    asks = []
        
        # Create a simple object to hold the order book data
        class OrderBook:
            def __init__(self, bids, asks):
                self.bids = bids
                self.asks = asks
        
        order_books_data.append({
            "market": {
                "condition_id": str(row.get("condition_id", "")),
                "question_id": str(row.get("question_id", "")),
            },
            "token": {
                "token_id": str(row.get("token_id", "")),
                "outcome": str(row.get("outcome", "")),
            },
            "order_book": OrderBook(bids, asks),
        })
    
    if not order_books_data:
        logger.warning("No order books found in CSV")
        return 0
    
    # Upload to database
    inserted = db_manager.insert_order_books(order_books_data)
    logger.info(f"Uploaded {inserted} order books to database")
    
    return inserted


# Create DAG
dag = DAG(
    "polymarket_data_pipeline",
    default_args=default_args,
    description="Polymarket data collection and database upload pipeline",
    schedule_interval="0 1 * * *",  # 1am daily (gamma_markets: truncate + insert; CSVs kept on disk)
    start_date=days_ago(1),
    catchup=False,
    tags=["polymarket", "data-collection"],
)

# Task 1a: CLOB -> raw_data
task_collect_trade_data = PythonOperator(
    task_id="collect_trade_data_to_csv",
    python_callable=collect_trade_data_to_csv,
    dag=dag,
)

# Task 1b: Gamma -> gamma_api
task_collect_gamma = PythonOperator(
    task_id="collect_gamma_to_csv",
    python_callable=collect_gamma_to_csv,
    dag=dag,
)

# Task 1c: Filter open markets (uses raw_data)
task_filter_open_markets = PythonOperator(
    task_id="filter_open_markets_to_csv",
    python_callable=filter_open_markets_to_csv,
    dag=dag,
)

# Task 1d: Collect order books
task_collect_order_books = PythonOperator(
    task_id="collect_order_books_to_csv",
    python_callable=collect_order_books_to_csv,
    dag=dag,
)

# Task 2a: raw_data -> trade_data
task_upload_trade_data = PythonOperator(
    task_id="upload_trade_data_to_sql",
    python_callable=upload_trade_data_to_sql,
    dag=dag,
)

# Task 2b: gamma_api -> gamma_markets
task_upload_gamma_markets = PythonOperator(
    task_id="upload_gamma_markets_to_sql",
    python_callable=upload_gamma_markets_to_sql,
    dag=dag,
)

# Task 2c: Order books
task_upload_order_books = PythonOperator(
    task_id="upload_order_books_to_sql",
    python_callable=upload_order_books_to_sql,
    dag=dag,
)

# Dependencies
task_collect_trade_data >> task_filter_open_markets >> task_collect_order_books
task_collect_trade_data >> task_upload_trade_data
task_collect_gamma >> task_upload_gamma_markets
task_collect_order_books >> task_upload_order_books
