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


def collect_markets_to_csv(**context):
    """Task 1: Collect all markets and save to CSV"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Starting market collection for {date_today}...")
    
    # Initialize components
    client = PolymarketClient()
    processor = MarketDataProcessor()
    
    # Setup directories
    processor.setup_directories()
    
    # Collect markets
    markets = client.fetch_all_markets()
    
    if not markets:
        raise ValueError("No markets collected")
    
    # Save to CSV
    csv_file = processor.get_output_path("raw_data", date_today)
    processor.save_markets_to_csv(markets, csv_file)
    
    logger.info(f"Collected {len(markets)} markets")
    logger.info(f"Saved to {csv_file}")
    
    # Push CSV path to XCom for next task
    return csv_file


def filter_open_markets_to_csv(**context):
    """Task 1b: Filter open markets and save to CSV"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Filtering open markets for {date_today}...")
    
    # Get CSV file from previous task
    ti = context["ti"]
    raw_data_csv = ti.xcom_pull(task_ids="collect_markets_to_csv")
    
    # If task was skipped, try to find existing CSV
    if not raw_data_csv:
        processor = MarketDataProcessor()
        raw_data_csv = processor.get_output_path("raw_data", date_today)
        if not os.path.exists(raw_data_csv):
            raise FileNotFoundError(f"Raw data CSV not found: {raw_data_csv}")
    
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
    """Task 1c: Collect order books and save to CSV"""
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


def upload_markets_to_sql(**context):
    """Task 2: Upload markets from CSV to PostgreSQL (resumable)"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Uploading markets to SQL for {date_today}...")
    
    # Get CSV file from previous task
    ti = context["ti"]
    csv_file = ti.xcom_pull(task_ids="collect_markets_to_csv")
    
    # If task was skipped or CSV path not in XCom, try to find existing CSV
    if not csv_file or not os.path.exists(csv_file):
        processor = MarketDataProcessor()
        csv_file = processor.get_output_path("raw_data", date_today)
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Markets CSV not found: {csv_file}")
        logger.info(f"Using existing CSV: {csv_file}")
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    # Load markets from CSV
    processor = MarketDataProcessor()
    markets = processor.load_markets_from_csv(csv_file)
    
    if not markets:
        raise ValueError("No markets found in CSV")
    
    # Upload to database
    inserted = db_manager.insert_markets(markets)
    logger.info(f"Uploaded {inserted} markets to database")
    
    return inserted


def upload_order_books_to_sql(**context):
    """Task 2b: Upload order books from CSV to PostgreSQL (resumable)"""
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
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=["polymarket", "data-collection"],
)

# Task 1: Collect markets to CSV
task_collect_markets = PythonOperator(
    task_id="collect_markets_to_csv",
    python_callable=collect_markets_to_csv,
    dag=dag,
)

# Task 1b: Filter open markets to CSV
task_filter_open_markets = PythonOperator(
    task_id="filter_open_markets_to_csv",
    python_callable=filter_open_markets_to_csv,
    dag=dag,
)

# Task 1c: Collect order books to CSV
task_collect_order_books = PythonOperator(
    task_id="collect_order_books_to_csv",
    python_callable=collect_order_books_to_csv,
    dag=dag,
)

# Task 2: Upload markets to SQL (resumable)
task_upload_markets = PythonOperator(
    task_id="upload_markets_to_sql",
    python_callable=upload_markets_to_sql,
    dag=dag,
)

# Task 2b: Upload order books to SQL (resumable)
task_upload_order_books = PythonOperator(
    task_id="upload_order_books_to_sql",
    python_callable=upload_order_books_to_sql,
    dag=dag,
)

# Set task dependencies
task_collect_markets >> task_filter_open_markets >> task_collect_order_books
task_filter_open_markets >> task_upload_markets
task_collect_order_books >> task_upload_order_books
