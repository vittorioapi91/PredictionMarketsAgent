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
    
    # Upload CSV to trade_data table in polymarket database
    csv_file = processor.get_output_path("raw_data", date_today)
    try:
        count = db_manager.upload_csv_to_trade_data(
            csv_path=csv_file,
            db_name="polymarket",
            table_name="trade_data"
        )
        logger.info(f"Uploaded {count} records to trade_data table")
        return count
    except Exception as e:
        logger.error(f"Failed to upload to trade_data table: {str(e)}")
        raise


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


def upload_csv_to_trade_data(**context):
    """Task 2c: Upload CSV data to trade_data table in PostgreSQL"""
    load_environment_file()
    date_today = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Uploading CSV to trade_data table for {date_today}...")
    
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
    
    # Extract date from filename
    import re
    filename = os.path.basename(csv_file)
    date_match = re.search(r'(\d{8})', filename)
    if not date_match:
        raise ValueError(f"Could not extract date from filename: {filename}")
    
    download_date_str = date_match.group(1)
    from datetime import datetime as dt
    download_date = dt.strptime(download_date_str, "%Y%m%d").date()
    logger.info(f"Extracted download date: {download_date} from filename")
    
    # Initialize database connection to polymarket database
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = "polymarket"
    
    connection_string = (
        f"postgresql://{db_user}:{db_password}@"
        f"{db_host}:{db_port}/{db_name}"
    )
    
    engine = create_engine(connection_string, pool_pre_ping=True, echo=False)
    
    # Test connection - will raise error if database doesn't exist
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info(f"Connected to database: {db_name} on {db_host}:{db_port}")
    
    # Create trade_data table if it doesn't exist
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trade_data (
                    id SERIAL PRIMARY KEY,
                    condition_id VARCHAR(255),
                    question_id VARCHAR(255),
                    question TEXT,
                    description TEXT,
                    market_slug VARCHAR(255),
                    category VARCHAR(255),
                    active BOOLEAN,
                    closed BOOLEAN,
                    archived BOOLEAN,
                    accepting_orders BOOLEAN,
                    accepting_order_timestamp TIMESTAMP,
                    enable_order_book BOOLEAN,
                    minimum_order_size NUMERIC,
                    minimum_tick_size NUMERIC,
                    min_incentive_size NUMERIC,
                    max_incentive_spread NUMERIC,
                    maker_base_fee NUMERIC,
                    taker_base_fee NUMERIC,
                    end_date_iso TIMESTAMP,
                    game_start_time TIMESTAMP,
                    seconds_delay INTEGER,
                    fpmm VARCHAR(255),
                    icon TEXT,
                    image TEXT,
                    neg_risk BOOLEAN,
                    neg_risk_market_id VARCHAR(255),
                    neg_risk_request_id VARCHAR(255),
                    is_50_50_outcome BOOLEAN,
                    token_0_id VARCHAR(255),
                    token_0_outcome TEXT,
                    token_0_price NUMERIC,
                    token_0_winner BOOLEAN,
                    token_1_id VARCHAR(255),
                    token_1_outcome TEXT,
                    token_1_price NUMERIC,
                    token_1_winner BOOLEAN,
                    rewards_rates TEXT,
                    rewards_min_size TEXT,
                    rewards_max_spread TEXT,
                    notifications_enabled BOOLEAN,
                    tags TEXT,
                    download_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create indexes
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trade_data_question 
                ON trade_data(question)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trade_data_download_date 
                ON trade_data(download_date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trade_data_condition_id 
                ON trade_data(condition_id)
            """))
            
            conn.commit()
        logger.info(f"Table trade_data created/verified in database {db_name}")
    except SQLAlchemyError as e:
        logger.error(f"Error creating table: {str(e)}")
        raise
    
    # Load markets from CSV
    processor = MarketDataProcessor()
    markets = processor.load_markets_from_csv(csv_file)
    
    if not markets:
        raise ValueError("No markets found in CSV")
    
    # Insert data
    import pandas as pd
    inserted = 0
    try:
        with engine.connect() as conn:
            for market in markets:
                # Convert boolean strings to actual booleans
                def to_bool(value):
                    if value is None or value == "":
                        return None
                    if isinstance(value, bool):
                        return value
                    if isinstance(value, str):
                        return value.lower() in ('true', '1', 'yes', 't')
                    return bool(value)
                
                # Convert numeric strings to numbers
                def to_numeric(value):
                    if value is None or value == "":
                        return None
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None
                
                # Convert timestamp strings
                def to_timestamp(value):
                    if value is None or value == "":
                        return None
                    try:
                        return pd.to_datetime(value, errors='coerce')
                    except:
                        return None
                
                data = {
                    "condition_id": str(market.get("condition_id", "")) if market.get("condition_id") else None,
                    "question_id": str(market.get("question_id", "")) if market.get("question_id") else None,
                    "question": str(market.get("question", "")) if market.get("question") else None,
                    "description": str(market.get("description", "")) if market.get("description") else None,
                    "market_slug": str(market.get("market_slug", "")) if market.get("market_slug") else None,
                    "category": str(market.get("category", "")) if market.get("category") else None,
                    "active": to_bool(market.get("active")),
                    "closed": to_bool(market.get("closed")),
                    "archived": to_bool(market.get("archived")),
                    "accepting_orders": to_bool(market.get("accepting_orders")),
                    "accepting_order_timestamp": to_timestamp(market.get("accepting_order_timestamp")),
                    "enable_order_book": to_bool(market.get("enable_order_book")),
                    "minimum_order_size": to_numeric(market.get("minimum_order_size")),
                    "minimum_tick_size": to_numeric(market.get("minimum_tick_size")),
                    "min_incentive_size": to_numeric(market.get("min_incentive_size")),
                    "max_incentive_spread": to_numeric(market.get("max_incentive_spread")),
                    "maker_base_fee": to_numeric(market.get("maker_base_fee")),
                    "taker_base_fee": to_numeric(market.get("taker_base_fee")),
                    "end_date_iso": to_timestamp(market.get("end_date_iso")),
                    "game_start_time": to_timestamp(market.get("game_start_time")),
                    "seconds_delay": int(market.get("seconds_delay")) if market.get("seconds_delay") else None,
                    "fpmm": str(market.get("fpmm", "")) if market.get("fpmm") else None,
                    "icon": str(market.get("icon", "")) if market.get("icon") else None,
                    "image": str(market.get("image", "")) if market.get("image") else None,
                    "neg_risk": to_bool(market.get("neg_risk")),
                    "neg_risk_market_id": str(market.get("neg_risk_market_id", "")) if market.get("neg_risk_market_id") else None,
                    "neg_risk_request_id": str(market.get("neg_risk_request_id", "")) if market.get("neg_risk_request_id") else None,
                    "is_50_50_outcome": to_bool(market.get("is_50_50_outcome")),
                    "token_0_id": str(market.get("token_0_id", "")) if market.get("token_0_id") else None,
                    "token_0_outcome": str(market.get("token_0_outcome", "")) if market.get("token_0_outcome") else None,
                    "token_0_price": to_numeric(market.get("token_0_price")),
                    "token_0_winner": to_bool(market.get("token_0_winner")),
                    "token_1_id": str(market.get("token_1_id", "")) if market.get("token_1_id") else None,
                    "token_1_outcome": str(market.get("token_1_outcome", "")) if market.get("token_1_outcome") else None,
                    "token_1_price": to_numeric(market.get("token_1_price")),
                    "token_1_winner": to_bool(market.get("token_1_winner")),
                    "rewards_rates": str(market.get("rewards_rates", "")) if market.get("rewards_rates") else None,
                    "rewards_min_size": str(market.get("rewards_min_size", "")) if market.get("rewards_min_size") else None,
                    "rewards_max_spread": str(market.get("rewards_max_spread", "")) if market.get("rewards_max_spread") else None,
                    "notifications_enabled": to_bool(market.get("notifications_enabled")),
                    "tags": str(market.get("tags", "")) if market.get("tags") else None,
                    "download_date": download_date,
                }
                
                # Build INSERT statement (only include non-None values)
                columns = [k for k, v in data.items() if v is not None]
                placeholders = ", ".join([f":{k}" for k in columns])
                values_dict = {k: v for k, v in data.items() if v is not None}
                
                conn.execute(text(f"""
                    INSERT INTO trade_data ({", ".join(columns)})
                    VALUES ({placeholders})
                """), values_dict)
                inserted += 1
            
            conn.commit()
        logger.info(f"Uploaded {inserted} records to trade_data table in {db_name}")
        return inserted
    except SQLAlchemyError as e:
        logger.error(f"Error inserting data: {str(e)}")
        raise


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

# Task 2c: Upload CSV to trade_data table
task_upload_trade_data = PythonOperator(
    task_id="upload_csv_to_trade_data",
    python_callable=upload_csv_to_trade_data,
    dag=dag,
)

# Set task dependencies
task_collect_markets >> task_filter_open_markets >> task_collect_order_books
task_filter_open_markets >> task_upload_markets
task_filter_open_markets >> task_upload_trade_data
task_collect_order_books >> task_upload_order_books
