"""Database operations for PredictionMarketsAgent"""

import os
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Boolean, Float, Integer, DateTime, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from tqdm import tqdm

try:
    from src.utils import load_environment_file, get_environment
except ImportError:
    from utils import load_environment_file, get_environment

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""

    def __init__(self):
        """
        Initialize database connection
        
        Raises:
            ValueError: If required environment variables are missing
        """
        load_environment_file()
        self.env = get_environment()
        
        # Get database credentials from environment - require all variables
        required_vars = {
            "DB_HOST": os.getenv("DB_HOST"),
            "DB_PORT": os.getenv("DB_PORT"),
            "DB_NAME": os.getenv("DB_NAME"),
            "DB_USER": os.getenv("DB_USER"),
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        }
        
        # Check for missing or empty variables
        missing_vars = [var for var, value in required_vars.items() if not value or value.strip() == ""]
        
        if missing_vars:
            raise ValueError(
                f"Missing or empty required database environment variables: {', '.join(missing_vars)}\n"
                f"Please set these variables in .env-{self.env} file."
            )
        
        self.db_host = required_vars["DB_HOST"]
        self.db_port = required_vars["DB_PORT"]
        self.db_name = required_vars["DB_NAME"]
        self.db_user = required_vars["DB_USER"]
        self.db_password = required_vars["DB_PASSWORD"]
        
        # Create connection string
        self.connection_string = (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )
        
        self.engine = None
        self.Session = None
        self._connect()

    def _connect(self):
        """Establish database connection"""
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                echo=False
            )
            self.Session = sessionmaker(bind=self.engine)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connected to database: {self.db_name} on {self.db_host}:{self.db_port}")
        except SQLAlchemyError as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        if self.engine is None:
            raise RuntimeError("Database engine not initialized. Call _connect() first.")
        try:
            with self.engine.connect() as conn:
                # Markets table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS markets (
                        id SERIAL PRIMARY KEY,
                        condition_id VARCHAR(255) UNIQUE,
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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Order books table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS order_books (
                        id SERIAL PRIMARY KEY,
                        condition_id VARCHAR(255),
                        question_id VARCHAR(255),
                        token_id VARCHAR(255),
                        outcome TEXT,
                        bids_count INTEGER,
                        asks_count INTEGER,
                        bids JSONB,
                        asks JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (condition_id) REFERENCES markets(condition_id)
                    )
                """))
                
                # Create indexes
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_markets_condition_id ON markets(condition_id)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(active)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_order_books_token_id ON order_books(token_id)
                """))
                
                conn.commit()
            logger.info("Database tables created/verified")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    def insert_order_books(self, order_books_data: List[Dict[str, Any]]) -> int:
        """
        Insert order books into the database.

        Args:
            order_books_data: List of dictionaries with market, token, and order_book

        Returns:
            Number of order books inserted
        """
        if not order_books_data:
            return 0

        if self.engine is None:
            raise RuntimeError("Database engine not initialized. Call _connect() first.")
        
        inserted = 0
        try:
            with self.engine.connect() as conn:
                for item in order_books_data:
                    market = item.get("market", {})
                    token = item.get("token", {})
                    order_book = item.get("order_book")

                    bids = []
                    asks = []
                    if order_book:
                        if hasattr(order_book, "bids"):
                            bids = order_book.bids if order_book.bids else []
                        if hasattr(order_book, "asks"):
                            asks = order_book.asks if order_book.asks else []

                    data = {
                        "condition_id": market.get("condition_id", ""),
                        "question_id": market.get("question_id", ""),
                        "token_id": token.get("token_id", ""),
                        "outcome": token.get("outcome", ""),
                        "bids_count": len(bids),
                        "asks_count": len(asks),
                        "bids": bids,
                        "asks": asks,
                    }

                    import json
                    conn.execute(text("""
                        INSERT INTO order_books (
                            condition_id, question_id, token_id, outcome,
                            bids_count, asks_count, bids, asks
                        ) VALUES (
                            :condition_id, :question_id, :token_id, :outcome,
                            :bids_count, :asks_count, :bids, :asks
                        )
                    """), {
                        **data,
                        "bids": json.dumps(bids) if bids else None,
                        "asks": json.dumps(asks) if asks else None,
                    })
                    inserted += 1
                conn.commit()
            logger.info(f"Inserted {inserted} order books in database")
            return inserted
        except SQLAlchemyError as e:
            logger.error(f"Error inserting order books: {str(e)}")
            raise

    def upload_csv_to_trade_data(
        self,
        csv_path: str,
        db_name: str = "polymarket",
        table_name: str = "trade_data"
    ) -> int:
        """
        Upload CSV data to the trade_data table in the specified database.
        Extracts download date from filename (format: polymarket_data_YYYYMMDD.csv).

        Args:
            csv_path: Path to the CSV file
            db_name: Database name (default: "polymarket")
            table_name: Table name (default: "trade_data")

        Returns:
            Number of records inserted
        """
        import re
        import pandas as pd
        from pathlib import Path
        
        # Extract date from filename
        filename = os.path.basename(csv_path)
        date_match = re.search(r'(\d{8})', filename)
        if not date_match:
            raise ValueError(f"Could not extract date from filename: {filename}")
        
        download_date_str = date_match.group(1)
        download_date = datetime.strptime(download_date_str, "%Y%m%d").date()
        logger.info(f"Extracted download date: {download_date} from filename")
        
        # Create connection to the specified database
        connection_string = (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{db_name}"
        )
        
        engine = create_engine(connection_string, pool_pre_ping=True, echo=False)
        
        try:
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connected to database: {db_name} on {self.db_host}:{self.db_port}")
        except SQLAlchemyError as e:
            logger.error(f"Error connecting to database {db_name} on {self.db_host}:{self.db_port}: {str(e)}")
            raise
        
        # Create trade_data table if it doesn't exist
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
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
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_question 
                    ON {table_name}(question)
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_download_date 
                    ON {table_name}(download_date)
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_condition_id 
                    ON {table_name}(condition_id)
                """))
                
                conn.commit()
            logger.info(f"Table {table_name} created/verified in database {db_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
        
        # Load CSV data
        try:
            logger.info(f"Loading CSV from: {csv_path}")
            df = pd.read_csv(csv_path, low_memory=False)
            logger.info(f"Loaded {len(df)} records from CSV")
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise
        
        # Insert data
        inserted = 0
        try:
            with engine.connect() as conn:
                # Use tqdm for progress indication
                pbar = tqdm(
                    desc="Uploading records",
                    total=len(df),
                    unit=" records",
                    dynamic_ncols=True,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                )
                
                for idx, row in df.iterrows():
                    # Helper function to safely get value from row
                    def get_value(key: str, default: Any = None) -> Any:
                        val = row.get(key, default)
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            return None
                        return val
                    
                    # Convert boolean strings to actual booleans
                    def to_bool(value: Any) -> Optional[bool]:
                        if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
                            return None
                        if isinstance(value, bool):
                            return value
                        if isinstance(value, str):
                            return value.lower() in ('true', '1', 'yes', 't')
                        return bool(value)
                    
                    # Convert numeric strings to numbers
                    def to_numeric(value: Any) -> Optional[float]:
                        if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
                            return None
                        try:
                            return float(value)
                        except (ValueError, TypeError):
                            return None
                    
                    # Convert timestamp strings
                    def to_timestamp(value: Any) -> Optional[datetime]:
                        if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
                            return None
                        try:
                            result = pd.to_datetime(value, errors='coerce')
                            if pd.isna(result):
                                return None
                            return result.to_pydatetime() if hasattr(result, 'to_pydatetime') else result
                        except:
                            return None
                    
                    # Helper to safely convert to string
                    def to_str(value: Any) -> Optional[str]:
                        if value is None or (isinstance(value, float) and pd.isna(value)):
                            return None
                        return str(value) if value != "" else None
                    
                    # Helper to safely convert to int
                    def to_int(value: Any) -> Optional[int]:
                        if value is None or (isinstance(value, float) and pd.isna(value)):
                            return None
                        try:
                            return int(value)
                        except (ValueError, TypeError):
                            return None
                    
                    data = {
                        "condition_id": to_str(get_value("condition_id")),
                        "question_id": to_str(get_value("question_id")),
                        "question": to_str(get_value("question")),
                        "description": to_str(get_value("description")),
                        "market_slug": to_str(get_value("market_slug")),
                        "category": to_str(get_value("category")),
                        "active": to_bool(get_value("active")),
                        "closed": to_bool(get_value("closed")),
                        "archived": to_bool(get_value("archived")),
                        "accepting_orders": to_bool(get_value("accepting_orders")),
                        "accepting_order_timestamp": to_timestamp(get_value("accepting_order_timestamp")),
                        "enable_order_book": to_bool(get_value("enable_order_book")),
                        "minimum_order_size": to_numeric(get_value("minimum_order_size")),
                        "minimum_tick_size": to_numeric(get_value("minimum_tick_size")),
                        "min_incentive_size": to_numeric(get_value("min_incentive_size")),
                        "max_incentive_spread": to_numeric(get_value("max_incentive_spread")),
                        "maker_base_fee": to_numeric(get_value("maker_base_fee")),
                        "taker_base_fee": to_numeric(get_value("taker_base_fee")),
                        "end_date_iso": to_timestamp(get_value("end_date_iso")),
                        "game_start_time": to_timestamp(get_value("game_start_time")),
                        "seconds_delay": to_int(get_value("seconds_delay")),
                        "fpmm": to_str(get_value("fpmm")),
                        "icon": to_str(get_value("icon")),
                        "image": to_str(get_value("image")),
                        "neg_risk": to_bool(get_value("neg_risk")),
                        "neg_risk_market_id": to_str(get_value("neg_risk_market_id")),
                        "neg_risk_request_id": to_str(get_value("neg_risk_request_id")),
                        "is_50_50_outcome": to_bool(get_value("is_50_50_outcome")),
                        "token_0_id": to_str(get_value("token_0_id")),
                        "token_0_outcome": to_str(get_value("token_0_outcome")),
                        "token_0_price": to_numeric(get_value("token_0_price")),
                        "token_0_winner": to_bool(get_value("token_0_winner")),
                        "token_1_id": to_str(get_value("token_1_id")),
                        "token_1_outcome": to_str(get_value("token_1_outcome")),
                        "token_1_price": to_numeric(get_value("token_1_price")),
                        "token_1_winner": to_bool(get_value("token_1_winner")),
                        "rewards_rates": to_str(get_value("rewards_rates")),
                        "rewards_min_size": to_str(get_value("rewards_min_size")),
                        "rewards_max_spread": to_str(get_value("rewards_max_spread")),
                        "notifications_enabled": to_bool(get_value("notifications_enabled")),
                        "tags": to_str(get_value("tags")),
                        "download_date": download_date,
                    }
                    
                    # Build INSERT statement (only include non-None values)
                    columns = [k for k, v in data.items() if v is not None]
                    placeholders = ", ".join([f":{k}" for k in columns])
                    values_dict = {k: v for k, v in data.items() if v is not None}
                    
                    conn.execute(text(f"""
                        INSERT INTO {table_name} ({", ".join(columns)})
                        VALUES ({placeholders})
                    """), values_dict)
                    inserted += 1
                    pbar.update(1)
                
                pbar.close()
                conn.commit()
            logger.info(f"✓ Inserted {inserted} records into {table_name} in database {db_name} on {self.db_host}:{self.db_port}")
            return inserted
        except SQLAlchemyError as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise
