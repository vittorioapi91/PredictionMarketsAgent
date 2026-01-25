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

        logger.info(
            "Connecting to DB: host=%s port=%s db=%s user=%s (env=%s from .env-%s)",
            self.db_host, self.db_port, self.db_name, self.db_user, self.env, self.env,
        )
        
        self.engine = None
        self.Session = None
        self._connect()

    def _connect(self):
        """
        Establish database connection.
        
        Raises:
            SQLAlchemyError: If connection fails (e.g., database doesn't exist)
        """
        self.engine = create_engine(
            self.connection_string,
            pool_pre_ping=True,
            echo=False
        )
        self.Session = sessionmaker(bind=self.engine)
        # Test connection - will raise error if database doesn't exist
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connected to database: {self.db_name} on {self.db_host}:{self.db_port}")

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        if self.engine is None:
            raise RuntimeError("Database engine not initialized. Call _connect() first.")
        try:
            with self.engine.connect() as conn:
                # Only create trade_data table if needed (handled in upload_csv_to_trade_data)
                # markets and order_books tables are not created here
                conn.commit()
            logger.info("Database tables created/verified")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {str(e)}")
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
        
        # Test connection - will raise error if database doesn't exist
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connected to database: {db_name} on {self.db_host}:{self.db_port}")
        
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
        
        # Prepare data for batch insert
        # Helper functions for data conversion
        def to_bool(value: Any) -> Optional[bool]:
            if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 't')
            return bool(value)
        
        def to_numeric(value: Any) -> Optional[float]:
            if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
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
        
        def to_str(value: Any) -> Optional[str]:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            return str(value) if value != "" else None
        
        def to_int(value: Any) -> Optional[int]:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        # Prepare DataFrame with proper data types
        logger.info("Preparing data for batch insert...")
        df_prepared = pd.DataFrame()
        
        # Map columns and apply conversions
        column_mappings = {
            "condition_id": lambda x: to_str(x),
            "question_id": lambda x: to_str(x),
            "question": lambda x: to_str(x),
            "description": lambda x: to_str(x),
            "market_slug": lambda x: to_str(x),
            "category": lambda x: to_str(x),
            "active": lambda x: to_bool(x),
            "closed": lambda x: to_bool(x),
            "archived": lambda x: to_bool(x),
            "accepting_orders": lambda x: to_bool(x),
            "accepting_order_timestamp": lambda x: to_timestamp(x),
            "enable_order_book": lambda x: to_bool(x),
            "minimum_order_size": lambda x: to_numeric(x),
            "minimum_tick_size": lambda x: to_numeric(x),
            "min_incentive_size": lambda x: to_numeric(x),
            "max_incentive_spread": lambda x: to_numeric(x),
            "maker_base_fee": lambda x: to_numeric(x),
            "taker_base_fee": lambda x: to_numeric(x),
            "end_date_iso": lambda x: to_timestamp(x),
            "game_start_time": lambda x: to_timestamp(x),
            "seconds_delay": lambda x: to_int(x),
            "fpmm": lambda x: to_str(x),
            "icon": lambda x: to_str(x),
            "image": lambda x: to_str(x),
            "neg_risk": lambda x: to_bool(x),
            "neg_risk_market_id": lambda x: to_str(x),
            "neg_risk_request_id": lambda x: to_str(x),
            "is_50_50_outcome": lambda x: to_bool(x),
            "token_0_id": lambda x: to_str(x),
            "token_0_outcome": lambda x: to_str(x),
            "token_0_price": lambda x: to_numeric(x),
            "token_0_winner": lambda x: to_bool(x),
            "token_1_id": lambda x: to_str(x),
            "token_1_outcome": lambda x: to_str(x),
            "token_1_price": lambda x: to_numeric(x),
            "token_1_winner": lambda x: to_bool(x),
            "rewards_rates": lambda x: to_str(x),
            "rewards_min_size": lambda x: to_str(x),
            "rewards_max_spread": lambda x: to_str(x),
            "notifications_enabled": lambda x: to_bool(x),
            "tags": lambda x: to_str(x),
        }
        
        # Apply conversions to existing columns
        for col, converter in column_mappings.items():
            if col in df.columns:
                df_prepared[col] = df[col].apply(converter)
        
        # Add download_date column
        df_prepared["download_date"] = download_date
        
        # Insert data in batches using pandas to_sql
        batch_size = 10000  # Insert 10,000 records at a time
        total_records = len(df_prepared)
        inserted = 0
        
        try:
            logger.info(f"Inserting {total_records} records in batches of {batch_size}...")
            
            # Use tqdm for progress indication
            pbar = tqdm(
                desc="Uploading records",
                total=total_records,
                unit=" records",
                dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )
            
            # Insert in batches
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = df_prepared.iloc[start_idx:end_idx]
                
                # Use pandas to_sql with if_exists='append' for batch inserts
                batch_df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',  # Use multi-row INSERT for better performance
                    chunksize=1000,  # Internal chunking within each batch
                )
                
                inserted += len(batch_df)
                pbar.update(len(batch_df))
            
            pbar.close()
            logger.info(f"✓ Inserted {inserted} records into {table_name} in database {db_name} on {self.db_host}:{self.db_port}")
            return inserted
        except SQLAlchemyError as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise
