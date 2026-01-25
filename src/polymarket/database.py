"""
Database operations for PredictionMarketsAgent

Table Data Sources:
-------------------
- trade_data: CLOB market data (https://clob.polymarket.com).
  - Fetched via: PolymarketClient.fetch_all_markets(); saved to raw_data CSV, then uploaded here.
  - Uploaded via: data_pipeline.collect_trade_data() or Airflow task upload_trade_data_to_sql.
  - Used for filter_open_markets / order books.

- gamma_markets: Flattened market data from Gamma API (https://gamma-api.polymarket.com/events).
  - Fetched via: gamma_client.fetch_all_events() (all events, include_closed=True).
  - Flattened via: gamma_events_to_trade_data_rows(); saved to gamma_api CSV, then uploaded here.
  - Uploaded via: data_pipeline.collect_gamma() or Airflow task upload_gamma_markets_to_sql.
  - Daily refresh: table is truncated before each upload (1am DAG); CSVs are kept on disk for analysis.
  - Search queries gamma_markets; filters active-only by default.
  - All columns use snake_case (condition_id, question_id, market_slug, volume_24hr, etc.).
"""

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

    def upload_csv_to_table(
        self,
        csv_path: str,
        db_name: str = "polymarket",
        table_name: str = "trade_data"
    ) -> int:
        """
        Generic method to upload CSV data to any table in the specified database.
        Automatically creates the table if it doesn't exist (based on table_name).
        Extracts download date from filename (format: *_YYYYMMDD.csv).

        Args:
            csv_path: Path to the CSV file
            db_name: Database name (default: "polymarket")
            table_name: Table name (default: "trade_data")

        Returns:
            Number of records inserted
        """
        import re
        import pandas as pd
        import json
        
        # Extract date from filename
        filename = os.path.basename(csv_path)
        date_match = re.search(r'(\d{8})', filename)
        if not date_match:
            try:
                file_time = os.path.getmtime(csv_path)
                download_date = datetime.fromtimestamp(file_time).date()
                logger.warning(f"Could not extract date from filename: {filename}, using file modification date: {download_date}")
            except Exception as e:
                raise ValueError(f"Could not extract date from filename: {filename}, and could not get file modification time: {str(e)}")
        else:
            download_date_str = date_match.group(1)
            download_date = datetime.strptime(download_date_str, "%Y%m%d").date()
            logger.info(f"Extracted download date: {download_date} from filename")
        
        # Create connection
        connection_string = (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{db_name}"
        )
        engine = create_engine(connection_string, pool_pre_ping=True, echo=False)
        
        # Test connection - will raise error if database doesn't exist
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connected to database: {db_name} on {self.db_host}:{self.db_port}")
        
        # Create table if it doesn't exist (based on table_name)
        if table_name == "gamma_markets":
            try:
                with engine.connect() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS gamma_markets (
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
                            restricted BOOLEAN,
                            volume NUMERIC,
                            volume_num NUMERIC,
                            volume_24hr NUMERIC,
                            volume_1wk NUMERIC,
                            volume_1mo NUMERIC,
                            volume_1yr NUMERIC,
                            volume_1wk_amm NUMERIC,
                            volume_1mo_amm NUMERIC,
                            volume_1yr_amm NUMERIC,
                            volume_1wk_clob NUMERIC,
                            volume_1mo_clob NUMERIC,
                            volume_1yr_clob NUMERIC,
                            liquidity NUMERIC,
                            liquidity_num NUMERIC,
                            liquidity_amm NUMERIC,
                            liquidity_clob NUMERIC,
                            open_interest NUMERIC,
                            competitive NUMERIC,
                            spread NUMERIC,
                            one_day_price_change NUMERIC,
                            one_hour_price_change NUMERIC,
                            one_week_price_change NUMERIC,
                            one_month_price_change NUMERIC,
                            one_year_price_change NUMERIC,
                            last_trade_price NUMERIC,
                            best_bid NUMERIC,
                            best_ask NUMERIC,
                            image TEXT,
                            icon TEXT,
                            end_date_iso TIMESTAMP,
                            clob_token_ids TEXT,
                            outcomes TEXT,
                            outcome_prices TEXT,
                            token_0_id VARCHAR(255),
                            token_0_outcome TEXT,
                            token_0_price NUMERIC,
                            token_0_winner BOOLEAN,
                            token_1_id VARCHAR(255),
                            token_1_outcome TEXT,
                            token_1_price NUMERIC,
                            token_1_winner BOOLEAN,
                            download_date DATE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    for idx_sql in [
                        "CREATE INDEX IF NOT EXISTS idx_gamma_markets_question ON gamma_markets(question)",
                        "CREATE INDEX IF NOT EXISTS idx_gamma_markets_condition_id ON gamma_markets(condition_id)",
                        "CREATE INDEX IF NOT EXISTS idx_gamma_markets_volume ON gamma_markets(volume) WHERE volume IS NOT NULL",
                        "CREATE INDEX IF NOT EXISTS idx_gamma_markets_download_date ON gamma_markets(download_date)",
                    ]:
                        conn.execute(text(idx_sql))
                    # Add any missing columns (migration for existing gamma_markets tables)
                    _gamma_extra_columns = [
                        ("volume_1wk", "NUMERIC"), ("volume_1mo", "NUMERIC"), ("volume_1yr", "NUMERIC"),
                        ("volume_1wk_amm", "NUMERIC"), ("volume_1mo_amm", "NUMERIC"), ("volume_1yr_amm", "NUMERIC"),
                        ("volume_1wk_clob", "NUMERIC"), ("volume_1mo_clob", "NUMERIC"), ("volume_1yr_clob", "NUMERIC"),
                        ("liquidity", "NUMERIC"), ("liquidity_num", "NUMERIC"), ("liquidity_amm", "NUMERIC"), ("liquidity_clob", "NUMERIC"),
                        ("open_interest", "NUMERIC"), ("competitive", "NUMERIC"), ("spread", "NUMERIC"),
                        ("one_day_price_change", "NUMERIC"), ("one_hour_price_change", "NUMERIC"),
                        ("one_week_price_change", "NUMERIC"), ("one_month_price_change", "NUMERIC"), ("one_year_price_change", "NUMERIC"),
                        ("last_trade_price", "NUMERIC"), ("best_bid", "NUMERIC"), ("best_ask", "NUMERIC"),
                    ]
                    for col, typ in _gamma_extra_columns:
                        r = conn.execute(text("""
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public' AND table_name = 'gamma_markets' AND column_name = :col
                        """), {"col": col})
                        if r.fetchone() is None:
                            conn.execute(text(f"ALTER TABLE gamma_markets ADD COLUMN {col} {typ}"))
                            logger.info("Added column gamma_markets.%s", col)
                    conn.commit()
                logger.info("Table gamma_markets created/verified")
            except SQLAlchemyError as e:
                logger.error(f"Error creating table: {str(e)}")
                raise
        elif table_name == "trade_data":
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY,
                            condition_id VARCHAR(255), question_id VARCHAR(255),
                            question TEXT, description TEXT, market_slug VARCHAR(255), category VARCHAR(255),
                            active BOOLEAN, closed BOOLEAN, archived BOOLEAN, accepting_orders BOOLEAN,
                            accepting_order_timestamp TIMESTAMP, enable_order_book BOOLEAN,
                            minimum_order_size NUMERIC, minimum_tick_size NUMERIC, min_incentive_size NUMERIC,
                            max_incentive_spread NUMERIC, maker_base_fee NUMERIC, taker_base_fee NUMERIC,
                            end_date_iso TIMESTAMP, game_start_time TIMESTAMP, seconds_delay INTEGER,
                            fpmm VARCHAR(255), icon TEXT, image TEXT, neg_risk BOOLEAN,
                            neg_risk_market_id VARCHAR(255), neg_risk_request_id VARCHAR(255), is_50_50_outcome BOOLEAN,
                            token_0_id VARCHAR(255), token_0_outcome TEXT, token_0_price NUMERIC, token_0_winner BOOLEAN,
                            token_1_id VARCHAR(255), token_1_outcome TEXT, token_1_price NUMERIC, token_1_winner BOOLEAN,
                            rewards_rates TEXT, rewards_min_size TEXT, rewards_max_spread TEXT,
                            notifications_enabled BOOLEAN, tags TEXT, volume NUMERIC,
                            download_date DATE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    for idx_sql in [
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_question ON {table_name}(question)",
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_condition_id ON {table_name}(condition_id)",
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_volume ON {table_name}(volume) WHERE volume IS NOT NULL",
                    ]:
                        conn.execute(text(idx_sql))
                    conn.commit()
                logger.info(f"Table {table_name} created/verified")
            except SQLAlchemyError as e:
                logger.error(f"Error creating table: {str(e)}")
                raise
        # Load CSV
        try:
            logger.info(f"Loading CSV from: {csv_path}")
            df = pd.read_csv(csv_path, low_memory=False)
            logger.info(f"Loaded {len(df)} records from CSV")
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise
        
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
        
        def to_json(value: Any) -> Optional[str]:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            if isinstance(value, str):
                try:
                    json.loads(value)
                    return value
                except:
                    return value
            try:
                return json.dumps(value)
            except:
                return str(value) if value != "" else None
        
        # Prepare DataFrame - apply type conversions based on column names
        logger.info("Preparing data for batch insert...")
        df_prepared = pd.DataFrame()
        
        bool_cols = ["active", "closed", "archived", "accepting_orders", "enable_order_book", 
                     "neg_risk", "is_50_50_outcome", "token_0_winner", "token_1_winner", 
                     "notifications_enabled", "restricted", "fpmm_live"]
        numeric_cols = ["volume", "volume_num", "volume_24hr", "volume_1wk", "volume_1mo", "volume_1yr",
                       "volume_1wk_amm", "volume_1mo_amm", "volume_1yr_amm",
                       "volume_1wk_clob", "volume_1mo_clob", "volume_1yr_clob",
                       "liquidity", "liquidity_num", "liquidity_amm", "liquidity_clob",
                       "open_interest",
                       "minimum_order_size", "minimum_tick_size", "min_incentive_size", "max_incentive_spread",
                       "maker_base_fee", "taker_base_fee", "token_0_price", "token_1_price",
                       "competitive", "spread", "one_day_price_change", "one_hour_price_change",
                       "one_week_price_change", "one_month_price_change", "one_year_price_change",
                       "last_trade_price", "best_bid", "best_ask"]
        timestamp_cols = ["accepting_order_timestamp", "end_date_iso", "game_start_time", "end_date"]
        json_cols = ["clob_token_ids", "outcomes", "outcome_prices"]
        
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in bool_cols:
                df_prepared[col] = df[col].apply(to_bool)
            elif col_lower in numeric_cols:
                df_prepared[col] = df[col].apply(to_numeric)
            elif col_lower in timestamp_cols:
                df_prepared[col] = df[col].apply(to_timestamp)
            elif col_lower in json_cols:
                df_prepared[col] = df[col].apply(to_json)
            else:
                df_prepared[col] = df[col].apply(to_str)
        
        # Add download_date if missing
        if "download_date" not in df_prepared.columns:
            df_prepared["download_date"] = download_date

        # For gamma_markets, restrict to table columns only (snake_case)
        if table_name == "gamma_markets":
            _gamma_cols = [
                "condition_id", "question_id", "question", "description", "market_slug", "category",
                "active", "closed", "archived", "accepting_orders", "restricted",
                "volume", "volume_num", "volume_24hr",
                "volume_1wk", "volume_1mo", "volume_1yr",
                "volume_1wk_amm", "volume_1mo_amm", "volume_1yr_amm",
                "volume_1wk_clob", "volume_1mo_clob", "volume_1yr_clob",
                "liquidity", "liquidity_num", "liquidity_amm", "liquidity_clob",
                "open_interest", "competitive", "spread",
                "one_day_price_change", "one_hour_price_change", "one_week_price_change",
                "one_month_price_change", "one_year_price_change",
                "last_trade_price", "best_bid", "best_ask",
                "image", "icon", "end_date_iso",
                "clob_token_ids", "outcomes", "outcome_prices",
                "token_0_id", "token_0_outcome", "token_0_price", "token_0_winner",
                "token_1_id", "token_1_outcome", "token_1_price", "token_1_winner",
                "download_date",
            ]
            _existing = [c for c in _gamma_cols if c in df_prepared.columns]
            df_prepared = df_prepared[_existing].copy()
        
        # Batch insert
        batch_size = 10000
        total_records = len(df_prepared)
        inserted = 0

        if table_name == "gamma_markets":
            try:
                with engine.connect() as conn:
                    conn.execute(text("TRUNCATE TABLE gamma_markets RESTART IDENTITY"))
                    conn.commit()
                logger.info("Truncated gamma_markets (daily refresh), inserting fresh data")
            except SQLAlchemyError as e:
                logger.error(f"Failed to truncate gamma_markets: {e}")
                raise
        
        try:
            logger.info(f"Inserting {total_records} records in batches of {batch_size}...")
            pbar = tqdm(
                desc="Uploading records",
                total=total_records,
                unit=" records",
                dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )
            
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = df_prepared.iloc[start_idx:end_idx]
                
                batch_df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000,
                )
                
                inserted += len(batch_df)
                pbar.update(len(batch_df))
            
            pbar.close()
            logger.info(f"✓ Inserted {inserted} records into {table_name} in database {db_name} on {self.db_host}:{self.db_port}")
            return inserted
        except SQLAlchemyError as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise

    def upload_csv_to_trade_data(
        self,
        csv_path: str,
        db_name: str = "polymarket",
        table_name: str = "trade_data"
    ) -> int:
        """
        Upload CSV data to the trade_data table.
        Wrapper around upload_csv_to_table() for backward compatibility.

        Args:
            csv_path: Path to the CSV file
            db_name: Database name (default: "polymarket")
            table_name: Table name (default: "trade_data")

        Returns:
            Number of records inserted
        """
        return self.upload_csv_to_table(csv_path, db_name, table_name)

    def upload_csv_to_gamma_markets(
        self,
        csv_path: str,
        db_name: str = "polymarket",
        table_name: str = "gamma_markets",
    ) -> int:
        """
        Upload CSV data to the gamma_markets table.
        CSV must contain snake_case columns (condition_id, question_id, market_slug, volume_24hr, etc.).

        Args:
            csv_path: Path to the CSV file
            db_name: Database name (default: "polymarket")
            table_name: Table name (default: "gamma_markets")

        Returns:
            Number of records inserted
        """
        return self.upload_csv_to_table(csv_path, db_name, table_name)
