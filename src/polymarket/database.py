"""Database operations for PredictionMarketsAgent"""

import os
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Boolean, Float, Integer, DateTime, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

try:
    from src.utils import load_environment_file, get_environment
except ImportError:
    from utils import load_environment_file, get_environment

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""

    def __init__(self):
        """Initialize database connection"""
        load_environment_file()
        self.env = get_environment()
        
        # Get database credentials from environment
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", f"{self.env}.PredictionMarketsAgent")
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "")
        
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
            logger.info(f"Connected to database: {self.db_name}")
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

    def insert_markets(self, markets: List[Dict[str, Any]]) -> int:
        """
        Insert or update markets in the database.

        Args:
            markets: List of market dictionaries

        Returns:
            Number of markets inserted/updated
        """
        if not markets:
            return 0

        if self.engine is None:
            raise RuntimeError("Database engine not initialized. Call _connect() first.")
        
        inserted = 0
        try:
            with self.engine.connect() as conn:
                for market in markets:
                    # Prepare data
                    tags = market.get("tags", [])
                    tags = tags if isinstance(tags, list) else []
                    
                    data = {
                        "condition_id": market.get("condition_id"),
                        "question_id": market.get("question_id"),
                        "question": market.get("question", ""),
                        "description": market.get("description", ""),
                        "market_slug": market.get("market_slug", ""),
                        "category": market.get("category", ""),
                        "active": market.get("active"),
                        "closed": market.get("closed"),
                        "archived": market.get("archived"),
                        "accepting_orders": market.get("accepting_orders"),
                        "accepting_order_timestamp": market.get("accepting_order_timestamp"),
                        "enable_order_book": market.get("enable_order_book"),
                        "minimum_order_size": market.get("minimum_order_size"),
                        "minimum_tick_size": market.get("minimum_tick_size"),
                        "min_incentive_size": market.get("min_incentive_size"),
                        "max_incentive_spread": market.get("max_incentive_spread"),
                        "maker_base_fee": market.get("maker_base_fee"),
                        "taker_base_fee": market.get("taker_base_fee"),
                        "end_date_iso": market.get("end_date_iso"),
                        "game_start_time": market.get("game_start_time"),
                        "seconds_delay": market.get("seconds_delay"),
                        "fpmm": market.get("fpmm", ""),
                        "icon": market.get("icon", ""),
                        "image": market.get("image", ""),
                        "neg_risk": market.get("neg_risk"),
                        "neg_risk_market_id": market.get("neg_risk_market_id", ""),
                        "neg_risk_request_id": market.get("neg_risk_request_id", ""),
                        "is_50_50_outcome": market.get("is_50_50_outcome"),
                        "token_0_id": market.get("tokens", [{}])[0].get("token_id", "") if market.get("tokens") else "",
                        "token_0_outcome": market.get("tokens", [{}])[0].get("outcome", "") if market.get("tokens") else "",
                        "token_0_price": market.get("tokens", [{}])[0].get("price") if market.get("tokens") else None,
                        "token_0_winner": market.get("tokens", [{}])[0].get("winner") if market.get("tokens") else None,
                        "token_1_id": market.get("tokens", [{}])[1].get("token_id", "") if len(market.get("tokens", [])) > 1 else "",
                        "token_1_outcome": market.get("tokens", [{}])[1].get("outcome", "") if len(market.get("tokens", [])) > 1 else "",
                        "token_1_price": market.get("tokens", [{}])[1].get("price") if len(market.get("tokens", [])) > 1 else None,
                        "token_1_winner": market.get("tokens", [{}])[1].get("winner") if len(market.get("tokens", [])) > 1 else None,
                        "rewards_rates": str(market.get("rewards", {}).get("rates", "")) if market.get("rewards") else "",
                        "rewards_min_size": str(market.get("rewards", {}).get("min_size", "")) if market.get("rewards") else "",
                        "rewards_max_spread": str(market.get("rewards", {}).get("max_spread", "")) if market.get("rewards") else "",
                        "notifications_enabled": market.get("notifications_enabled"),
                        "tags": ",".join(tags),
                        "updated_at": datetime.now()
                    }
                    
                    # Use INSERT ... ON CONFLICT to upsert
                    conn.execute(text("""
                        INSERT INTO markets (
                            condition_id, question_id, question, description, market_slug, category,
                            active, closed, archived, accepting_orders, accepting_order_timestamp,
                            enable_order_book, minimum_order_size, minimum_tick_size, min_incentive_size,
                            max_incentive_spread, maker_base_fee, taker_base_fee, end_date_iso,
                            game_start_time, seconds_delay, fpmm, icon, image, neg_risk,
                            neg_risk_market_id, neg_risk_request_id, is_50_50_outcome,
                            token_0_id, token_0_outcome, token_0_price, token_0_winner,
                            token_1_id, token_1_outcome, token_1_price, token_1_winner,
                            rewards_rates, rewards_min_size, rewards_max_spread,
                            notifications_enabled, tags, updated_at
                        ) VALUES (
                            :condition_id, :question_id, :question, :description, :market_slug, :category,
                            :active, :closed, :archived, :accepting_orders, :accepting_order_timestamp,
                            :enable_order_book, :minimum_order_size, :minimum_tick_size, :min_incentive_size,
                            :max_incentive_spread, :maker_base_fee, :taker_base_fee, :end_date_iso,
                            :game_start_time, :seconds_delay, :fpmm, :icon, :image, :neg_risk,
                            :neg_risk_market_id, :neg_risk_request_id, :is_50_50_outcome,
                            :token_0_id, :token_0_outcome, :token_0_price, :token_0_winner,
                            :token_1_id, :token_1_outcome, :token_1_price, :token_1_winner,
                            :rewards_rates, :rewards_min_size, :rewards_max_spread,
                            :notifications_enabled, :tags, :updated_at
                        )
                        ON CONFLICT (condition_id) DO UPDATE SET
                            question_id = EXCLUDED.question_id,
                            question = EXCLUDED.question,
                            description = EXCLUDED.description,
                            market_slug = EXCLUDED.market_slug,
                            category = EXCLUDED.category,
                            active = EXCLUDED.active,
                            closed = EXCLUDED.closed,
                            archived = EXCLUDED.archived,
                            accepting_orders = EXCLUDED.accepting_orders,
                            accepting_order_timestamp = EXCLUDED.accepting_order_timestamp,
                            enable_order_book = EXCLUDED.enable_order_book,
                            minimum_order_size = EXCLUDED.minimum_order_size,
                            minimum_tick_size = EXCLUDED.minimum_tick_size,
                            min_incentive_size = EXCLUDED.min_incentive_size,
                            max_incentive_spread = EXCLUDED.max_incentive_spread,
                            maker_base_fee = EXCLUDED.maker_base_fee,
                            taker_base_fee = EXCLUDED.taker_base_fee,
                            end_date_iso = EXCLUDED.end_date_iso,
                            game_start_time = EXCLUDED.game_start_time,
                            seconds_delay = EXCLUDED.seconds_delay,
                            fpmm = EXCLUDED.fpmm,
                            icon = EXCLUDED.icon,
                            image = EXCLUDED.image,
                            neg_risk = EXCLUDED.neg_risk,
                            neg_risk_market_id = EXCLUDED.neg_risk_market_id,
                            neg_risk_request_id = EXCLUDED.neg_risk_request_id,
                            is_50_50_outcome = EXCLUDED.is_50_50_outcome,
                            token_0_id = EXCLUDED.token_0_id,
                            token_0_outcome = EXCLUDED.token_0_outcome,
                            token_0_price = EXCLUDED.token_0_price,
                            token_0_winner = EXCLUDED.token_0_winner,
                            token_1_id = EXCLUDED.token_1_id,
                            token_1_outcome = EXCLUDED.token_1_outcome,
                            token_1_price = EXCLUDED.token_1_price,
                            token_1_winner = EXCLUDED.token_1_winner,
                            rewards_rates = EXCLUDED.rewards_rates,
                            rewards_min_size = EXCLUDED.rewards_min_size,
                            rewards_max_spread = EXCLUDED.rewards_max_spread,
                            notifications_enabled = EXCLUDED.notifications_enabled,
                            tags = EXCLUDED.tags,
                            updated_at = EXCLUDED.updated_at
                    """), data)
                    inserted += 1
                conn.commit()
            logger.info(f"Inserted/updated {inserted} markets in database")
            return inserted
        except SQLAlchemyError as e:
            logger.error(f"Error inserting markets: {str(e)}")
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
