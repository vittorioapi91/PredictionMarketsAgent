"""Market data processing and storage"""

import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from src.utils import get_storage_path
except ImportError:
    from utils import get_storage_path

logger = logging.getLogger(__name__)


class MarketDataProcessor:
    """Process and manage market data"""

    def __init__(self):
        """Initialize the market data processor"""
        pass

    @staticmethod
    def clean_text(text):
        """
        Clean text fields for CSV output.

        Args:
            text: Text to clean

        Returns:
            Cleaned text string
        """
        if not text:
            return ""
        # Replace newlines and multiple spaces with single space
        text = " ".join(text.split())
        # Remove any problematic characters
        text = text.replace('"', '""')  # Escape quotes
        return text

    @staticmethod
    def filter_open_markets(markets_data: list) -> list:
        """
        Filter markets to keep only those that are open and accepting orders.

        Args:
            markets_data: List of market dictionaries

        Returns:
            Filtered list of open markets
        """
        open_markets = []
        for market in markets_data:
            if (
                market.get("closed") is False
                and market.get("accepting_orders") is True
                and market.get("active") is True
            ):
                open_markets.append(market)
        return open_markets

    def save_markets_to_csv(self, markets: list, filename: str, source: str = ""):
        """
        Save markets data to CSV file with all available fields.
        Generic method that handles any market format (clob-api, etc.).

        Args:
            markets: List of market dictionaries
            filename: Output CSV file path
            source: Source of the data (e.g., "clob-api")
        """
        import json
        
        data = []
        for market in markets:
            row = {}
            
            # Fields that should be cleaned (text fields)
            text_fields = ["question", "description", "category", "token_0_outcome", "token_1_outcome"]
            
            # Fields that should be converted to JSON strings (lists/dicts)
            json_fields = ["clob_token_ids", "outcomes", "outcome_prices", "tokens", "rewards"]
            
            # Process all fields in the market dictionary
            for key, value in market.items():
                # Skip nested structures that we'll handle separately
                if key in ["tokens", "rewards"]:
                    continue
                
                # Handle JSON fields
                if key in json_fields:
                    if value:
                        try:
                            row[key] = json.dumps(value) if not isinstance(value, str) else value
                        except (TypeError, ValueError):
                            row[key] = str(value)
                    else:
                        row[key] = ""
                # Handle text fields that need cleaning
                elif key in text_fields:
                    row[key] = self.clean_text(value) if value else ""
                # Handle tags specially (convert list to comma-separated string)
                elif key == "tags":
                    tags = value if isinstance(value, list) else (value if value else [])
                    row[key] = ",".join(str(t) for t in tags) if tags else ""
                # Handle all other fields
                else:
                    if value is None:
                        row[key] = ""
                    elif isinstance(value, (dict, list)):
                        # Convert complex types to JSON
                        try:
                            row[key] = json.dumps(value)
                        except (TypeError, ValueError):
                            row[key] = str(value)
                    else:
                        row[key] = value
            
            # Handle tokens array (flatten to token_0_*, token_1_* fields)
            if "tokens" in market and market.get("tokens"):
                tokens = market["tokens"]
                if len(tokens) > 0:
                    row["token_0_id"] = tokens[0].get("token_id", "")
                    row["token_0_outcome"] = self.clean_text(tokens[0].get("outcome", ""))
                    row["token_0_price"] = tokens[0].get("price", "")
                    row["token_0_winner"] = tokens[0].get("winner", "")
                if len(tokens) > 1:
                    row["token_1_id"] = tokens[1].get("token_id", "")
                    row["token_1_outcome"] = self.clean_text(tokens[1].get("outcome", ""))
                    row["token_1_price"] = tokens[1].get("price", "")
                    row["token_1_winner"] = tokens[1].get("winner", "")
            
            # Handle rewards dict (flatten to rewards_* fields)
            if "rewards" in market and market.get("rewards"):
                rewards = market["rewards"]
                row["rewards_rates"] = str(rewards.get("rates", "")) if rewards.get("rates") else ""
                row["rewards_min_size"] = str(rewards.get("min_size", "")) if rewards.get("min_size") else ""
                row["rewards_max_spread"] = str(rewards.get("max_spread", "")) if rewards.get("max_spread") else ""
            
            
            data.append(row)

        # Convert to DataFrame and save to CSV
        df = pd.DataFrame(data)
        df.to_csv(
            filename,
            index=False,
            encoding="utf-8",
            quoting=1,  # Quote all fields
            escapechar="\\",  # Use backslash as escape character
            doublequote=True,  # Double up quotes for escaping
        )
        logger.info(f"Data saved to {filename}")

    def load_markets_from_csv(self, filename: str) -> list:
        """
        Load markets data from CSV file.

        Args:
            filename: Input CSV file path

        Returns:
            List of market dictionaries
        """
        return pd.read_csv(filename).to_dict("records")

    def get_output_path(self, subpath: str, date: Optional[str] = None) -> str:
        """
        Get environment-specific output file path.

        Args:
            subpath: Subdirectory (e.g., 'raw_data', 'open_markets')
            date: Date string in YYYYMMDD format (default: today)

        Returns:
            Full path to output file
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        storage_dir = get_storage_path(subpath)
        Path(storage_dir).mkdir(parents=True, exist_ok=True)

        if subpath == "raw_data":
            filename = f"polymarket_data_{date}.csv"
        elif subpath == "open_markets":
            filename = f"open_markets_{date}.csv"
        elif subpath == "order_books":
            filename = f"order_books_{date}.csv"
        else:
            filename = f"markets_{date}.csv"

        return os.path.join(storage_dir, filename)

    def save_order_books_to_csv(
        self, order_books_data: list, filename: str
    ):
        """
        Save order books data to CSV file.

        Args:
            order_books_data: List of dictionaries with market, token, and order_book
            filename: Output CSV file path
        """
        data = []
        for item in order_books_data:
            market = item.get("market", {})
            token = item.get("token", {})
            order_book = item.get("order_book")

            # Extract order book data
            bids = []
            asks = []
            if order_book:
                # OrderBookSummary typically has bids and asks
                if hasattr(order_book, "bids"):
                    bids = order_book.bids if order_book.bids else []
                if hasattr(order_book, "asks"):
                    asks = order_book.asks if order_book.asks else []

            row = {
                "condition_id": market.get("condition_id", ""),
                "question_id": market.get("question_id", ""),
                "token_id": token.get("token_id", ""),
                "outcome": self.clean_text(token.get("outcome", "")),
                "bids_count": len(bids),
                "asks_count": len(asks),
                "bids": str(bids) if bids else "",
                "asks": str(asks) if asks else "",
            }
            data.append(row)

        df = pd.DataFrame(data)
        df.to_csv(
            filename,
            index=False,
            encoding="utf-8",
            quoting=1,
            escapechar="\\",
            doublequote=True,
        )
        logger.info(f"Order books data saved to {filename}")

    def setup_directories(self):
        """Create necessary directories if they don't exist"""
        raw_data_dir = get_storage_path("raw_data")
        open_markets_dir = get_storage_path("open_markets")
        order_books_dir = get_storage_path("order_books")
        env_storage_dir = get_storage_path()

        for directory in [
            env_storage_dir,
            raw_data_dir,
            open_markets_dir,
            order_books_dir,
        ]:
            Path(directory).mkdir(parents=True, exist_ok=True)
