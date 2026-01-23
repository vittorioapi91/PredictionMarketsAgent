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

    def save_markets_to_csv(self, markets: list, filename: str):
        """
        Save markets data to CSV file with all available fields.

        Args:
            markets: List of market dictionaries
            filename: Output CSV file path
        """
        data = []
        for market in markets:
            # Handle tags - ensure we have a list even if tags is None
            tags = market.get("tags", [])
            tags = tags if isinstance(tags, list) else []

            row = {
                # Market identification
                "condition_id": market.get("condition_id", ""),
                "question_id": market.get("question_id", ""),
                "question": self.clean_text(market.get("question", "")),
                "description": self.clean_text(market.get("description", "")),
                "market_slug": market.get("market_slug", ""),
                "category": self.clean_text(market.get("category", "")),
                # Status flags
                "active": market.get("active", ""),
                "closed": market.get("closed", ""),
                "archived": market.get("archived", ""),
                "accepting_orders": market.get("accepting_orders", ""),
                "accepting_order_timestamp": market.get(
                    "accepting_order_timestamp", ""
                ),
                "enable_order_book": market.get("enable_order_book", ""),
                # Market parameters
                "minimum_order_size": market.get("minimum_order_size", ""),
                "minimum_tick_size": market.get("minimum_tick_size", ""),
                "min_incentive_size": market.get("min_incentive_size", ""),
                "max_incentive_spread": market.get("max_incentive_spread", ""),
                "maker_base_fee": market.get("maker_base_fee", ""),
                "taker_base_fee": market.get("taker_base_fee", ""),
                # Timing information
                "end_date_iso": market.get("end_date_iso", ""),
                "game_start_time": market.get("game_start_time", ""),
                "seconds_delay": market.get("seconds_delay", ""),
                # Contract information
                "fpmm": market.get("fpmm", ""),
                "icon": market.get("icon", ""),
                "image": market.get("image", ""),
                # Risk parameters
                "neg_risk": market.get("neg_risk", ""),
                "neg_risk_market_id": market.get("neg_risk_market_id", ""),
                "neg_risk_request_id": market.get("neg_risk_request_id", ""),
                "is_50_50_outcome": market.get("is_50_50_outcome", ""),
                # Token 0 information
                "token_0_id": (
                    market["tokens"][0].get("token_id", "")
                    if market.get("tokens")
                    else ""
                ),
                "token_0_outcome": (
                    self.clean_text(market["tokens"][0].get("outcome", ""))
                    if market.get("tokens")
                    else ""
                ),
                "token_0_price": (
                    market["tokens"][0].get("price", "") if market.get("tokens") else ""
                ),
                "token_0_winner": (
                    market["tokens"][0].get("winner", "")
                    if market.get("tokens")
                    else ""
                ),
                # Token 1 information
                "token_1_id": (
                    market["tokens"][1].get("token_id", "")
                    if len(market.get("tokens", [])) > 1
                    else ""
                ),
                "token_1_outcome": (
                    self.clean_text(market["tokens"][1].get("outcome", ""))
                    if len(market.get("tokens", [])) > 1
                    else ""
                ),
                "token_1_price": (
                    market["tokens"][1].get("price", "")
                    if len(market.get("tokens", [])) > 1
                    else ""
                ),
                "token_1_winner": (
                    market["tokens"][1].get("winner", "")
                    if len(market.get("tokens", [])) > 1
                    else ""
                ),
                # Rewards information
                "rewards_rates": (
                    str(market.get("rewards", {}).get("rates", ""))
                    if market.get("rewards")
                    else ""
                ),
                "rewards_min_size": (
                    str(market.get("rewards", {}).get("min_size", ""))
                    if market.get("rewards")
                    else ""
                ),
                "rewards_max_spread": (
                    str(market.get("rewards", {}).get("max_spread", ""))
                    if market.get("rewards")
                    else ""
                ),
                # Notifications and tags
                "notifications_enabled": market.get("notifications_enabled", ""),
                "tags": ",".join(tags),
            }
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
