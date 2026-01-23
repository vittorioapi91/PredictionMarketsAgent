"""
Stream price changes for a condition_id and display them on screen.
"""

import sys
import time
import logging
import pandas as pd
from datetime import datetime
from typing import Optional, cast
from src.polymarket import PolymarketClient
from src.utils import load_environment_file

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main():
    """Stream price changes for a condition_id"""
    load_environment_file()
    client = PolymarketClient()

    # Get token_ids for condition_id
    condition_id = "0x2393ed0b0fdc450054c7b9071907eca75cf4fc36e385adf4a0a5f99ee62243e8"
    
    df = pd.read_csv("storage/test/raw_data/polymarket_data_20260121.csv", low_memory=False)
    filtered = df[
        (df["active"] == True)
        & (df["closed"] == False)
        & (df["archived"] == False)
        & (df["accepting_orders"] == True)
    ]
    market_filtered = cast(pd.DataFrame, filtered[filtered["condition_id"] == condition_id])
    if len(market_filtered) > 0:
        market = market_filtered.iloc[0]
    else:
        market = None

    if market is None:
        logger.error("Market not found")
        return

    token_ids = [t for t in [market["token_0_id"], market["token_1_id"]] if pd.notna(t) and t]
    question = market.get("question", "Unknown market")[:60]
    # Get tick_size from market data, fallback to 0.0001 if not available
    tick_size = float(market.get("minimum_tick_size", 0.0001)) if pd.notna(market.get("minimum_tick_size")) else 0.0001

    logger.info("=" * 100)
    logger.info(f"Streaming price changes for 60 seconds")
    logger.info(f"Condition ID: {condition_id}")
    logger.info(f"Question: {question}")
    logger.info(f"Token IDs: {len(token_ids)} tokens")
    logger.info(f"Tick size: {tick_size}")
    logger.info("=" * 100)
    logger.info("")
    logger.info(
        f"{'Time':<12} | {'Token':<20} | {'Best Bid':<12} | {'Best Ask':<12} | {'Price':<12} | {'Size':<12} | {'Side':<6}"
    )
    logger.info("-" * 100)

    update_count = {"count": 0}
    # Track previous prices per token to filter out unchanged prices
    previous_prices = {}

    def on_update(order_book_data, timestamp):
        token_id = order_book_data.get("token_id", "unknown")
        best_bid = order_book_data.get("best_bid", "N/A")
        best_ask = order_book_data.get("best_ask", "N/A")
        price = order_book_data.get("price", "N/A")
        size = order_book_data.get("size", "N/A")
        side = order_book_data.get("side", "N/A")

        # Only show price_change events if the price actually changed
        try:
            current_price = float(price) if price != "N/A" and price is not None else None
            prev_price = previous_prices.get(token_id)
            
            # Check if price actually changed
            price_changed = False
            if current_price is not None:
                if prev_price is None:
                    # First time seeing this token - show it
                    price_changed = True
                elif abs(current_price - prev_price) >= tick_size:  # Use tick_size as threshold
                    price_changed = True
                
                if price_changed:
                    previous_prices[token_id] = current_price
            else:
                # Can't determine if price changed, skip
                return
            
            if not price_changed:
                # Price hasn't changed, don't show this update
                return
            
            update_count["count"] += 1
            time_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
            token_short = token_id[:18] + "..." if len(token_id) > 18 else token_id

            logger.info(
                f"{time_str:<12} | {token_short:<20} | {str(best_bid):<12} | {str(best_ask):<12} | {str(price):<12} | {str(size):<12} | {str(side):<6}"
            )
        except (ValueError, TypeError):
            # If we can't parse the price, skip this update
            return

    client.stream_order_books(token_ids, on_update, duration=60)
    time.sleep(60)
    client.stop_streaming()

    logger.info("")
    logger.info("-" * 100)
    logger.info(f"Total updates received: {update_count['count']}")
    logger.info("=" * 100)


if __name__ == "__main__":
    main()
