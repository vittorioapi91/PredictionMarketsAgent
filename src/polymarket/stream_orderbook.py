"""
Stream order book data in real-time via WebSocket.
The WebSocket sends full L2 order book updates (book events) and price change events.
Supports streaming by token_id or condition_id.
"""

import signal
import sys
import time
import logging
import argparse
import pandas as pd
from datetime import datetime
from typing import cast, Optional, List
from src.polymarket import PolymarketClient
from src.utils import load_environment_file

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def format_order_book_levels(levels, max_levels=10):
    """Format order book levels for display"""
    if not levels:
        return "No orders"
    
    formatted = []
    for level in levels[:max_levels]:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            price = level[0]
            size = level[1]
            formatted.append(f"{price}@{size}")
        elif isinstance(level, dict):
            # Handle OrderSummary objects from WebSocket: {"price": "0.48", "size": "30"}
            price = level.get("price", "0")
            size = level.get("size", "0")
            formatted.append(f"{price}@{size}")
        elif hasattr(level, "price") and hasattr(level, "size"):
            # Handle OrderSummary objects from REST API
            # Type checker: at this point level is not a list/tuple, so accessing attributes is safe
            price = getattr(level, "price", "0")
            size = getattr(level, "size", "0")
            formatted.append(f"{price}@{size}")
        else:
            formatted.append(str(level))
    
    if len(levels) > max_levels:
        formatted.append(f"... ({len(levels) - max_levels} more)")
    
    return ", ".join(formatted)


def get_token_ids_from_condition_id(condition_id: str, csv_path: str = "storage/test/raw_data/polymarket_data_20260121.csv") -> tuple[Optional[List[str]], Optional[str], Optional[float]]:
    """Get token_ids, question, and tick_size for a condition_id from CSV"""
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        filtered = df[
            (df["active"] == True)
            & (df["closed"] == False)
            & (df["archived"] == False)
            & (df["accepting_orders"] == True)
        ]
        market_filtered = cast(pd.DataFrame, filtered[filtered["condition_id"] == condition_id])
        
        if market_filtered.empty:
            return None, None, None
        
        market = market_filtered.iloc[0]
        token_ids = [t for t in [market.get("token_0_id"), market.get("token_1_id")] if pd.notna(t) and t]
        question = market.get("question", "Unknown market")
        # Get tick_size, fallback to 0.0001 if not available
        tick_size = float(market.get("minimum_tick_size", 0.0001)) if pd.notna(market.get("minimum_tick_size")) else 0.0001
        return token_ids, question, tick_size
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        return None, None, None
    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        return None, None, None


def main():
    """Main entry point for order book streaming"""
    parser = argparse.ArgumentParser(
        description="Stream order book data for Polymarket tokens in real-time via WebSocket"
    )
    parser.add_argument(
        "--token-id",
        help="Token ID to stream order book for (can specify multiple times)",
        action="append",
        dest="token_ids",
    )
    parser.add_argument(
        "--condition-id",
        help="Condition ID (market) to stream order books for (finds token_ids from CSV)",
    )
    parser.add_argument(
        "--csv-path",
        help="Path to market data CSV (default: storage/test/raw_data/polymarket_data_20260121.csv)",
        default="storage/test/raw_data/polymarket_data_20260121.csv",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Streaming duration in seconds (default: 60, use 0 for infinite)",
    )
    parser.add_argument(
        "--no-initial-snapshot",
        action="store_true",
        help="Skip fetching initial order book snapshot via REST API",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show all price change events (default: show first 5 and every 10th)",
    )

    args = parser.parse_args()

    # Load environment
    load_environment_file()

    # Determine token_ids
    token_ids: List[str] = []
    question: Optional[str] = None
    condition_id: Optional[str] = None
    tick_size: float = 0.0001  # Default fallback

    if args.condition_id:
        condition_id = args.condition_id
        found_token_ids, found_question, found_tick_size = get_token_ids_from_condition_id(str(condition_id), args.csv_path)
        if not found_token_ids:
            logger.error(f"Could not find token_ids for condition_id: {condition_id}")
            sys.exit(1)
        token_ids = found_token_ids
        question = found_question
        if found_tick_size is not None:
            tick_size = found_tick_size
    elif args.token_ids:
        token_ids = args.token_ids
    else:
        parser.error("Must specify either --token-id or --condition-id")

    # Initialize client
    client = PolymarketClient()

    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        logger.info("\nStopping stream...")
        client.stop_streaming()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Display header
    logger.info("=" * 120)
    if condition_id:
        logger.info(f"Streaming order book for condition_id: {condition_id}")
        if question:
            logger.info(f"Question: {question[:80]}")
        logger.info(f"Tick size: {tick_size}")
    else:
        logger.info(f"Streaming order book for {len(token_ids)} token(s)")
        logger.info(f"Tick size: {tick_size} (default)")
    logger.info(f"Token IDs: {', '.join([t[:20] + '...' if len(t) > 20 else t for t in token_ids])}")
    if args.duration > 0:
        logger.info(f"Duration: {args.duration} seconds")
    else:
        logger.info("Duration: infinite (Ctrl+C to stop)")
    logger.info("=" * 120)
    logger.info("")

    # Fetch initial order books via REST API (unless disabled)
    if not args.no_initial_snapshot:
        logger.info("Fetching initial order books via REST API...")
        for token_id in token_ids:
            order_book = client.get_order_book(token_id)
            if order_book:
                logger.info(f"\n📖 Initial Order Book for Token: {token_id[:20]}...")
                if hasattr(order_book, "bids") and order_book.bids:
                    best_bid_price = order_book.bids[0].price if hasattr(order_book.bids[0], "price") else order_book.bids[0][0] if isinstance(order_book.bids[0], (list, tuple)) else "N/A"
                    best_bid_size = order_book.bids[0].size if hasattr(order_book.bids[0], "size") else order_book.bids[0][1] if isinstance(order_book.bids[0], (list, tuple)) else "N/A"
                    logger.info(f"  Best Bid: {best_bid_price}@{best_bid_size}")
                    logger.info(f"  Bids ({len(order_book.bids)} levels): {format_order_book_levels(order_book.bids, max_levels=5)}")
                if hasattr(order_book, "asks") and order_book.asks:
                    best_ask_price = order_book.asks[0].price if hasattr(order_book.asks[0], "price") else order_book.asks[0][0] if isinstance(order_book.asks[0], (list, tuple)) else "N/A"
                    best_ask_size = order_book.asks[0].size if hasattr(order_book.asks[0], "size") else order_book.asks[0][1] if isinstance(order_book.asks[0], (list, tuple)) else "N/A"
                    logger.info(f"  Best Ask: {best_ask_price}@{best_ask_size}")
                    logger.info(f"  Asks ({len(order_book.asks)} levels): {format_order_book_levels(order_book.asks, max_levels=5)}")
        logger.info("")
        logger.info("-" * 120)
        logger.info("Now streaming real-time updates via WebSocket:")
        logger.info("  - Full order book updates (book events) when trades affect the book")
        logger.info("  - Price change events when orders are placed/cancelled")
        logger.info("-" * 120)
        logger.info("")

    # Statistics
    book_count = {"count": 0}
    price_count = {"count": 0}
    # Track previous prices per token to filter out unchanged prices
    previous_prices = {}

    def on_update(order_book_data: dict, timestamp: datetime):
        """Callback for order book updates"""
        event_type = order_book_data.get("event_type", "unknown")
        token_id = order_book_data.get("token_id", "unknown")
        bids = order_book_data.get("bids", [])
        asks = order_book_data.get("asks", [])
        best_bid = order_book_data.get("best_bid", "N/A")
        best_ask = order_book_data.get("best_ask", "N/A")
        price = order_book_data.get("price", "N/A")
        size = order_book_data.get("size", "N/A")

        time_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
        token_short = token_id[:16] + "..." if len(token_id) > 16 else token_id

        if event_type == "book":
            book_count["count"] += 1
            logger.info("")
            logger.info(f"[{time_str}] 📖 BOOK (WebSocket) #{book_count['count']} | Token: {token_short}")
            logger.info(f"  Bids ({len(bids)} levels): {format_order_book_levels(bids, max_levels=10)}")
            logger.info(f"  Asks ({len(asks)} levels): {format_order_book_levels(asks, max_levels=10)}")
            if bids:
                if isinstance(bids[0], (list, tuple)):
                    bid_price = bids[0][0]
                    bid_size = bids[0][1] if len(bids[0]) > 1 else "N/A"
                elif isinstance(bids[0], dict):
                    bid_price = bids[0].get("price", "N/A")
                    bid_size = bids[0].get("size", "N/A")
                else:
                    bid_price = bids[0].price if hasattr(bids[0], "price") else str(bids[0])
                    bid_size = bids[0].size if hasattr(bids[0], "size") else "N/A"
                logger.info(f"  Best Bid: {bid_price}@{bid_size}")
            if asks:
                if isinstance(asks[0], (list, tuple)):
                    ask_price = asks[0][0]
                    ask_size = asks[0][1] if len(asks[0]) > 1 else "N/A"
                elif isinstance(asks[0], dict):
                    ask_price = asks[0].get("price", "N/A")
                    ask_size = asks[0].get("size", "N/A")
                else:
                    ask_price = asks[0].price if hasattr(asks[0], "price") else str(asks[0])
                    ask_size = asks[0].size if hasattr(asks[0], "size") else "N/A"
                logger.info(f"  Best Ask: {ask_price}@{ask_size}")
        else:
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
                
                price_count["count"] += 1
                should_log = args.verbose or price_count["count"] <= 5 or price_count["count"] % 10 == 0
                if should_log:
                    logger.info(
                        f"[{time_str}] 💰 PRICE #{price_count['count']} | Token: {token_short} | "
                        f"Best Bid: {best_bid} | Best Ask: {best_ask} | Price: {price} | Size: {size}"
                    )
            except (ValueError, TypeError):
                # If we can't parse the price, skip this update
                return

    # Start streaming
    try:
        duration = args.duration if args.duration > 0 else None
        client.stream_order_books(token_ids, on_update, duration=duration)

        # Keep main thread alive
        if duration is None:
            # Infinite stream - wait for interrupt
            try:
                while client._streaming_active:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                signal_handler(None, None)
        else:
            # Wait for duration
            time.sleep(duration)
            client.stop_streaming()

        # Log summary
        logger.info("")
        logger.info("-" * 120)
        logger.info(f"Summary:")
        logger.info(f"  BOOK events received: {book_count['count']}")
        logger.info(f"  PRICE_CHANGE events received: {price_count['count']}")
        logger.info("=" * 120)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        client.stop_streaming()
        sys.exit(1)


if __name__ == "__main__":
    main()
