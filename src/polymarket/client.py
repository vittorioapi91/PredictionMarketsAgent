"""Polymarket API client for fetching market data"""

import os
import time
import json
import ssl
import logging
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
from dotenv import load_dotenv
from tqdm import tqdm
import websocket
import requests
from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams

logger = logging.getLogger(__name__)


class PolymarketClient:
    """Client for interacting with Polymarket API"""

    def __init__(
        self, host: str = "https://clob.polymarket.com", chain_id: int = POLYGON
    ):
        """
        Initialize the Polymarket client.

        Args:
            host: Polymarket API host URL
            chain_id: Blockchain chain ID (default: POLYGON)
        """
        self.host = host
        self.chain_id = chain_id
        self.client: ClobClient = None  # type: ignore
        self._streaming_active = False
        self._streaming_thread: Optional[threading.Thread] = None
        self._websocket: Optional[websocket.WebSocketApp] = None
        self._ws_callback: Optional[Callable] = None
        self._ws_token_id: Optional[str] = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize the ClobClient with credentials from environment"""
        from src.utils import load_environment_file
        load_environment_file()
        private_key = os.getenv("PolyMarketPrivateKey")
        if not private_key:
            raise ValueError(
                "PolyMarketPrivateKey environment variable is not set. Please set it in your .env file."
            )
        self.client = ClobClient(
            host=self.host, key=private_key, chain_id=self.chain_id
        )

    def fetch_all_markets(self) -> list:
        """
        Fetch all market data from Polymarket API with pagination support.

        Returns:
            List of all market dictionaries
        """
        all_markets = []
        next_cursor = None
        start_time = datetime.now()

        # Initialize tqdm progress bar (total unknown initially)
        pbar = tqdm(
            desc="Fetching markets",
            unit=" markets",
            initial=0,
            dynamic_ncols=True,
        )

        while True:
            try:
                response = self._fetch_page(next_cursor)

                if (
                    not response
                    or not isinstance(response, dict)
                    or not response.get("data")
                ):
                    pbar.close()
                    self._print_completion_stats(all_markets, start_time)
                    break

                markets = response.get("data", [])
                if not markets:
                    pbar.close()
                    self._print_completion_stats(all_markets, start_time)
                    break

                all_markets.extend(markets)
                # Update progress bar with current total
                pbar.update(len(markets))
                pbar.set_postfix({"total": len(all_markets)})

                next_cursor = response.get("next_cursor")

            except Exception as e:
                if "next item should be greater than or equal to 0" in str(e):
                    # This is actually a successful completion
                    pbar.close()
                    self._print_completion_stats(all_markets, start_time)
                    break
                else:
                    pbar.close()
                    logger.error(f"Error fetching page: {str(e)}")
                    break

        return all_markets

    def _fetch_page(self, cursor: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch a single page of market data.

        Args:
            cursor: Optional cursor for pagination

        Returns:
            API response dictionary
        """
        if self.client is None:
            raise RuntimeError("Client not initialized")
        if cursor is None:
            response = self.client.get_markets()
        else:
            response = self.client.get_markets(next_cursor=cursor)
        # Ensure response is a dict
        if not isinstance(response, dict):
            return {}
        return response

    def get_order_book(self, token_id: str) -> Optional[Any]:
        """
        Fetch order book for a single token.

        Args:
            token_id: Token ID to fetch order book for

        Returns:
            OrderBookSummary object or None if error
        """
        if self.client is None:
            raise RuntimeError("Client not initialized")
        try:
            return self.client.get_order_book(token_id)
        except Exception as e:
            logger.error(f"Error fetching order book for token {token_id}: {str(e)}")
            return None

    def get_order_books_for_markets(
        self, markets: List[Dict[str, Any]], max_markets: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch order books for multiple markets.

        Args:
            markets: List of market dictionaries with token information
            max_markets: Maximum number of markets to process (None for all)

        Returns:
            List of dictionaries containing market info and order book data
        """
        if self.client is None:
            raise RuntimeError("Client not initialized")

        markets_to_process = markets[:max_markets] if max_markets else markets
        order_books_data = []

        # Prepare BookParams for batch request
        book_params = []
        market_token_map = {}  # Map to track which market each token belongs to

        for market in markets_to_process:
            tokens = market.get("tokens", [])
            if not tokens:
                continue

            # Get token IDs for both sides (if available)
            for token in tokens:
                token_id = token.get("token_id")
                if token_id:
                    book_params.append(BookParams(token_id=token_id))
                    market_token_map[len(book_params) - 1] = {
                        "market": market,
                        "token": token,
                    }

        if not book_params:
            return []

        # Fetch order books in batch
        pbar = tqdm(
            desc="Fetching order books",
            total=len(book_params),
            unit=" books",
            dynamic_ncols=True,
        )

        try:
            order_books = self.client.get_order_books(book_params)

            for idx, order_book in enumerate(order_books):
                if idx in market_token_map:
                    market_info = market_token_map[idx]
                    order_books_data.append(
                        {
                            "market": market_info["market"],
                            "token": market_info["token"],
                            "order_book": order_book,
                        }
                    )
                pbar.update(1)

            pbar.close()
        except Exception as e:
            pbar.close()
            logger.error(f"Error fetching order books: {str(e)}")

        return order_books_data

    def stream_order_book(
        self,
        token_id: str,
        callback: Callable[[Dict[str, Any], datetime], None],
        interval: float = 1.0,
        duration: Optional[float] = None,
    ) -> None:
        """
        Stream order book updates for a single token in real-time using WebSocket.

        Args:
            token_id: Token ID to stream order book for
            callback: Function to call with each order book update.
                      Signature: callback(order_book_data, timestamp)
            interval: Not used (kept for compatibility, WebSocket is event-driven)
            duration: Total streaming duration in seconds (None for infinite)

        Example:
            def on_update(order_book, timestamp):
                logger.info(f"[{timestamp}] Bids: {len(order_book.get('bids', []))}, "
                           f"Asks: {len(order_book.get('asks', []))}")

            client.stream_order_book("0x123...", on_update)
        """
        if self.client is None:
            raise RuntimeError("Client not initialized")

        if self._streaming_active:
            raise RuntimeError("Streaming already active. Stop current stream first.")

        self._streaming_active = True
        self._ws_callback = callback
        self._ws_token_id = token_id
        start_time = time.time()

        # WebSocket URL - CLOB Market Channel endpoint
        # https://docs.polymarket.com/developers/CLOB/websocket/wss-overview
        ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

        def on_message(ws, message):
            """Handle incoming WebSocket messages (CLOB Market Channel format)"""
            # Handle PONG responses (heartbeat)
            if message == "PONG":
                return
            
            try:
                data = json.loads(message)
                
                # CLOB Market Channel message format
                if isinstance(data, dict):
                    # Check if this is an order book update
                    # CLOB messages may have bids/asks directly or nested
                    if "bids" in data or "asks" in data:
                        # Extract token_id from the message (may be in different fields)
                        msg_token_id = data.get("asset_id") or data.get("token_id") or token_id
                        order_book_data = {
                            "token_id": msg_token_id,
                            "timestamp": datetime.now(),
                            "bids": data.get("bids", []),
                            "asks": data.get("asks", []),
                        }
                        callback(order_book_data, order_book_data["timestamp"])
                    elif "type" in data:
                        # Handle subscription confirmations or other message types
                        if data.get("type") == "subscribed" or data.get("type") == "subscription_success":
                            logger.info(f"Subscribed to order book for token {token_id}")
                        elif data.get("type") == "error":
                            logger.error(f"WebSocket error: {data.get('message', 'Unknown error')}")
                    else:
                        # Log other message types for debugging
                        logger.debug(f"Received message: {data}")
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse WebSocket message: {message}")
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {str(e)}")

        def on_error(ws, error):
            """Handle WebSocket errors"""
            logger.error(f"WebSocket error: {str(error)}")

        def on_close(ws, close_status_code, close_msg):
            """Handle WebSocket close"""
            self._streaming_active = False
            logger.info("WebSocket connection closed")

        def on_open(ws):
            """Handle WebSocket open - subscribe to order book (CLOB Market Channel format)"""
            logger.info(f"WebSocket connected. Subscribing to order book for token {token_id}...")
            
            # CLOB Market Channel subscription format
            # https://docs.polymarket.com/developers/CLOB/websocket/wss-overview
            subscribe_message = {
                "assets_ids": [token_id],
                "type": "market"
            }
            ws.send(json.dumps(subscribe_message))
            logger.info(f"Sent subscription for token_id: {token_id}")

        def run_websocket():
            """Run WebSocket in a separate thread with reconnection logic"""
            reconnect_delay = 1.0
            max_reconnect_delay = 60.0
            update_count = 0
            
            while self._streaming_active:
                try:
                    # Check duration limit before connecting
                    if duration is not None:
                        elapsed = time.time() - start_time
                        if elapsed >= duration:
                            logger.info(f"Streaming completed after {elapsed:.1f} seconds")
                            break
                    
                    # Create WebSocket connection
                    self._websocket = websocket.WebSocketApp(
                        ws_url,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close,
                        on_open=on_open,
                    )
                    
                    # Run WebSocket with SSL (disable verification for testing)
                    self._websocket.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
                    
                    # If we get here, connection was closed
                    if self._streaming_active:
                        logger.warning(f"WebSocket disconnected. Reconnecting in {reconnect_delay:.1f}s...")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    else:
                        break
                        
                except Exception as e:
                    logger.error(f"WebSocket error: {str(e)}")
                    if self._streaming_active:
                        logger.info(f"Reconnecting in {reconnect_delay:.1f}s...")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    else:
                        break

        # Start WebSocket in background thread
        self._streaming_thread = threading.Thread(target=run_websocket, daemon=True)
        self._streaming_thread.start()
        logger.info(f"Started WebSocket streaming for token {token_id}")

    def stream_order_books(
        self,
        token_ids: List[str],
        callback: Callable[[Dict[str, Any], datetime], None],
        duration: Optional[float] = None,
    ) -> None:
        """
        Stream order book updates for multiple tokens in real-time using CLOB Market Channel WebSocket.

        Args:
            token_ids: List of token IDs to stream order books for
            callback: Function to call with each order book update.
                      Signature: callback(order_book_data, timestamp)
            duration: Total streaming duration in seconds (None for infinite)

        Example:
            def on_update(order_book, timestamp):
                logger.info(f"[{timestamp}] Token: {order_book['token_id']} | "
                           f"Bids: {len(order_book.get('bids', []))}, "
                           f"Asks: {len(order_book.get('asks', []))}")

            client.stream_order_books(["0x123...", "0x456..."], on_update)
        """
        if self.client is None:
            raise RuntimeError("Client not initialized")

        if self._streaming_active:
            raise RuntimeError("Streaming already active. Stop current stream first.")

        if not token_ids:
            raise ValueError("At least one token_id is required")

        self._streaming_active = True
        self._ws_callback = callback
        self._ws_token_id = token_ids[0]  # Store first token_id for reference
        start_time = time.time()

        # WebSocket URL - CLOB Market Channel endpoint
        ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

        def on_message(ws, message):
            """Handle incoming WebSocket messages (CLOB Market Channel format)"""
            # Handle PONG responses (heartbeat)
            if message == "PONG":
                return
            
            try:
                data = json.loads(message)
                
                # CLOB Market Channel message format
                if isinstance(data, dict):
                    # Handle "book" events - L2 order book snapshots and updates
                    # These contain full bids/asks arrays
                    # Check both camelCase (eventType) and snake_case (event_type) formats
                    event_type = data.get("eventType") or data.get("event_type", "")
                    
                    if event_type == "book":
                        asset_id = data.get("asset_id", "")
                        if asset_id in token_ids:
                            timestamp_ms = data.get("timestamp")
                            # Handle both string and int timestamps
                            if isinstance(timestamp_ms, str):
                                timestamp = datetime.fromtimestamp(int(timestamp_ms) / 1000)
                            elif timestamp_ms:
                                timestamp = datetime.fromtimestamp(int(timestamp_ms) / 1000)
                            else:
                                timestamp = datetime.now()
                            
                            # Market Channel uses "buys" and "sells" (not "bids" and "asks")
                            # OrderSummary format: { "price": "0.48", "size": "30" }
                            buys = data.get("buys", [])
                            sells = data.get("sells", [])
                            
                            # Convert OrderSummary objects to tuples for consistency: (price, size)
                            bids = [(float(b.get("price", 0)), float(b.get("size", 0))) if isinstance(b, dict) else b for b in buys]
                            asks = [(float(s.get("price", 0)), float(s.get("size", 0))) if isinstance(s, dict) else s for s in sells]
                            
                            order_book_data = {
                                "token_id": asset_id,
                                "market": data.get("market", ""),  # condition_id
                                "timestamp": timestamp,
                                "bids": bids,  # buys = bids (buy orders)
                                "asks": asks,  # sells = asks (sell orders)
                                "event_type": "book",
                                "hash": data.get("hash", ""),  # For sanity checks
                            }
                            logger.info(f"Received BOOK event for token {asset_id[:20]}... with {len(bids)} bids and {len(asks)} asks")
                            callback(order_book_data, timestamp)
                    # Handle price_change events (real-time price updates)
                    elif data.get("event_type") == "price_change" and "price_changes" in data:
                        price_changes = data.get("price_changes", [])
                        timestamp_ms = int(data.get("timestamp", time.time() * 1000))
                        timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                        
                        # Process each price change (each represents a token's price update)
                        for price_change in price_changes:
                            asset_id = price_change.get("asset_id", "")
                            if asset_id in token_ids:
                                # Extract order book data from price change
                                order_book_data = {
                                    "token_id": asset_id,
                                    "timestamp": timestamp,
                                    "best_bid": price_change.get("best_bid"),
                                    "best_ask": price_change.get("best_ask"),
                                    "price": price_change.get("price"),
                                    "size": price_change.get("size"),
                                    "side": price_change.get("side"),
                                    "market": data.get("market"),  # condition_id
                                    "event_type": "price_change",
                                    "bids": [],  # Full order book not available in price_change events
                                    "asks": [],  # Full order book not available in price_change events
                                }
                                callback(order_book_data, timestamp)
                    # Handle subscription confirmations
                    elif "type" in data:
                        if data.get("type") == "subscribed" or data.get("type") == "subscription_success":
                            logger.info(f"Subscribed to order books for {len(token_ids)} tokens")
                        elif data.get("type") == "error":
                            logger.error(f"WebSocket error: {data.get('message', 'Unknown error')}")
                    else:
                        # Log unhandled message types for debugging
                        logger.info(f"Unhandled message type: {list(data.keys())}, event_type: {event_type}, data: {str(data)[:200]}")
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse WebSocket message: {message}")
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {str(e)}")

        def on_error(ws, error):
            """Handle WebSocket errors"""
            logger.error(f"WebSocket error: {str(error)}")

        def on_close(ws, close_status_code, close_msg):
            """Handle WebSocket close"""
            self._streaming_active = False
            logger.info("WebSocket connection closed")

        def on_open(ws):
            """Handle WebSocket open - subscribe to order books for all token_ids"""
            logger.info(f"WebSocket connected. Subscribing to order books for {len(token_ids)} tokens...")
            
            # CLOB Market Channel subscription format - subscribe to all token_ids
            subscribe_message = {
                "assets_ids": token_ids,
                "type": "market"
            }
            ws.send(json.dumps(subscribe_message))
            logger.info(f"Sent subscription for {len(token_ids)} token_ids: {token_ids[:2]}...")
            
            # Start PING heartbeat to keep connection alive (every 10 seconds)
            def send_ping():
                while self._streaming_active:
                    try:
                        ws.send("PING")
                        time.sleep(10)
                    except Exception:
                        break
            
            ping_thread = threading.Thread(target=send_ping, daemon=True)
            ping_thread.start()

        def run_websocket():
            """Run WebSocket in a separate thread with reconnection logic"""
            reconnect_delay = 1.0
            max_reconnect_delay = 60.0
            
            while self._streaming_active:
                try:
                    # Check duration limit before connecting
                    if duration is not None:
                        elapsed = time.time() - start_time
                        if elapsed >= duration:
                            logger.info(f"Streaming completed after {elapsed:.1f} seconds")
                            break
                    
                    # Create WebSocket connection
                    self._websocket = websocket.WebSocketApp(
                        ws_url,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close,
                        on_open=on_open,
                    )
                    
                    # Run WebSocket with SSL (disable verification for testing)
                    self._websocket.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
                    
                    # If we get here, connection was closed
                    if self._streaming_active:
                        logger.warning(f"WebSocket disconnected. Reconnecting in {reconnect_delay:.1f}s...")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    else:
                        break
                        
                except Exception as e:
                    logger.error(f"WebSocket error: {str(e)}")
                    if self._streaming_active:
                        logger.info(f"Reconnecting in {reconnect_delay:.1f}s...")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    else:
                        break

        # Start WebSocket in background thread
        self._streaming_thread = threading.Thread(target=run_websocket, daemon=True)
        self._streaming_thread.start()
        logger.info(f"Started WebSocket streaming for {len(token_ids)} tokens")

    def stream_order_book_for_market(
        self,
        market: Dict[str, Any],
        callback: Callable[[Dict[str, Any], datetime], None],
        interval: float = 1.0,
        duration: Optional[float] = None,
        token_index: int = 0,
    ) -> None:
        """
        Stream order book updates for a specific market.

        Args:
            market: Market dictionary with token information
            callback: Function to call with each order book update.
                      Signature: callback(order_book_data, timestamp)
            interval: Polling interval in seconds (default: 1.0)
            duration: Total streaming duration in seconds (None for infinite)
            token_index: Which token to stream (0 for first token, 1 for second, etc.)

        Example:
            market = {"tokens": [{"token_id": "0x123..."}]}
            client.stream_order_book_for_market(market, on_update)
        """
        tokens = market.get("tokens", [])
        if not tokens or len(tokens) <= token_index:
            raise ValueError(
                f"Market does not have token at index {token_index}. "
                f"Available tokens: {len(tokens)}"
            )

        token_id = tokens[token_index].get("token_id")
        if not token_id:
            raise ValueError(f"Token at index {token_index} has no token_id")

        # Enhanced callback that includes market info
        def enhanced_callback(order_book_data: Dict[str, Any], timestamp: datetime):
            enriched_data = {
                **order_book_data,
                "market": {
                    "condition_id": market.get("condition_id"),
                    "question_id": market.get("question_id"),
                    "question": market.get("question"),
                },
                "token": tokens[token_index],
            }
            callback(enriched_data, timestamp)

        self.stream_order_book(token_id, enhanced_callback, interval, duration)

    def stop_streaming(self):
        """Stop the currently active order book stream"""
        if self._streaming_active:
            self._streaming_active = False
            
            # Close WebSocket connection
            if self._websocket:
                try:
                    self._websocket.close()
                except Exception:
                    pass
                self._websocket = None
            
            # Wait for thread to finish
            if self._streaming_thread and self._streaming_thread.is_alive():
                self._streaming_thread.join(timeout=2.0)
            
            self._ws_callback = None
            self._ws_token_id = None
            logger.info("Streaming stopped")
        else:
            logger.warning("No active stream to stop")

    def get_trades_from_data_api(
        self,
        condition_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades from Polymarket Data API.
        
        Args:
            condition_id: Filter trades by market/condition ID (optional)
            start_time: Start time for filtering trades (optional)
            end_time: End time for filtering trades (optional)
            limit: Maximum number of trades to return (optional)
            
        Returns:
            List of trade dictionaries with timestamps converted to datetime objects
        """
        base_url = "https://data-api.polymarket.com/trades"
        params = {}
        
        if condition_id:
            params["market"] = condition_id
        if start_time:
            # Convert datetime to timestamp (milliseconds)
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            # Convert datetime to timestamp (milliseconds)
            params["endTime"] = int(end_time.timestamp() * 1000)
        if limit:
            params["limit"] = limit
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            trades = response.json()
            
            # Convert timestamps to datetime objects
            for trade in trades:
                if "timestamp" in trade:
                    # Handle both millisecond and second timestamps
                    ts = trade["timestamp"]
                    if isinstance(ts, str):
                        # Try parsing as ISO format or timestamp
                        try:
                            trade["timestamp"] = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        except (ValueError, AttributeError):
                            try:
                                ts_int = int(float(ts))
                                # Assume milliseconds if > year 2000 in seconds
                                if ts_int > 946684800000:  # Year 2000 in milliseconds
                                    trade["timestamp"] = datetime.fromtimestamp(ts_int / 1000)
                                else:
                                    trade["timestamp"] = datetime.fromtimestamp(ts_int)
                            except (ValueError, TypeError):
                                logger.warning(f"Could not parse timestamp: {ts}")
                                trade["timestamp"] = None
                    elif isinstance(ts, (int, float)):
                        # Assume milliseconds if > year 2000 in seconds
                        ts_int = int(ts)
                        if ts_int > 946684800000:  # Year 2000 in milliseconds
                            trade["timestamp"] = datetime.fromtimestamp(ts_int / 1000)
                        else:
                            trade["timestamp"] = datetime.fromtimestamp(ts_int)
                    else:
                        trade["timestamp"] = None
                
                # Ensure all required fields are present (handle different field names)
                # Data API uses "conditionId" (camelCase), normalize to both formats
                if "conditionId" in trade:
                    trade["condition_id"] = trade["conditionId"]
                    trade["market"] = trade["conditionId"]
                elif "condition_id" in trade:
                    trade["market"] = trade["condition_id"]
                elif "market" in trade:
                    trade["condition_id"] = trade["market"]
            
            logger.info(f"Fetched {len(trades)} trades from Data API")
            return trades
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching trades from Data API: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching trades: {str(e)}")
            return []

    def calculate_volume_from_trades(
        self,
        trades: List[Dict[str, Any]],
        group_by_market: bool = True,
        include_notional: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Calculate volume from trades data.
        
        Args:
            trades: List of trade dictionaries from get_trades_from_data_api
            group_by_market: If True, aggregate by market/condition_id
            include_notional: If True, also calculate notional volume (size * price)
            
        Returns:
            Dictionary with volume statistics. If group_by_market=True, keys are condition_ids.
            Otherwise, returns a single entry with key "all".
            Each entry contains:
            - total_volume: Sum of sizes (Σ size_i)
            - notional_volume: Sum of size * price (Σ size_i · price_i) if include_notional=True
            - trade_count: Number of trades
            - first_trade_time: Timestamp of first trade
            - last_trade_time: Timestamp of last trade
        """
        if not trades:
            return {}
        
        volume_data = {}
        
        for trade in trades:
            market_id = trade.get("market") or trade.get("condition_id") or trade.get("conditionId", "unknown")
            
            if group_by_market:
                if market_id not in volume_data:
                    volume_data[market_id] = {
                        "condition_id": market_id,
                        "total_volume": 0.0,
                        "notional_volume": 0.0,
                        "trade_count": 0,
                        "first_trade_time": None,
                        "last_trade_time": None,
                    }
                market_data = volume_data[market_id]
            else:
                if "all" not in volume_data:
                    volume_data["all"] = {
                        "total_volume": 0.0,
                        "notional_volume": 0.0,
                        "trade_count": 0,
                        "first_trade_time": None,
                        "last_trade_time": None,
                    }
                market_data = volume_data["all"]
            
            # Extract trade data
            size = float(trade.get("size", 0)) if trade.get("size") else 0.0
            price = float(trade.get("price", 0)) if trade.get("price") else 0.0
            timestamp = trade.get("timestamp")
            
            # Aggregate volume
            market_data["total_volume"] += size
            if include_notional:
                market_data["notional_volume"] += size * price
            market_data["trade_count"] += 1
            
            # Track timestamps
            if timestamp:
                if market_data["first_trade_time"] is None or timestamp < market_data["first_trade_time"]:
                    market_data["first_trade_time"] = timestamp
                if market_data["last_trade_time"] is None or timestamp > market_data["last_trade_time"]:
                    market_data["last_trade_time"] = timestamp
        
        logger.info(f"Calculated volume for {len(volume_data)} market(s)")
        return volume_data

    @staticmethod
    def _print_completion_stats(markets: list, start_time: datetime):
        """Print completion statistics"""
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("Fetch completed successfully!")
        logger.info(f"Total markets fetched: {len(markets)}")
        logger.info(f"Time taken: {duration:.2f} seconds")
        if duration > 0:
            logger.info(f"Average rate: {len(markets)/duration:.1f} markets/second")
