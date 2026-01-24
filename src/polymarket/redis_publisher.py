"""
Redis publisher for order book snapshots.
Publishes order book data to Redis for real-time access.
"""

import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import redis

logger = logging.getLogger(__name__)


class RedisOrderBookPublisher:
    """Publishes order book snapshots to Redis"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        enabled: bool = True,
    ):
        """
        Initialize Redis publisher.

        Args:
            host: Redis host (default: localhost)
            port: Redis port (default: 6379)
            db: Redis database number (default: 0)
            password: Redis password (optional)
            enabled: Whether Redis publishing is enabled (default: True)
        """
        self.enabled = enabled
        self.redis_client: Optional[redis.Redis] = None
        self._warned_unavailable = False  # Track if we've already warned about unavailability

        if not enabled:
            logger.warning("Redis publishing is disabled")
            return

        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=False,  # Keep binary for JSON
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.warning(f"Failed to connect to Redis at {host}:{port}: {str(e)}")
            logger.warning("Order book publishing to Redis is disabled")
            self.enabled = False
            self.redis_client = None
        except Exception as e:
            logger.error(f"Unexpected error connecting to Redis: {str(e)}")
            self.enabled = False
            self.redis_client = None

    def publish_order_book(
        self,
        token_id: str,
        bids: List[Any],
        asks: List[Any],
        timestamp: Optional[datetime] = None,
        condition_id: Optional[str] = None,
    ) -> bool:
        """
        Publish an order book snapshot to Redis.

        Args:
            token_id: Token ID for the order book
            bids: List of bid orders (can be tuples, dicts, or OrderSummary objects)
            asks: List of ask orders (can be tuples, dicts, or OrderSummary objects)
            timestamp: Timestamp of the snapshot (default: now)
            condition_id: Optional condition/market ID

        Returns:
            True if published successfully, False otherwise
        """
        if not self.enabled or self.redis_client is None:
            if not self._warned_unavailable:
                logger.warning("Redis is not available. Order book snapshots will not be published to Redis.")
                self._warned_unavailable = True
            return False

        try:
            # Normalize order book data
            normalized_bids = self._normalize_levels(bids)
            normalized_asks = self._normalize_levels(asks)

            # Create snapshot payload
            snapshot = {
                "token_id": token_id,
                "condition_id": condition_id,
                "timestamp": (timestamp or datetime.now()).isoformat(),
                "bids": normalized_bids,
                "asks": normalized_asks,
                "bid_count": len(normalized_bids),
                "ask_count": len(normalized_asks),
            }

            # Calculate best bid/ask
            if normalized_bids:
                snapshot["best_bid"] = {
                    "price": float(normalized_bids[0][0]),
                    "size": float(normalized_bids[0][1]),
                }
            if normalized_asks:
                snapshot["best_ask"] = {
                    "price": float(normalized_asks[0][0]),
                    "size": float(normalized_asks[0][1]),
                }

            # Serialize to JSON
            json_data = json.dumps(snapshot).encode("utf-8")

            # Publish to Redis with key pattern: orderbook:{token_id}
            key = f"orderbook:{token_id}"
            self.redis_client.set(key, json_data)
            
            # Also publish to a channel for pub/sub (optional, for real-time subscribers)
            channel = f"orderbook:updates"
            self.redis_client.publish(channel, json_data)

            logger.debug(f"Published order book snapshot for token {token_id[:20]}... to Redis")
            return True

        except redis.RedisError as e:
            logger.warning(f"Redis error publishing order book for {token_id[:20]}...: {str(e)}")
            # Mark as unavailable if connection error
            if "Connection" in str(type(e).__name__) or "connection" in str(e).lower():
                self.enabled = False
                self.redis_client = None
            return False
        except Exception as e:
            logger.warning(f"Error publishing order book to Redis: {str(e)}")
            return False

    def _normalize_levels(self, levels: List[Any]) -> List[tuple]:
        """
        Normalize order book levels to (price, size) tuples.

        Args:
            levels: List of order levels (can be tuples, dicts, or objects with price/size)

        Returns:
            List of (price, size) tuples
        """
        normalized = []
        for level in levels:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                # Already a tuple/list: (price, size)
                normalized.append((float(level[0]), float(level[1])))
            elif isinstance(level, dict):
                # Dict format: {"price": "0.48", "size": "30"}
                price = float(level.get("price", 0))
                size = float(level.get("size", 0))
                normalized.append((price, size))
            elif hasattr(level, "price") and hasattr(level, "size"):
                # Object with price/size attributes
                price = float(getattr(level, "price", 0))
                size = float(getattr(level, "size", 0))
                normalized.append((price, size))
            else:
                logger.warning(f"Unknown order level format: {type(level)} - {level}")
        
        return normalized

    def get_order_book(self, token_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve the latest order book snapshot from Redis.

        Args:
            token_id: Token ID to retrieve

        Returns:
            Order book snapshot dict or None if not found
        """
        if not self.enabled or self.redis_client is None:
            return None

        try:
            key = f"orderbook:{token_id}"
            data = self.redis_client.get(key)
            if data:
                return json.loads(data.decode("utf-8"))
            return None
        except Exception as e:
            logger.error(f"Error retrieving order book from Redis: {str(e)}")
            return None

    def close(self):
        """Close Redis connection"""
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {str(e)}")
