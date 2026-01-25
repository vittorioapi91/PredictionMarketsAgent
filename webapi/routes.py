"""
API routes for Polymarket dashboard.
"""

import logging
import os
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
from sqlalchemy import text
try:
    from src.polymarket import PolymarketClient
    from src.polymarket.database import DatabaseManager
    from src.utils import get_storage_path, get_environment
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.polymarket import PolymarketClient
    from src.polymarket.database import DatabaseManager
    from src.utils import get_storage_path, get_environment

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize client (will be set by server)
_client: Optional[PolymarketClient] = None

# Initialize database manager (lazy initialization)
_db_manager: Optional[DatabaseManager] = None


def set_client(client: PolymarketClient):
    """Set the Polymarket client instance"""
    global _client
    _client = client


def _get_db_manager() -> DatabaseManager:
    """Get or create database manager instance"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@router.get("/api/markets")
async def get_markets(
    active: Optional[bool] = Query(None, description="Filter by active status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of markets to return"),
):
    """Get list of markets"""
    if _client is None:
        raise HTTPException(status_code=503, detail="Polymarket client not initialized")
    
    try:
        markets = _client.fetch_all_markets()
        
        # Apply filters
        if active is not None:
            markets = [m for m in markets if m.get("active") == active]
        
        # Limit results
        markets = markets[:limit]
        
        return {
            "count": len(markets),
            "markets": markets,
        }
    except Exception as e:
        logger.error(f"Error fetching markets: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching markets: {str(e)}")


@router.get("/api/markets/{condition_id}")
async def get_market(condition_id: str):
    """Get details for a specific market"""
    if _client is None:
        raise HTTPException(status_code=503, detail="Polymarket client not initialized")
    
    try:
        markets = _client.fetch_all_markets()
        market = next((m for m in markets if m.get("condition_id") == condition_id), None)
        
        if not market:
            raise HTTPException(status_code=404, detail=f"Market {condition_id} not found")
        
        return market
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching market: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching market: {str(e)}")


@router.get("/api/markets/{condition_id}/orderbook")
async def get_orderbook(condition_id: str):
    """Get order book for a specific market"""
    if _client is None:
        raise HTTPException(status_code=503, detail="Polymarket client not initialized")
    
    try:
        markets = _client.fetch_all_markets()
        market = next((m for m in markets if m.get("condition_id") == condition_id), None)
        
        if not market:
            raise HTTPException(status_code=404, detail=f"Market {condition_id} not found")
        
        tokens = market.get("tokens", [])
        if not tokens:
            return {"condition_id": condition_id, "order_books": []}
        
        # Fetch order books for all tokens
        order_books = []
        for token in tokens:
            token_id = token.get("token_id")
            if token_id:
                order_book = _client.get_order_book(token_id)
                if order_book:
                    order_books.append({
                        "token_id": token_id,
                        "order_book": {
                            "bids": [
                                {"price": str(b.price), "size": str(b.size)} 
                                for b in (order_book.bids or [])
                            ] if hasattr(order_book, "bids") else [],
                            "asks": [
                                {"price": str(a.price), "size": str(a.size)} 
                                for a in (order_book.asks or [])
                            ] if hasattr(order_book, "asks") else [],
                        }
                    })
        
        return {
            "condition_id": condition_id,
            "order_books": order_books,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching order book: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching order book: {str(e)}")


@router.get("/api/markets/{condition_id}/volume")
async def get_volume(
    condition_id: str,
    hours: int = Query(24, ge=1, le=168, description="Number of hours to look back"),
):
    """Get volume data for a specific market"""
    if _client is None:
        raise HTTPException(status_code=503, detail="Polymarket client not initialized")
    
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        trades = _client.get_trades_from_data_api(
            condition_id=condition_id,
            start_time=start_time,
            end_time=end_time,
        )
        
        volume_data = _client.calculate_volume_from_trades(
            trades,
            group_by_market=True,
            include_notional=True,
        )
        
        return {
            "condition_id": condition_id,
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            },
            "volume": volume_data.get(condition_id, {}),
        }
    except Exception as e:
        logger.error(f"Error fetching volume: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching volume: {str(e)}")


# Cache database engine to avoid recreating connections
_search_engine_cache = None

def _get_search_engine():
    """Get or create a cached database engine for search queries"""
    global _search_engine_cache
    if _search_engine_cache is None:
        db_manager = _get_db_manager()
        connection_string = (
            f"postgresql://{db_manager.db_user}:{db_manager.db_password}@"
            f"{db_manager.db_host}:{db_manager.db_port}/polymarket"
        )
        from sqlalchemy import create_engine
        _search_engine_cache = create_engine(
            connection_string, 
            pool_pre_ping=True, 
            echo=False,
            pool_size=5,
            max_overflow=10
        )
    return _search_engine_cache

def _search_gamma_markets(query: str, limit: int = 10, include_inactive: bool = False) -> List[Dict[str, Any]]:
    """
    Search gamma_markets table for markets matching the query in the "question" field.

    Args:
        query: Search query string
        limit: Maximum number of results to return
        include_inactive: If True, include inactive/closed/archived/not accepting orders

    Returns:
        List of matching market dictionaries
    """
    if not query or len(query.strip()) < 2:
        return []

    try:
        engine = _get_search_engine()

        if include_inactive:
            search_query = text("""
                SELECT question
                FROM gamma_markets
                WHERE question ILIKE :query_pattern
                ORDER BY COALESCE(volume_24hr, 0) DESC
                LIMIT :limit
            """)
        else:
            search_query = text("""
                SELECT question
                FROM gamma_markets
                WHERE active = true
                    AND closed = false
                    AND archived = false
                    AND accepting_orders = true
                    AND question ILIKE :query_pattern
                ORDER BY COALESCE(volume_24hr, 0) DESC
                LIMIT :limit
            """)

        query_pattern = f"%{query.strip()}%"

        with engine.connect() as conn:
            results = conn.execute(
                search_query,
                {"query_pattern": query_pattern, "limit": limit}
            )

            columns = results.keys()
            results_list = []
            for row in results:
                row_dict = {}
                for col in columns:
                    value = getattr(row, col)
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        row_dict[col] = None
                    else:
                        row_dict[col] = value
                results_list.append(row_dict)

            logger.info(f"Search for '{query}' found {len(results_list)} results from gamma_markets table")
            return results_list

    except Exception as e:
        logger.error(f"Error searching gamma_markets: {str(e)}", exc_info=True)
        return []


@router.get("/search")
async def search_markets(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of results"),
    include_inactive: bool = Query(False, description="Include inactive/closed/archived/not accepting orders bets"),
):
    """
    Search markets in gamma_markets table.
    Returns top results matching the query in the question field.
    By default, only returns active, not closed, accepting orders, not archived.
    Connects to the appropriate database based on environment (dev/test/prod).
    """
    try:
        logger.info(f"Search request: query='{q}', limit={limit}, include_inactive={include_inactive}")
        results = _search_gamma_markets(q, limit=limit, include_inactive=include_inactive)
        logger.info(f"Search returned {len(results)} results for query '{q}'")
        return {
            "query": q,
            "count": len(results),
            "results": results,
        }
    except Exception as e:
        logger.error(f"Error searching markets: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching markets: {str(e)}")
