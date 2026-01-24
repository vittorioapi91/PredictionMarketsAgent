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
try:
    from src.polymarket import PolymarketClient
    from src.utils import get_storage_path, get_environment
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.polymarket import PolymarketClient
    from src.utils import get_storage_path, get_environment

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize client (will be set by server)
_client: Optional[PolymarketClient] = None


def set_client(client: PolymarketClient):
    """Set the Polymarket client instance"""
    global _client
    _client = client


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


# Cache for CSV data to avoid reloading on every search
_csv_cache: Optional[pd.DataFrame] = None
_csv_cache_path: Optional[str] = None


def _load_csv_data() -> pd.DataFrame:
    """Load CSV data with caching"""
    global _csv_cache, _csv_cache_path
    
    # Try multiple paths to find the CSV file
    possible_paths = []
    
    # Path 1: Absolute path from project root (webapi/ is 2 levels up from routes.py)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    possible_paths.append(os.path.join(project_root, "storage", "test", "raw_data", "polymarket_data_20260121.csv"))
    
    # Path 2: Using get_storage_path()
    try:
        env = get_environment()
        possible_paths.append(os.path.join(get_storage_path(), "raw_data", "polymarket_data_20260121.csv"))
    except:
        pass
    
    # Path 3: Relative to current file
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths.append(os.path.join(current_file_dir, "..", "..", "storage", "test", "raw_data", "polymarket_data_20260121.csv"))
    
    csv_path = None
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            csv_path = abs_path
            break
    
    if csv_path is None:
        logger.error(f"CSV file not found in any of these paths: {possible_paths}")
        return pd.DataFrame()
    
    # If cache is valid, return it
    if _csv_cache is not None and _csv_cache_path == csv_path and os.path.exists(csv_path):
        return _csv_cache
    
    try:
        logger.info(f"Loading CSV from: {csv_path}")
        _csv_cache = pd.read_csv(csv_path, low_memory=False)
        _csv_cache_path = csv_path
        logger.info(f"Loaded CSV data: {len(_csv_cache)} records from {csv_path}")
        return _csv_cache
    except Exception as e:
        logger.error(f"Error loading CSV: {str(e)}", exc_info=True)
        return pd.DataFrame()


def _search_csv(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search CSV data for markets matching the query in the "question" field.
    This function is designed to be easily replaceable with a DB query later.
    
    Args:
        query: Search query string
        limit: Maximum number of results to return
        
    Returns:
        List of matching market dictionaries
    """
    if not query or len(query.strip()) < 2:
        return []
    
    df = _load_csv_data()
    if df.empty:
        logger.warning("CSV data is empty")
        return []
    
    query_lower = query.lower().strip()
    
    # Search only in the "question" column
    if 'question' not in df.columns:
        logger.warning(f"Column 'question' not found in CSV. Available columns: {list(df.columns)}")
        return []
    
    try:
        # Create a mask for matching rows (case-insensitive search in question field)
        # Handle NaN values by converting to string first
        mask = df['question'].fillna('').astype(str).str.lower().str.contains(query_lower, na=False, regex=False)
        
        # Filter results
        results_df = df[mask].head(limit)
        
        logger.info(f"Search for '{query}' found {len(results_df)} results")
        
        # Convert to list of dictionaries
        # Use orient='records' to get list of dicts
        results = results_df.to_dict(orient='records')  # type: ignore
        
        # Clean up the results (handle NaN values)
        for result in results:
            for key, value in result.items():
                if pd.isna(value):
                    result[key] = None
                elif isinstance(value, (int, float)) and pd.isna(value):
                    result[key] = None
        
        return results
    except Exception as e:
        logger.error(f"Error searching CSV: {str(e)}")
        return []


@router.get("/search")
async def search_markets(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of results"),
):
    """
    Search markets in CSV data.
    Returns top results matching the query.
    """
    try:
        logger.info(f"Search request: query='{q}', limit={limit}")
        results = _search_csv(q, limit=limit)
        logger.info(f"Search returned {len(results)} results for query '{q}'")
        return {
            "query": q,
            "count": len(results),
            "results": results,
        }
    except Exception as e:
        logger.error(f"Error searching markets: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching markets: {str(e)}")
