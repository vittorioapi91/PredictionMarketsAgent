"""
Gamma API client for fetching Polymarket events.

Uses https://gamma-api.polymarket.com/events with pagination.
Supports order=volume24hr, closed/active/archived filters.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"


def fetch_all_events(
    *,
    limit: int = 100,
    include_closed: bool = True,
    order: str = "volume24hr",
    ascending: bool = False,
) -> List[Dict[str, Any]]:
    """
    Paginate through Gamma API events and return all of them.

    Args:
        limit: Page size (default 100).
        include_closed: If True, fetch all events (default). If False, add closed=false.
        order: Sort field (default "volume24hr").
        ascending: Sort direction (default False = desc).

    Returns:
        List of event dicts as returned by the API.
    """
    out: List[Dict[str, Any]] = []
    offset = 0
    num_batches = 0
    last_batch_size = 0

    pbar = tqdm(
        desc="Fetching Gamma events",
        unit=" events",
        initial=0,
        dynamic_ncols=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    )

    while True:
        params: Dict[str, Any] = {
            "order": order,
            "ascending": str(ascending).lower(),
            "limit": limit,
            "offset": offset,
        }
        if not include_closed:
            params["closed"] = "false"

        try:
            resp = requests.get(GAMMA_EVENTS_URL, params=params, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Gamma API request failed: %s", e)
            break

        batch = resp.json() if resp.text else []
        if not isinstance(batch, list):
            logger.warning("Unexpected Gamma API response format (expected list)")
            break

        if not batch:
            break

        last_batch_size = len(batch)
        num_batches += 1
        for ev in batch:
            out.append(ev)

        pbar.update(len(batch))
        pbar.set_postfix(total=len(out))

        if len(batch) < limit:
            break
        offset += limit

    pbar.close()
    logger.info("Fetched %d Gamma events in %d pages (limit=%d)", len(out), num_batches, limit)
    if num_batches > 0:
        if last_batch_size < limit:
            logger.info(
                "Pagination complete: last page had %d events (partial). All pages navigated.",
                last_batch_size,
            )
        else:
            logger.warning(
                "Last page had %d events (full limit). Possible API cap; more data may exist.",
                last_batch_size,
            )
    return out


def gamma_events_to_trade_data_rows(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten Gamma events into gamma_markets-style market rows.

    Each event has a 'markets' array. We emit one row per market, using
    event-level fields (e.g. volume24hr, active, closed) as fallbacks when
    market-level values are missing. Output keys use snake_case to match
    gamma_markets (condition_id, question_id, market_slug, volume_24hr,
    accepting_orders, etc.).
    """
    rows: List[Dict[str, Any]] = []

    for ev in events:
        markets = ev.get("markets") or []
        if not markets:
            # Event with no markets: use event as single "market" row
            rows.append(_event_to_row(ev, None))
            continue

        for m in markets:
            rows.append(_event_to_row(ev, m))

    return rows


def _event_to_row(ev: Dict[str, Any], market: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    def _v(*keys: str, default: Any = None) -> Any:
        for k in keys:
            obj = market if market is not None else ev
            if obj and k in obj and obj[k] is not None:
                return obj[k]
        if market is not None and ev is not None:
            for k in keys:
                if k in ev and ev[k] is not None:
                    return ev[k]
        return default

    vol24 = _v("volume24hr", default=0.0)
    vol = _v("volume", "volumeNum", default=vol24)
    cond_id = _v("conditionId", "condition_id") or _v("id")

    def _num(*keys: str, default: Any = None) -> Any:
        v = _v(*keys, default=default)
        if v is None:
            return None
        try:
            return float(v) if v != "" else None
        except (TypeError, ValueError):
            return None

    row: Dict[str, Any] = {
        "condition_id": cond_id,
        "question_id": _v("questionID", "question_id"),
        "question": _v("question") or _v("title", default=""),
        "description": _v("description", default=""),
        "market_slug": _v("slug", default=""),
        "category": _v("category", default=""),
        "active": _v("active", default=True),
        "closed": _v("closed", default=False),
        "archived": _v("archived", default=False),
        "accepting_orders": _v("acceptingOrders", "accepting_orders", default=True),
        "restricted": _v("restricted", default=False),
        "volume": vol,
        "volume_num": vol if isinstance(vol, (int, float)) else None,
        "volume_24hr": vol24,
        "volume_1wk": _num("volume1wk", "volume_1wk"),
        "volume_1mo": _num("volume1mo", "volume_1mo"),
        "volume_1yr": _num("volume1yr", "volume_1yr"),
        "volume_1wk_amm": _num("volume1wkAmm", "volume_1wk_amm"),
        "volume_1mo_amm": _num("volume1moAmm", "volume_1mo_amm"),
        "volume_1yr_amm": _num("volume1yrAmm", "volume_1yr_amm"),
        "volume_1wk_clob": _num("volume1wkClob", "volume_1wk_clob"),
        "volume_1mo_clob": _num("volume1moClob", "volume_1mo_clob"),
        "volume_1yr_clob": _num("volume1yrClob", "volume_1yr_clob"),
        "liquidity": _num("liquidity"),
        "liquidity_num": _num("liquidityNum", "liquidity_num"),
        "liquidity_amm": _num("liquidityAmm", "liquidity_amm"),
        "liquidity_clob": _num("liquidityClob", "liquidity_clob"),
        "open_interest": _num("openInterest", "open_interest"),
        "competitive": _num("competitive", "competitive"),
        "spread": _num("spread", "spread"),
        "one_day_price_change": _num("oneDayPriceChange", "one_day_price_change"),
        "one_hour_price_change": _num("oneHourPriceChange", "one_hour_price_change"),
        "one_week_price_change": _num("oneWeekPriceChange", "one_week_price_change"),
        "one_month_price_change": _num("oneMonthPriceChange", "one_month_price_change"),
        "one_year_price_change": _num("oneYearPriceChange", "one_year_price_change"),
        "last_trade_price": _num("lastTradePrice", "last_trade_price"),
        "best_bid": _num("bestBid", "best_bid"),
        "best_ask": _num("bestAsk", "best_ask"),
        "image": _v("image", default=""),
        "icon": _v("icon", default=""),
        "end_date_iso": _v("endDate", "endDateIso", "end_date_iso"),
        "clob_token_ids": _v("clobTokenIds", "clob_token_ids"),
        "outcomes": _v("outcomes"),
        "outcome_prices": _v("outcomePrices", "outcome_prices"),
    }

    tokens = _tokens_from_market(market or ev)
    if tokens:
        row["tokens"] = tokens

    return row


def _tokens_from_market(m: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build tokens list from clobTokenIds + outcomes for order book fetching."""
    ids_raw = m.get("clobTokenIds") or m.get("clob_token_ids")
    out_raw = m.get("outcomes")

    if not ids_raw:
        return []

    try:
        ids = ids_raw if isinstance(ids_raw, list) else json.loads(ids_raw)
    except (TypeError, json.JSONDecodeError):
        return []

    if isinstance(out_raw, list):
        outcomes = [str(x) for x in out_raw]
    elif isinstance(out_raw, str):
        try:
            outcomes = json.loads(out_raw)
            outcomes = [str(x) for x in outcomes]
        except (TypeError, json.JSONDecodeError):
            outcomes = [str(x).strip() for x in out_raw.split(",") if x.strip()]
    else:
        outcomes = []

    tokens = []
    for i, tid in enumerate(ids):
        tid_str = str(tid).strip()
        if not tid_str:
            continue
        outcome = outcomes[i] if i < len(outcomes) else ""
        tokens.append({"token_id": tid_str, "outcome": outcome})
    return tokens
