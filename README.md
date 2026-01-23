# Polymarket Data Collection Pipeline

A Python pipeline for collecting and analyzing market data from [Polymarket](https://polymarket.com).

## Setup

### Prerequisites
- Python 3.8+
- Required packages: pandas, python-dotenv
- Polymarket API credentials

### Environment Setup
1. Create a `.env` file in the project root
2. Add your Polymarket private key:
```
PK=your_private_key_here
```

## Directory Structure
```
PredictionMarketsAgent/
├── src/
│   ├── main.py                    # Main entry point (collects data)
│   ├── utils.py                   # Utility functions
│   ├── polymarket/                # Polymarket package
│   │   ├── __init__.py            # Package exports
│   │   ├── client.py              # API client for Polymarket
│   │   ├── market_processor.py   # Data processing and filtering
│   │   ├── database.py            # Database operations
│   │   ├── data_pipeline.py       # Complete pipeline orchestrator
│   │   ├── stream_orderbook.py    # Order book streaming
│   │   └── stream_prices.py       # Price streaming
│   └── _airflow_dags_/            # Airflow DAGs
├── storage/
│   ├── {env}/raw_data/            # All market data (env: prod/test/dev)
│   └── {env}/open_markets/        # Only open markets
└── tests/                          # Test files
```

## Usage

### Run Complete Pipeline
```bash
python src/polymarket/data_pipeline.py
# or
python -m src.polymarket.data_pipeline
```

### Collect Data Only
```bash
python src/main.py
# or
prediction-markets-agent
```

### Stream Order Book in Real-Time (WebSocket)
```bash
# Stream order book for a specific condition_id
python src/polymarket/stream_orderbook.py --condition-id <condition_id>

# Stream for a specific duration (e.g., 60 seconds)
python src/polymarket/stream_orderbook.py --condition-id <condition_id> --duration 60

# Or use the console script
stream-orderbook --condition-id <condition_id>
```

**Example:**
```python
from src.polymarket import PolymarketClient

def on_update(order_book_data, timestamp):
    print(f"[{timestamp}] Bids: {len(order_book_data['bids'])}, "
          f"Asks: {len(order_book_data['asks'])}")

client = PolymarketClient()
# WebSocket-based streaming (real-time, event-driven)
client.stream_order_book("0x123...", on_update)
```

**Note:** Order book streaming uses WebSocket connections for real-time updates. The connection automatically reconnects if it drops.

This will:
1. Create necessary directories if they don't exist
2. Fetch all Polymarket data (~30k+ markets)
3. Filter for currently open markets
4. Save results in CSV format with date stamps

### Output Files
- Raw data: `storage/{env}/raw_data/polymarket_data_YYYYMMDD.csv`
- Open markets: `storage/{env}/open_markets/polymarket_data_YYYYMMDD.csv`

## Performance
- Typical runtime: ~45 seconds
- Handles pagination automatically
- Processes 30,000+ markets efficiently

## Author
BD_Harold