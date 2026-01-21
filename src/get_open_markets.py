import os
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
try:
    from src.utils import get_storage_path
except ImportError:
    # If running as a script directly, use relative import
    from utils import get_storage_path

logging.basicConfig(
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def filter_open_markets(markets_data):
    """Filter markets to keep only those that are not closed"""
    open_markets = []
    for market in markets_data:
        if market.get('closed') == False and market.get('accepting_orders') == True and market.get('active') == True:
            open_markets.append(market)
    return open_markets

def main():
    date_today = datetime.now().strftime('%Y%m%d')
    
    # Get environment-specific storage paths
    raw_data_dir = get_storage_path('raw_data')
    open_markets_dir = get_storage_path('open_markets')
    
    # Ensure directories exist
    Path(raw_data_dir).mkdir(parents=True, exist_ok=True)
    Path(open_markets_dir).mkdir(parents=True, exist_ok=True)
    
    # Input file path
    input_file = os.path.join(
        raw_data_dir,
        f'polymarket_data_{date_today}.csv'
    )
    
    # Output file path
    output_file = os.path.join(
        open_markets_dir,
        f'open_markets_{date_today}.csv'
    )
    
    try:
        markets_data = pd.read_csv(input_file).to_dict('records')
        open_markets = filter_open_markets(markets_data)
        
        # Save filtered markets to CSV
        df = pd.DataFrame(open_markets)
        df.to_csv(output_file,
                 index=False,
                 encoding='utf-8',
                 quoting=1)
        
        logger.info(f"Total markets: {len(markets_data)}")
        logger.info(f"Open markets: {len(open_markets)}")
        logger.info(f"Data saved to {output_file}")
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
    except Exception as e:
        logger.error(f"Error processing markets: {str(e)}")

if __name__ == "__main__":
    main()
