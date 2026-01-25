"""Data collection pipeline orchestrator"""

import argparse
import logging
import os
from datetime import datetime
from typing import Optional

from src.polymarket import PolymarketClient, MarketDataProcessor, DatabaseManager
from src.utils import load_environment_file, get_environment

# Set up logging
logging.basicConfig(
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """Orchestrates the complete Polymarket data collection pipeline"""

    def __init__(
        self,
        client: Optional[PolymarketClient] = None,
        processor: Optional[MarketDataProcessor] = None,
        db_manager: Optional[DatabaseManager] = None,
    ):
        """
        Initialize the data pipeline.

        Args:
            client: PolymarketClient instance (creates new if None)
            processor: MarketDataProcessor instance (creates new if None)
            db_manager: DatabaseManager instance (creates new if None)
            
        Raises:
            FileNotFoundError: If environment file (.env-{env}) is not found
            ValueError: If required environment variables are missing
        """
        # Load environment file and validate connection data
        try:
            env = get_environment()
            env_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                f".env-{env}"
            )
            
            # Check if environment file exists
            if not os.path.exists(env_file_path):
                raise FileNotFoundError(
                    f"Environment file not found: .env-{env}\n"
                    f"Expected path: {env_file_path}\n"
                    f"Please create the environment file for the current environment ({env})."
                )
            
            # Load the environment file
            load_environment_file()
            logger.info(f"Loaded environment: {env} from .env-{env}")
            
        except FileNotFoundError:
            raise  # Re-raise FileNotFoundError as-is
        except Exception as e:
            raise RuntimeError(
                f"Failed to load environment file (.env-{get_environment()}): {str(e)}. "
                "Please ensure the environment file exists and is properly formatted."
            ) from e
        
        # Validate required environment variables for database connection
        required_db_vars = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD"]
        missing_vars = []
        for var in required_db_vars:
            value = os.getenv(var)
            if not value or value.strip() == "":
                missing_vars.append(var)
        
        if missing_vars:
            env = get_environment()
            raise ValueError(
                f"Missing or empty required environment variables in .env-{env}: {', '.join(missing_vars)}\n"
                f"Please set these variables in .env-{env} file."
            )
        
        self.client = client or PolymarketClient()
        self.processor = processor or MarketDataProcessor()
        # DatabaseManager will raise error if database doesn't exist - don't catch it here
        self.db_manager = db_manager or DatabaseManager()
        self.date_today = datetime.now().strftime("%Y%m%d")

    def setup_directories(self):
        """Create necessary directories if they don't exist"""
        self.processor.setup_directories()
        # Initialize database tables
        try:
            self.db_manager.create_tables()
        except Exception as e:
            logger.warning(f"Database setup failed: {str(e)}")

    def collect_data(self) -> bool:
        """
        Step 1: Collect all Polymarket data and save to CSV.

        Returns:
            True if successful, False otherwise
        """
        try:
            markets = self.client.fetch_all_markets()

            if markets:
                # Save to CSV
                csv_file = self.processor.get_output_path("raw_data", self.date_today)
                self.processor.save_markets_to_csv(markets, csv_file, source="clob-api")
                
                # Upload CSV to trade_data table in polymarket database
                logger.info("Uploading CSV to trade_data table in polymarket database...")
                count = self.db_manager.upload_csv_to_trade_data(
                    csv_path=csv_file,
                    db_name="polymarket",
                    table_name="trade_data"
                )
                logger.info(f"Uploaded {count} records to trade_data table")
                
                logger.info(f"Collected {len(markets)} markets")
                return True
            else:
                logger.warning("No markets collected")
                return False
        except Exception as e:
            logger.error(f"Error collecting data: {str(e)}")
            return False


    def filter_open_markets(self) -> bool:
        """
        Step 2: Filter for open markets and save to CSV.

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Step 2/4: Filtering open markets...")

            # Load raw data
            input_file = self.processor.get_output_path("raw_data", self.date_today)
            markets_data = self.processor.load_markets_from_csv(input_file)

            # Filter open markets
            open_markets = self.processor.filter_open_markets(markets_data)

            # Save filtered markets to CSV
            output_file = self.processor.get_output_path(
                "open_markets", self.date_today
            )
            self.processor.save_markets_to_csv(open_markets, output_file)
            
            # Update database (markets already inserted, this is just for CSV backup)
            logger.info(f"Total markets: {len(markets_data)}")
            logger.info(f"Open markets: {len(open_markets)}")
            logger.info(f"Data saved to {output_file}")
            return True
        except FileNotFoundError:
            logger.error(f"Input file not found: {input_file}")
            return False
        except Exception as e:
            logger.error(f"Error processing markets: {str(e)}")
            return False

    def collect_order_books(self, max_markets: Optional[int] = None) -> bool:
        """
        Step 3: Collect order books for open markets and save to CSV.

        Args:
            max_markets: Maximum number of markets to process (None for all)

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Step 3/4: Collecting order books...")

            # Load open markets
            input_file = self.processor.get_output_path("open_markets", self.date_today)
            open_markets = self.processor.load_markets_from_csv(input_file)

            if not open_markets:
                logger.warning("No open markets found for order book collection")
                return False

            # Fetch order books
            order_books_data = self.client.get_order_books_for_markets(
                open_markets, max_markets=max_markets
            )

            if order_books_data:
                # Save order books to CSV
                output_file = self.processor.get_output_path(
                    "order_books", self.date_today
                )
                self.processor.save_order_books_to_csv(order_books_data, output_file)
                
                logger.info(f"Collected order books for {len(order_books_data)} tokens")
                logger.info(f"Data saved to {output_file}")
                return True
            else:
                logger.warning("No order books collected")
                return False
        except FileNotFoundError:
            logger.error(f"Input file not found: {input_file}")
            return False
        except Exception as e:
            logger.error(f"Error collecting order books: {str(e)}")
            return False

    def run(
        self,
        trade_data: bool = False,
        order_book: bool = False,
        filter_markets: bool = True,
    ) -> bool:
        """
        Run the pipeline with optional switches.

        Args:
            trade_data: If True, download trade data to CSV and insert into DB
            order_book: If True, collect order books
            filter_markets: If True, filter open markets (default: True, only used when running full pipeline)

        Returns:
            True if pipeline completed successfully, False otherwise
            
        Raises:
            ValueError: If no switches are provided
        """
        start_time = datetime.now()
        
        # Require at least one switch
        if not trade_data and not order_book:
            raise ValueError(
                "At least one switch must be provided. Use --trade_data and/or --order_book. "
                "Run with --help for usage information."
            )
        
        # Run specific steps based on switches
        logger.info("Starting Polymarket data collection pipeline (selective)...")
        
        try:
            # Setup directories
            self.setup_directories()
            
            success = True
            
            if trade_data:
                logger.info("Running trade_data step...")
                if not self.collect_data():
                    logger.error("Trade data collection failed")
                    success = False
            
            if order_book:
                logger.info("Running order_book step...")
                # Order books require open markets, so filter first if needed
                if not hasattr(self, '_open_markets_loaded'):
                    if not self.filter_open_markets():
                        logger.warning("Could not filter open markets, order book collection may fail")
                if not self.collect_order_books():
                    logger.error("Order book collection failed")
                    success = False
            
            duration = (datetime.now() - start_time).total_seconds()
            if success:
                logger.info(f"Pipeline completed successfully - {self.date_today}")
            logger.info(f"Pipeline execution completed in {duration:.2f} seconds")
            return success

        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}")
            return False



def main():
    """Main entry point for pipeline execution"""
    parser = argparse.ArgumentParser(
        description="Polymarket data collection pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download trade data to CSV and insert into DB
  python -m src.polymarket.data_pipeline --trade_data

  # Collect order books
  python -m src.polymarket.data_pipeline --order_book

  # Fetch builders volume data
  python -m src.polymarket.data_pipeline --builders_volume

  # Run multiple steps
  python -m src.polymarket.data_pipeline --trade_data --order_book --builders_volume

Note: At least one switch (--trade_data, --order_book, or --builders_volume) must be provided.
        """
    )
    
    parser.add_argument(
        '--trade_data',
        action='store_true',
        help='Download trade data to CSV and insert into trade_data table in polymarket database'
    )
    
    parser.add_argument(
        '--order_book',
        action='store_true',
        help='Collect order books for open markets'
    )
    
    args = parser.parse_args()
    
    pipeline = DataPipeline()
    success = pipeline.run(
        trade_data=args.trade_data,
        order_book=args.order_book,
    )
    
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
