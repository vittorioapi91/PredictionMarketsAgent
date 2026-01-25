"""Data collection pipeline orchestrator"""

import argparse
import logging
import os
from datetime import datetime
from typing import Optional

from src.polymarket import PolymarketClient, MarketDataProcessor, DatabaseManager
from src.polymarket.gamma_client import fetch_all_events, gamma_events_to_trade_data_rows
from src.utils import load_environment_file, get_environment

# Set up logging
logging.basicConfig(
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def run_gamma_fetch_and_save(
    processor: MarketDataProcessor,
    date_today: str,
    *,
    limit: int = 100,
    include_closed: bool = True,
) -> Optional[str]:
    """
    Shared Gamma fetch + flatten + save. Used by both CLI and Airflow DAG.

    Fetches all events from Gamma API (all pages), flattens to markets,
    saves to gamma_api CSV. Logs pagination verification (partial vs full last page).

    Args:
        processor: MarketDataProcessor for paths and save.
        date_today: YYYYMMDD string.
        limit: Page size for Gamma API.
        include_closed: If True, fetch all events (default).

    Returns:
        Path to saved CSV, or None if fetch/flatten produced no data.
    """
    events = fetch_all_events(limit=limit, include_closed=include_closed)
    if not events:
        return None
    markets = gamma_events_to_trade_data_rows(events)
    if not markets:
        return None
    csv_file = processor.get_output_path("gamma_api", date_today)
    processor.save_markets_to_csv(markets, csv_file, source="gamma-api")
    logger.info("Collected %d markets from %d Gamma events, saved to %s", len(markets), len(events), csv_file)
    return csv_file


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

    def collect_trade_data(self) -> bool:
        """
        Collect CLOB market data and upload to trade_data table.

        Fetches markets via PolymarketClient (CLOB API), saves to raw_data CSV,
        uploads to trade_data. Used for filter_open_markets / order books.
        """
        try:
            markets = self.client.fetch_all_markets()
            if not markets:
                logger.warning("No CLOB markets fetched")
                return False

            csv_file = self.processor.get_output_path("raw_data", self.date_today)
            self.processor.save_markets_to_csv(markets, csv_file, source="clob-api")

            logger.info("Uploading CSV to trade_data table in polymarket database...")
            count = self.db_manager.upload_csv_to_trade_data(
                csv_path=csv_file,
                db_name="polymarket",
                table_name="trade_data",
            )
            logger.info(f"Uploaded {count} records to trade_data table")
            logger.info(f"Collected {len(markets)} CLOB markets")
            return True
        except Exception as e:
            logger.error(f"Error collecting trade data: {str(e)}")
            return False

    def collect_gamma(self) -> bool:
        """
        Collect Gamma API events and upload to gamma_markets table.

        Uses run_gamma_fetch_and_save (shared with DAG), then uploads to DB.
        Search uses active-only by default.
        """
        try:
            csv_file = run_gamma_fetch_and_save(
                self.processor,
                self.date_today,
                limit=100,
                include_closed=True,
            )
            if not csv_file:
                logger.warning("No Gamma data to upload")
                return False

            logger.info("Uploading CSV to gamma_markets table in polymarket database...")
            count = self.db_manager.upload_csv_to_gamma_markets(
                csv_path=csv_file,
                db_name="polymarket",
                table_name="gamma_markets",
            )
            logger.info("Uploaded %d records to gamma_markets table", count)
            return True
        except Exception as e:
            logger.error("Error collecting Gamma data: %s", e)
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
        gamma: bool = False,
        order_book: bool = False,
        filter_markets: bool = True,
    ) -> bool:
        """
        Run the pipeline with optional switches.

        Args:
            trade_data: If True, fetch CLOB markets -> raw_data CSV -> trade_data table
            gamma: If True, fetch Gamma events -> gamma_api CSV -> gamma_markets table
            order_book: If True, collect order books (uses raw_data from trade_data)
            filter_markets: If True, filter open markets (default: True, only used when running full pipeline)

        Returns:
            True if pipeline completed successfully, False otherwise

        Raises:
            ValueError: If no switches are provided
        """
        start_time = datetime.now()

        if not trade_data and not gamma and not order_book:
            raise ValueError(
                "At least one switch must be provided. Use --trade_data, --gamma, and/or --order_book. "
                "Run with --help for usage information."
            )

        logger.info("Starting Polymarket data collection pipeline (selective)...")

        try:
            self.setup_directories()
            success = True

            if trade_data:
                logger.info("Running trade_data step (CLOB -> trade_data)...")
                if not self.collect_trade_data():
                    logger.error("Trade data collection failed")
                    success = False

            if gamma:
                logger.info("Running gamma step (Gamma API -> gamma_markets)...")
                if not self.collect_gamma():
                    logger.error("Gamma collection failed")
                    success = False

            if order_book:
                logger.info("Running order_book step...")
                if not hasattr(self, "_open_markets_loaded"):
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
  # CLOB -> trade_data table
  python -m src.polymarket.data_pipeline --trade_data

  # Gamma API -> gamma_markets table
  python -m src.polymarket.data_pipeline --gamma

  # Both
  python -m src.polymarket.data_pipeline --trade_data --gamma

  # Order books (uses raw_data from --trade_data)
  python -m src.polymarket.data_pipeline --trade_data --order_book

Note: At least one of --trade_data, --gamma, or --order_book must be provided.
        """
    )

    parser.add_argument(
        "--trade_data",
        action="store_true",
        help="Fetch CLOB markets -> raw_data CSV -> trade_data table",
    )
    parser.add_argument(
        "--gamma",
        action="store_true",
        help="Fetch Gamma events -> gamma_api CSV -> gamma_markets table",
    )
    parser.add_argument(
        "--order_book",
        action="store_true",
        help="Collect order books for open markets (uses raw_data from --trade_data)",
    )

    args = parser.parse_args()

    pipeline = DataPipeline()
    success = pipeline.run(
        trade_data=args.trade_data,
        gamma=args.gamma,
        order_book=args.order_book,
    )
    
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
