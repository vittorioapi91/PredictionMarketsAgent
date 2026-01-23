"""Data collection pipeline orchestrator"""

import logging
from datetime import datetime
from typing import Optional

from src.polymarket import PolymarketClient, MarketDataProcessor, DatabaseManager

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
        """
        self.client = client or PolymarketClient()
        self.processor = processor or MarketDataProcessor()
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
            logger.info("Step 1/3: Collecting Polymarket data...")
            markets = self.client.fetch_all_markets()

            if markets:
                # Save to CSV
                csv_file = self.processor.get_output_path("raw_data", self.date_today)
                self.processor.save_markets_to_csv(markets, csv_file)
                
                # Save to database
                try:
                    self.db_manager.insert_markets(markets)
                except Exception as e:
                    logger.warning(f"Database insert failed: {str(e)}")
                
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
            logger.info("Step 2/3: Filtering open markets...")

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
            logger.info("Step 3/3: Collecting order books...")

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
                
                # Save to database
                try:
                    self.db_manager.insert_order_books(order_books_data)
                except Exception as e:
                    logger.warning(f"Database insert failed: {str(e)}")
                
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

    def run(self) -> bool:
        """
        Run the complete pipeline.

        Returns:
            True if pipeline completed successfully, False otherwise
        """
        start_time = datetime.now()
        logger.info("Starting Polymarket data collection pipeline...")

        try:
            # Setup directories
            self.setup_directories()

            # Step 1: Collect data
            if not self.collect_data():
                return False

            # Step 2: Filter open markets
            if not self.filter_open_markets():
                return False

            # Step 3: Collect order books
            if not self.collect_order_books():
                logger.warning("Order book collection failed, but continuing...")

            logger.info(f"Pipeline completed successfully - {self.date_today}")
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Pipeline execution completed in {duration:.2f} seconds")
            return True

        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}")
            return False


def main():
    """Main entry point for pipeline execution"""
    pipeline = DataPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
